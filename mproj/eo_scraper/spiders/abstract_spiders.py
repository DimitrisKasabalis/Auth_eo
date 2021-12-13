import json
import os
import re
from celery.utils.log import get_task_logger
from datetime import datetime, date as dt_date
from django.core.exceptions import ObjectDoesNotExist
from pytz import utc
from scrapy import Request, FormRequest
from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from scrapy.selector.unified import Selector
from scrapy.spiders import CrawlSpider, Spider
from scrapy.spiders import Rule
from scrapy.spiders.init import InitSpider
from typing import List, Pattern, Optional
from urllib.parse import urlsplit, urlparse

from eo_engine.models import EOSourceGroup
from eo_engine.models.other import CrawlerConfiguration
from eo_scraper.items import RemoteSourceItem
from eo_scraper.utils import get_credentials, credentials

logger = get_task_logger(__name__)

'''
https://docs.scrapy.org/en/latest/topics/spiders.html

For spiders, the scraping cycle goes through something like this:

1.  You start by generating the initial Requests to crawl the first URLs, 
    and specify a callback function to be called with the response downloaded 
    from those requests.

1.  The first requests to perform are obtained by calling the start_requests() 
    method which (by default) generates Request for the URLs specified in the 
    start_urls and the parse method as callback function for the Requests.

1.  In the callback function, you parse the response (web page) and return 
    item objects, Request objects, or an iterable of these objects. Those Requests 
    will also contain a callback (maybe the same) and will then be downloaded by Scrapy 
    and then their response handled by the specified callback.

1.  In callback functions, you parse the page contents, typically using Selectors 
    (but you can also use BeautifulSoup, lxml or whatever mechanism you prefer) 
    and generate items with the parsed data.

1.  Finally, the items returned from the spider will be typically persisted to a 
        database (in some Item Pipeline) or written to a file using Feed exports.
        
        
There are multiple variation of Spiders, The simplest one is the scrapy.Spider. 
    Then we have 
        - CrawlSpider
        - InitSpider
'''


class AfricultureCrawlerMixin:
    """ A set of common functions for all Crawlers """

    def get_crawler_settings(self):
        return CrawlerConfiguration.objects.filter(group=self.name).get()

    def get_group_settings(self) -> EOSourceGroup:
        return EOSourceGroup.objects.get(name=self.name)

    def should_process_response(self, response: Response) -> bool:
        """if false, the request will not be processed"""
        return True

    def date_reference_from_filename(self, filename) -> Optional[dt_date]:
        from eo_engine.common.misc import str_to_date
        eo_source_group = EOSourceGroup.objects.get(name=self.name)

        return str_to_date(token=filename, regex_string=eo_source_group.date_regex)

    def is_expected_filename(self, filename: str) -> bool:
        """Return True if filename passes the date_regex check. False otherwise"""
        eo_source_group = EOSourceGroup.objects.get(name=self.name)
        regex_str = eo_source_group.date_regex
        match = re.match(eo_source_group.date_regex, filename, re.IGNORECASE)
        if match:
            return True
        logger.warning(f'{filename} did not match the reg-ex string {regex_str}')
        return False

    # noinspection PyMethodMayBeStatic
    def should_process_filename(self, filename: str) -> bool:
        """Return True to process the Entry. False to Drop.
        Some entries should not be processed based on the their filename. Eg tiles or other.
        """
        return True


class CopernicusVgtDatapool(InitSpider, CrawlSpider, AfricultureCrawlerMixin):
    allowed_domains: List[str] = ['land.copernicus.vgt.vito.be']
    login_page: str = 'https://land.copernicus.vgt.vito.be/PDF/datapool'

    credentials: [str, str]  # username, password

    browse_page_regex: Pattern[str] = re.compile(r"^https.*/$")
    catalog_page_regex: Pattern[str] = re.compile(r"^https.*\d{4}/\d{2}/\d{2}/.*/$")

    rules: List[Rule] = (
        Rule(LinkExtractor(allow=(browse_page_regex,), deny=(catalog_page_regex,)), follow=True, callback='parse'),
        Rule(LinkExtractor(allow=(catalog_page_regex,)), callback='parse_catalog', follow=True)
    )

    def init_request(self):
        url_split = urlsplit(self.login_page)
        domain = url_split.netloc
        self.credentials = get_credentials(domain)
        return Request(url=self.login_page, callback=self.login)

    def login(self, response):
        return FormRequest.from_response(
            response, formdata={"login": self.credentials[0],
                                "password": self.credentials[1]},
            formid='loginForm', callback=self.check_login_response)

    def check_login_response(self, response):
        print(response)
        if "Authentication failure" in response.css('#loginDiv *::text').getall():
            self.logger.error("Authentication Failed")
            return

        return self.initialized()

    def parse(self, response, **kwargs):
        # https://docs.scrapy.org/en/latest/topics/spiders.html#scrapy.spiders.Spider.parse
        pass

    def parse_catalog(self, response):
        # if not need to process the response further, yield nothing
        if not self.should_process_response(response):
            yield

        tableRow: Selector
        for idx, tableRow in enumerate(response.xpath('//tr[count(td)=4]')):
            filename = str(tableRow.xpath('.//td[1]/text()').get()).strip()
            if not self.is_expected_filename(filename=filename):
                return
            dt_reference = self.date_reference_from_filename(filename)
            loader = ItemLoader(item=RemoteSourceItem(), selector=tableRow)
            loader.add_xpath('size', 'td[position()=2]/text()', pos=2)

            loader.add_value('datetime_reference', dt_reference)
            loader.add_value('filename', filename)
            loader.add_value('datetime_seen', datetime.utcnow())
            loader.add_value('domain', response.url)
            loader.add_value('url', response.url)
            loader.add_xpath('url', 'td/a/@href')

            yield loader.load_item()


class FtpRequest(Request, AfricultureCrawlerMixin):

    def __init__(self, url, credentials: Optional[credentials], *args, **kwargs):
        super(FtpRequest, self).__init__(url, *args, **kwargs)
        meta = {}
        if credentials:
            meta.update(ftp_user=credentials.username,
                        ftp_password=credentials.password)

        # self.meta.update(meta)
        super(FtpRequest, self).__init__(url,
                                         meta=meta,
                                         errback=self.errback_for_failure,
                                         *args, **kwargs)

    def errback_for_failure(self, failure):
        pass


class FtpSpider(Spider, AfricultureCrawlerMixin):
    # there's no robots.txt
    custom_settings = {
        'ROBOTSTXT_OBEY': False
    }
    ftp_root_url = None
    credentials: Optional[credentials]

    def filename_filter(self, token: str) -> bool:
        """a function that accepts the filename token and returns T of F.
        If False the file is ignored."""
        return True

    def __init__(self, *args, **kwargs):
        super(FtpSpider, self).__init__(*args, **kwargs)
        if self.ftp_root_url is None:
            raise NotImplementedError('ftp_root_url is unset')

        url_split = urlsplit(self.ftp_root_url)
        domain = url_split.netloc
        try:
            self.credentials = get_credentials(domain)
        except ObjectDoesNotExist:
            self.credentials = None

    def start_requests(self):
        yield FtpRequest(self.ftp_root_url, credentials=self.credentials)

    def parse(self, response, **kwargs):
        url = urlparse(response.url)
        files = json.loads(response.body)
        for f in files:
            if f['filetype'] == 'd':  # filetype is 'd' -> Directory, route for seed
                path = os.path.join(response.url, f['filename'])
                yield FtpRequest(path, self.credentials)

            if f['filetype'] == '-':
                result = RemoteSourceItem(
                    filename=f.get('filename', None),
                    # extension=f.get('filename', None),
                    size=f.get('size', None),
                    domain=url.netloc,
                    # datetime_uploaded=0,  # we cannot tell from ftp
                    datetime_seen=datetime.utcnow().replace(tzinfo=utc),
                    url=os.path.join(response.url, f['filename'])
                )

                if self.filename_filter(result['filename']):
                    # if filename filter in place, and is true
                    yield result
