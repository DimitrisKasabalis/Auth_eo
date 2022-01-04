import json
import os
import re
from celery.utils.log import get_task_logger
from datetime import datetime, date as dt_date
from django.core.exceptions import ObjectDoesNotExist
from django.utils.datetime_safe import datetime
from pytz import utc
from scrapy import Request, FormRequest
from scrapy.http import Response
from scrapy.loader import ItemLoader
from scrapy.selector.unified import Selector
from scrapy.spiders import Spider
from scrapy.spiders.init import InitSpider
from typing import Optional
from urllib.parse import urlsplit, urlparse

from eo_engine.models import EOSourceGroup
from eo_engine.models.other import CrawlerConfiguration
from eo_scraper.items import RemoteSourceItem
from eo_scraper.utils import get_credentials, credentials

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

logger = get_task_logger(__name__)


class AfricultureCrawlerMixin:
    """ A set of common functions for all Crawlers """

    name: str

    @property
    def from_date(self):
        return self.get_crawler_settings().from_date

    def get_crawler_settings(self) -> CrawlerConfiguration:
        return CrawlerConfiguration.objects.filter(group=self.name).get()

    def get_group_settings(self) -> EOSourceGroup:
        return EOSourceGroup.objects.get(name=self.name)

    def should_process_response(self, response: Response) -> bool:
        """if false, the request will not be processed. Overrider if needed """
        return True

    def date_reference_from_filename(self, filename) -> Optional[dt_date]:
        from eo_engine.common.misc import str_to_date
        eo_source_group = EOSourceGroup.objects.get(name=self.name)

        return str_to_date(token=filename, regex_string=eo_source_group.date_regex)

    def is_expected_filename(self, filename: str) -> bool:
        """Return True if filename passes the date_regex check. False otherwise"""

        group_settings = self.get_group_settings()
        filename_regex = group_settings.date_regex_cached  # cached version
        match = re.match(filename_regex, filename, re.IGNORECASE)
        if match is None:
            self.logger.info(
                f'SHOULD_PROCESS_FILENAME:FAILED: +{filename}+ did not pass regex check: +{filename_regex}+')
            return False
        return True

    # noinspection PyMethodMayBeStatic
    def should_process_filename(self, filename: str) -> bool:
        """Return True to process the Entry. False to Drop. Override as needed.
        Some entries should not be processed based on their filename. Eg tiles or other.
        """
        return True


class AfricultureSpider(Spider, AfricultureCrawlerMixin):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.crawler_settings = self.get_crawler_settings()
        self.group_settings = self.get_group_settings()
        self.start_date = self.crawler_settings.from_date
        self.min_year = self.start_date.year
        self.min_doy = int(self.start_date.strftime('%j'))


class AfricultureSpiderLogin(InitSpider, AfricultureSpider):

    def __init__(self, login_page: str, **kwargs):
        super().__init__(**kwargs)
        self.logger.info(f"initing {self.__class__.__name__}!")
        self.login_page = login_page

        url_split = urlsplit(self.login_page)
        domain = url_split.netloc
        try:
            self.credentials = get_credentials(domain)
        except ObjectDoesNotExist:
            print(f'Could not find domain for {self.login_page}')
            self.credentials = None

    def init_request(self):
        self.logger.info("Init login")
        return Request(url=self.login_page, callback=self._do_login)

    def _do_login(self, response):
        def check_login_response(login_response):
            if "Authentication failure" in login_response.css('#loginDiv *::text').getall():
                self.logger.error("Authentication Failed")
                return None
            # after we return this, operates as normal
            self.logger.info("Log in Successful")
            return self.initialized()

        return FormRequest.from_response(
            response, formdata={
                "login": self.credentials.username,
                "password": self.credentials.password},
            formid='loginForm', callback=check_login_response)

    @property
    def login_page(self) -> str:
        return self._login_page

    @login_page.setter
    def login_page(self, val):
        self._login_page = val


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
                filename = result['filename']
                if self.should_process_filename(filename=filename):
                    yield result


class CopernicusSpider(AfricultureSpiderLogin):
    allowed_domains = ['land.copernicus.vgt.vito.be']

    def __init__(self, **kwargs):
        super().__init__(
            login_page='https://land.copernicus.vgt.vito.be/PDF/datapool',
            **kwargs
        )

    def parse(self, response: Response, **kwargs):
        # https://regex101.com/r/XalJaa/1
        all_hrefs = list(response.copy().xpath('//a/@href').getall())

        href: str
        for href in all_hrefs:
            url_date_pat = re.compile(
                r'https?://.+/(?P<YEAR>\d{4})/?(?P<MONTH>\d{2})?/?(?P<DAY>\d{2})?/?(?P<PRODUCT_NAME>.*)?/$',
                re.IGNORECASE)
            target_url = href
            match = url_date_pat.match(target_url)
            if match is None:
                self.logger.info(f'{target_url} did not match pat ++{url_date_pat.pattern}++')
                continue
            self.logger.debug(f'PROCESSING URL: {target_url}')
            groupdict = match.groupdict()
            year = int(groupdict.get('YEAR'))
            month = int(groupdict.get('MONTH')) if groupdict.get('MONTH') else None
            day = int(groupdict.get('DAY')) if groupdict.get('DAY') else None
            product_name = groupdict.get('PRODUCT_NAME')
            if year is not None and year >= self.from_date.year:
                if month is None:
                    yield response.follow(url=target_url, callback=self.parse)
                elif month >= self.from_date.month:
                    if day is None:
                        yield response.follow(url=target_url, callback=self.parse)
                    elif day >= self.from_date.day:
                        if product_name:
                            yield response.follow(url=target_url, callback=self.parse_catalog)
                        yield response.follow(url=target_url, callback=self.parse)
                    else:
                        self.logger.info(
                            f'Response year {day} is older than the requested day {self.from_date.day}')
                else:
                    self.logger.info(f'Response year {month} is older than the requested month {self.from_date.month}')
            else:
                self.logger.info(f'Response year {year} is older than the requested year {self.from_date.year}')

            yield None

    def should_process_filename(self, filename: str) -> bool:
        if filename.endswith('.nc'):
            return True
        return False

    def parse_catalog(self, response):
        self.logger.info(f'hi from parse_catalog: {response}')
        tableRow: Selector
        for idx, tableRow in enumerate(response.xpath('//tr[count(td)=4]')):
            filename = tableRow.xpath('.//a/@href').get()
            if not self.should_process_filename(filename):
                continue

            loader = ItemLoader(item=RemoteSourceItem(), selector=tableRow)

            loader.add_value('filename', filename)
            loader.add_xpath('size', 'td[position()=2]/text()', pos=2)
            loader.add_value('datetime_seen', datetime.utcnow())
            loader.add_value('domain', response.url)
            loader.add_value('url', response.url)
            loader.add_xpath('url', 'td/a/@href')

            yield loader.load_item()
