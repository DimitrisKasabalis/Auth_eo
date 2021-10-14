import json
import os
import re
from datetime import datetime
from typing import List, Pattern, Callable, Optional
from urllib.parse import urlsplit, urlparse

from django.core.exceptions import ObjectDoesNotExist
from pytz import utc
from scrapy import Request, FormRequest
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from scrapy.spiders import CrawlSpider, Spider
from scrapy.spiders import Rule
from scrapy.spiders.init import InitSpider

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


class CopernicusVgtDatapool(InitSpider, CrawlSpider):
    allowed_domains: List[str] = ['land.copernicus.vgt.vito.be']
    login_page: str = 'https://land.copernicus.vgt.vito.be/PDF/datapool'
    product_name: str

    credentials: [str, str]  # username, password

    browse_page_regex: Pattern[str] = re.compile(r"^https.*/$")
    catalog_page_regex: Pattern[str] = re.compile(r"^https.*\d{4}/\d{2}/\d{2}/.*/$")

    rules: List[Rule] = (
        Rule(LinkExtractor(allow=(browse_page_regex,), deny=(catalog_page_regex,)), follow=True, callback='parse'),
        Rule(LinkExtractor(allow=(catalog_page_regex,)), callback='parse_catalog', follow=True)
    )

    def __init__(self, *args, **kwargs):
        super(CopernicusVgtDatapool, self).__init__(*args, **kwargs)  # default init binds kwargs to self

    def init_request(self):
        print("init_request")
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

        # This is the default callback used by Scrapy to process
        # downloaded responses, when their requests don’t specify a callback.

        # self.logger.info('A response from %s just arrived!' % response.url)
        pass

    def parse_catalog(self, response):
        self.logger.info('PARSE_CATALOG: A response from %s just arrived!' % response.url)
        for idx, tableRow in enumerate(response.xpath('//tr[count(td)=4]')):
            loader = ItemLoader(item=RemoteSourceItem(),
                                selector=tableRow)

            # loader.add_value('product_name', self.product_name)
            loader.add_xpath('filename', 'td[position()=1]/text()', pos=1)
            # loader.add_xpath('extension', 'td[position()=1]/text()', pos=1)
            loader.add_xpath('size', 'td[position()=2]/text()', pos=2)
            # loader.add_xpath('datetime_uploaded', 'td[position()=3]/text()', pos=3)
            loader.add_value('datetime_seen', datetime.utcnow())
            loader.add_value('domain', response.url)
            loader.add_value('url', response.url)
            loader.add_xpath('url', 'td/a/@href')

            yield loader.load_item()


class FtpRequest(Request):

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


class FtpSpider(Spider):
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
                    # product_name='asf',
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
