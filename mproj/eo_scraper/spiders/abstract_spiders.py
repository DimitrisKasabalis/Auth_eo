import re
from datetime import datetime
from typing import Dict, List, Pattern
from urllib.parse import urlsplit

from scrapy import Request, FormRequest
from scrapy.loader import ItemLoader
from scrapy.spiders import CrawlSpider
from scrapy.spiders import Rule
from scrapy.spiders.init import InitSpider

from eo_scraper.items import VitoEODataItem
from eo_scraper.utils import get_auth
from scrapy.linkextractors import LinkExtractor


# class BaseSpider(CrawlSpider):
#
#     def __init__(self, *args, **kwargs):
#         super(CrawlSpider, self).__init__(*args, **kwargs)
#
#     def parse(self, response, **kwargs):
#         pass


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
        self.credentials = get_auth(domain)
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
        # self.logger.info('A response from %s just arrived!' % response.url)
        pass

    def parse_catalog(self, response):
        self.logger.info('A response from %s just arrived!' % response.url)
        for idx, tableRow in enumerate(response.xpath('//tr[count(td)=4]')):
            loader = ItemLoader(item=VitoEODataItem(),
                                selector=tableRow)

            loader.add_value('product_name', self.product_name)
            loader.add_xpath('filename', 'td[position()=1]/text()', pos=1)
            loader.add_xpath('extension', 'td[position()=1]/text()', pos=1)
            loader.add_xpath('size', 'td[position()=2]/text()', pos=2)
            loader.add_xpath('datetime_uploaded', 'td[position()=3]/text()', pos=3)
            loader.add_value('datetime_scrapped', datetime.utcnow())
            loader.add_value('domain', response.url)
            loader.add_value('url', response.url)
            loader.add_xpath('url', 'td/a/@href')

            yield loader.load_item()
