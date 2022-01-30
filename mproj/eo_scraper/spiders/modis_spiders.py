import re
from django.utils.datetime_safe import datetime
from itemloaders import ItemLoader
from scrapy import Selector
from scrapy.http import Response
from typing import List

from celery.utils.log import get_task_logger

from eo_engine.models import EOSourceGroupChoices
from eo_scraper.items import RemoteSourceItem
from eo_scraper.spiders.abstract_spiders import AfricultureSpider
from eo_engine.errors import AfriCultuReSError

logger = get_task_logger(__name__)

AFRICA_TILES = ["h16v05", "h16v06", "h16v07", "h16v08", "h16v09", "h17v05", "h17v06", "h17v07", "h17v08", "h18v05",
                "h18v06", "h18v07", "h18v08", "h18v09", "h19v05", "h19v06", "h19v07", "h19v08", "h19v09", "h19v10",
                "h19v11", "h19v12", "h20v05", "h20v06", "h20v07", "h20v08", "h20v09", "h20v10", "h20v11", "h20v12",
                "h21v05", "h21v06", "h21v07", "h21v08", "h21v09", "h21v10", "h21v11", "h22v06", "h22v07", "h22v08",
                "h22v09", "h22v10", "h22v11", "h23v07", "h23v08"]


class MODISSpider(AfricultureSpider):
    tiles: List[str]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.crawler_settings = self.get_crawler_settings()
        self.group_settings = self.get_group_settings()

        self.start_date = self.crawler_settings.from_date
        self.min_year = self.start_date.year

    def get_tiles(self) -> List[str]:
        return self.tiles

    def parse(self, response, **kwargs):
        # implement on application level
        raise NotImplementedError()


class ModisMCD12Q1Spider(MODISSpider):
    name = EOSourceGroupChoices.S04P01_LULC_500M_MCD12Q1_v6
    tiles = AFRICA_TILES
    start_urls = [
        f"https://e4ftl01.cr.usgs.gov/MOTA/MCD12Q1.006/"
    ]
    mcd12q1_catalog_page = re.compile(r'(?P<YEAR>\d\d\d\d)\.\d\d\.\d\d/', re.IGNORECASE)
    mcd12q1_tile_regex = re.compile(r'^MCD12Q1\.A\d{7}\.(?P<TILE>.{6}).*hdf$', re.IGNORECASE)

    def parse(self, response, **kwargs):
        all_hrefs = list(response.copy().xpath('//a/@href').getall())
        href: str
        for href in all_hrefs:
            target_url = response.url + href
            print(target_url)
            match = self.mcd12q1_catalog_page.match(href)
            if match:
                group_dict = match.groupdict()
                try:
                    year = int(group_dict['YEAR'])
                except TypeError as e:
                    logger.warn(
                        """
                        target_url:{target_url}
                        resp_year: {resp_year},min_year: {min_year}
                        resp_doy: {resp_doy}, min_doy: {min_doy}""")

                    raise AfriCultuReSError("ModisMCD12Q1Spider error") from e
                print(f'year is {year}. compared vs {self.min_year}')
                if year >= self.min_year:
                    yield response.follow(target_url, callback=self.parse_catalog)

    def parse_catalog(self, response, **kwargs):
        # yes, regular expressions, in xml, why not? What could possibly go wrong?
        # https://lxml.de/xpathxslt.html#xpath
        # all_hrefs = list(/@href").getall())
        selector: Selector
        for selector in response.copy().xpath("//a[re:test(.,'.*hdf$','i')]"):
            loader = ItemLoader(item=RemoteSourceItem(), selector=selector)
            filename = selector.xpath('./@href').get()
            loader.add_value('filename', filename)
            loader.add_xpath('size', 'td[position()=4]/text()', pos=4)
            loader.add_value('datetime_seen', datetime.utcnow())
            loader.add_value('domain', response.url)
            loader.add_value('url', response.url)
            loader.add_value('url', filename)

            yield loader.load_item()
