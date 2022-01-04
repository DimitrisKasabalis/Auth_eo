import re
from celery.utils.log import get_task_logger
from datetime import datetime
from itemloaders import ItemLoader
from scrapy import Selector
from scrapy.http import Response
from scrapy.linkextractors import IGNORED_EXTENSIONS as DEFAULT_IGNORED_EXTENSIONS
from scrapy.spiders import Spider
from typing import List

from eo_engine.common.patterns import GMOD09Q1_PAGE_PATTERN
from eo_engine.models import EOSourceGroupChoices
from eo_scraper.items import RemoteSourceItem
from eo_scraper.spiders.abstract_spiders import AfricultureCrawlerMixin

# We don't want to 'download' the file
IGNORED_EXTENSIONS = DEFAULT_IGNORED_EXTENSIONS + ['tif.gz', ]

logger = get_task_logger(__name__)


class NDVIAnomaly(Spider, AfricultureCrawlerMixin):
    tiles: List[str]
    start_urls = [
        f"https://gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI_anom_S2001-2018/"
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.crawler_settings = self.get_crawler_settings()
        self.group_settings = self.get_group_settings()
        self.start_date = self.crawler_settings.from_date
        self.min_year = self.start_date.year
        self.min_doy = int(self.start_date.strftime('%j'))

    def get_tiles(self) -> List[str]:
        return self.tiles

    def parse(self, response: Response, **kwargs):
        all_hrefs = list(response.copy().xpath('//a/@href').getall())
        href: str
        for href in all_hrefs:
            target_url = response.url + href
            match = GMOD09Q1_PAGE_PATTERN.match(target_url)
            if match:
                group_dict = match.groupdict()
                try:
                    resp_year = int(group_dict['year'])
                except TypeError:
                    resp_year = None
                try:
                    resp_doy = int(group_dict['doy'])
                except TypeError:
                    resp_doy = None
                logger.warn(
                    """
                    target_url:{target_url}
                    resp_year: {resp_year},min_year: {min_year}
                    resp_doy: {resp_doy}, min_doy: {min_doy}""")

                if resp_year is None:
                    return None

                # year page
                if resp_year >= self.min_year:
                    if resp_doy is None:
                        yield response.follow(href, callback=self.parse)
                    elif resp_doy >= self.min_doy:
                        # catalog page, that has all the links to the files
                        yield response.follow(target_url, callback=self.parse_catalog)
            yield None

    def is_expected_filename(self, filename: str) -> bool:
        group_settings = self.get_group_settings()
        filename_regex = group_settings.date_regex_cached  # cached version
        match = re.match(filename_regex, filename, re.IGNORECASE)
        if match is None:
            self.logger.info(
                f'SHOULD_PROCESS_FILENAME:FAILED: +{filename}+ did not pass regex check: +{filename_regex}+')
            return False
        return True

    def should_process_filename(self, filename: str) -> bool:
        #  file is checked to be in the correct form
        # GMOD09Q1.A2019057.08d.latlon.x01y04.6v1.NDVI_anom_S2001-2015.tif.gz
        pattern = r'^GMOD09Q1\.A((?P<year>\d{4})(?P<doy>\d{3})).*(?P<tile>(x\d{1,2}y\d{1,2}))\.6v1.NDVI_anom_S2001-2018\.tif\.gz$'
        match = re.match(pattern, filename, re.IGNORECASE)
        if match:
            groupdict = match.groupdict()
            tile = groupdict['tile']
            # if 'tile' is in tiles
            # self.logger.info(f'Tile: {tile} does not exist in {self.tiles}')
            return bool(self.tiles.count(tile))
        else:
            self.logger.warn(
                f'Potential BUG REPORT. Filename +{filename}+ ws not captured by the tile regex: {pattern}')
            return False

    def parse_catalog(self, response, **kwargs):
        # parse catalog page
        # self.logger.info('PARSE_CATALOG: A response from %s just arrived!' % response.url)
        tableRow: Selector
        for idx, tableRow in enumerate(response.xpath('//tr[count(td)=4]')):
            # ignore the first two iterations, (catalog build)
            if idx < 2:
                continue

            filename = tableRow.xpath('.//a/text()').get()

            # for speed-up reasons we check if the tiles should be processed here,
            # same check is happening over at the 2nd tier level
            if not self.should_process_filename(filename):
                continue

            loader = ItemLoader(item=RemoteSourceItem(), selector=tableRow)

            loader.add_value('filename', filename)
            loader.add_xpath('size', 'td[position()=4]/text()', pos=4)
            loader.add_value('datetime_seen', datetime.utcnow())
            loader.add_value('domain', response.url)
            loader.add_value('url', response.url)
            loader.add_xpath('url', 'td/a/@href')

            yield loader.load_item()


# South Africa
class NDVIAnomalyZAF(NDVIAnomaly):
    tiles = tiles_zaf = ["x21y13", "x22y12", "x22y13", "x23y12", "x23y13"]
    name = EOSourceGroupChoices.S02P02_NDVIA_250M_ZAF_GMOD


# Mozambique
class NDVIAnomalyMOZ(NDVIAnomaly):
    tiles = tiles_moz = ["x23y11", "x23y12", "x24y11", "x24y12"]
    name = EOSourceGroupChoices.S02P02_NDVIA_250M_MOZ_GMOD


# Tunisia
class NDVIAnomalyTUN(NDVIAnomaly):
    tiles = tiles_tun = ["x20y05", "x20y06", "x21y05", "x21y06"]
    name = EOSourceGroupChoices.S02P02_NDVIA_250M_TUN_GMOD


# Kenya
class NDVIAnomalyKEN(NDVIAnomaly):
    tiles = tiles_ken = ["x23y09", "x23y10", "x24y09", "x24y10"]
    name = EOSourceGroupChoices.S02P02_NDVIA_250M_KEN_GMOD


# GHANA
class NDVIAnomalyGHA(NDVIAnomaly):
    tiles = tiles_gha = ["x19y08", "x19y09", "x20y08", "x20y09"]
    name = EOSourceGroupChoices.S02P02_NDVIA_250M_GHA_GMOD


# RWANDA
class NDVIAnomalyRWA(NDVIAnomaly):
    tiles = tiles_rwa = ["x23y10"]
    name = EOSourceGroupChoices.S02P02_NDVIA_250M_RWA_GMOD


# ETHIOPIA
class NDVIAnomalyETH(NDVIAnomaly):
    tiles = tiles_eth = ["x23y08", "x23y09", "x24y08", "x24y09", "x25y09"]
    name = EOSourceGroupChoices.S02P02_NDVIA_250M_ETH_GMOD


class NDVIAnomalyNER(NDVIAnomaly):
    tiles = tiles_ner = ["x19y08", "x19y09", "x20y07", "x20y08", "x20y09", "x21y07", "x21y08"]
    name = EOSourceGroupChoices.S02P02_NDVIA_250M_NER_GMOD
