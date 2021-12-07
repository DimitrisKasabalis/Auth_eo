import re
from datetime import datetime
from typing import List, Match

from celery.utils.log import get_task_logger
from itemloaders import ItemLoader
from scrapy import Selector
from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor, IGNORED_EXTENSIONS as DEFAULT_IGNORED_EXTENSIONS
from scrapy.spiders import CrawlSpider, Rule, Spider

from eo_engine.common.patterns import GMOD09Q1_FILE_REGEX
from eo_engine.models import EOSourceGroupChoices
from eo_engine.models.other import EOSourceMeta
from eo_scraper.items import RemoteSourceItem

# We don't want to 'download' the file
IGNORED_EXTENSIONS = DEFAULT_IGNORED_EXTENSIONS + ['tif.gz', ]

# matches https://gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/...
page_regex = re.compile(r'https://.*/GMOD09Q1/.*/$')
page_regex = re.compile(r'^https://gimms\.gsfc\.nasa\.gov/MODIS/std/GMOD09Q1/tif/NDVI_anom_S2001-2015(/(?P<year>\d{4})?(/(?P<doy>\d{3}))?)?/$')

# matches https://gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI_anom_S2001-2015/2001/073/
# which is the catalog page
catalog_page_regex = re.compile(r'https://.*/GMOD09Q1/.*/(?P<year>\d{4})/(?P<date>\d{3})/$')

logger = get_task_logger(__name__)


class NDVIAnomaly(Spider):
    tiles: List[str]
    start_urls = [
        f"https://gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI_anom_S2001-2015/"
    ]

    def get_group(self):
        raise NotImplementedError()

    # rules: List[Rule] = (
    #     Rule(LinkExtractor(allow=(page_regex,), deny=(catalog_page_regex,)), callback='parse'),
    #     # Rule(LinkExtractor(allow=(catalog_page_regex,)), callback='parse_catalog', follow=False)
    # )

    def __init__(self, *args, **kwargs):
        super(NDVIAnomaly, self).__init__(*args, **kwargs)

    def get_tiles(self) -> List[str]:
        return self.tiles

    def parse(self, response: Response, **kwargs):
        print(f'hello from response: {response.url}')
        params = EOSourceMeta.objects.get(group=self.get_group())
        start_date = params.from_date
        min_year = start_date.year
        min_doy = int(start_date.strftime('%j'))
        all_hrefs = list(response.copy().xpath('//a/@href').getall())
        print(f"all_ahrefs: {all_hrefs}")

        href: str
        for href in all_hrefs:
            target_url = response.url + href
            logger.warn(href)
            logger.warn(f'+++target_url+++ {target_url}')
            match = page_regex.match(target_url)
            logger.warn(match)
            if match:
                logger.warn(f'target_url (After match gt10): {target_url}')
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
                    f'\ntarget_url:{target_url}\nresp_year: {resp_year},min_year: {min_year}\nresp_doy: {resp_doy}, min_doy: {min_doy}')

                if resp_year is None:
                    return None

                # year page
                if resp_year >= min_year:
                    if resp_doy is None:
                        yield response.follow(href, callback=self.parse)
                    elif resp_doy >= min_doy:
                        # catalog page, that has all the links to the files
                        yield response.follow(target_url, callback=self.parse_catalog)

    def parse_catalog(self, response, **kwargs):
        # parse catalog page
        self.logger.info('PARSE_CATALOG: A response from %s just arrived!' % response.url)
        tableRow: Selector
        for idx, tableRow in enumerate(response.xpath('//tr[count(td)=4]')):
            # ignore the first two iterations
            if idx < 2:
                continue
            filename = tableRow.xpath('.//a/text()').get()
            tile_match = GMOD09Q1_FILE_REGEX.match(filename)
            if tile_match is None:
                continue

            tile_match: Match
            tile: str = tile_match.groupdict()['tile']
            if tile not in self.tiles:
                continue

            loader = ItemLoader(item=RemoteSourceItem(), selector=tableRow)

            # loader.add_value('product_name', self.product_name)
            loader.add_value('filename', filename)
            # loader.add_xpath('extension', 'td[position()=1]/text()', pos=1)
            loader.add_xpath('size', 'td[position()=4]/text()', pos=4)
            # loader.add_xpath('datetime_uploaded', 'td[position()=3]/text()', pos=3)
            loader.add_value('datetime_seen', datetime.utcnow())
            loader.add_value('domain', response.url)
            loader.add_value('url', response.url)
            loader.add_xpath('url', 'td/a/@href')

            yield loader.load_item()


# South Africa
class NDVIAnomalyZAF(NDVIAnomaly):
    tiles = tiles_zaf = ["x21y13", "x22y12", "x22y13", "x23y12", "x23y13"]
    name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_ZAF
    product_name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_ZAF

    def get_group(self) -> str:
        print(self.product_name.value)
        return self.product_name.value


# Mozambique
class NDVIAnomalyMOZ(NDVIAnomaly):
    tiles = tiles_moz = ["x23y11", "x23y12", "x24y11", "x24y12"]
    name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_MOZ
    product_name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_MOZ

    def get_group(self):
        return self.product_name


# Tunisia
class NDVIAnomalyTUN(NDVIAnomaly):
    tiles = tiles_tun = ["x20y05", "x20y06", "x21y05", "x21y06"]
    name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_TUN
    product_name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_TUN

    def get_group(self):
        return self.product_name


# Kenya
class NDVIAnomalyKEN(NDVIAnomaly):
    tiles = tiles_ken = ["x23y09", "x23y10", "x24y09", "x24y10"]
    name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_KEN
    product_name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_KEN

    def get_group(self):
        return self.product_name


# GHANA
class NDVIAnomalyGHA(NDVIAnomaly):
    tiles = tiles_gha = ["x23y08", "x23y09", "x24y08", "x24y09", "x25y09"]
    name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_GHA
    product_name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_GHA

    def get_group(self):
        return self.product_name


# RWANDA
class NDVIAnomalyRWA(NDVIAnomaly):
    tiles = tiles_rwa = ["x23y10"]
    name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_RWA
    product_name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_RWA

    def get_group(self):
        return self.product_name


# ETHIOPIA
class NDVIAnomalyETH(NDVIAnomaly):
    tiles = tiles_eth = ["x23y08", "x23y09", "x24y08", "x24y09", "x25y09"]
    name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_ETH
    product_name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_ETH

    def get_group(self):
        return self.product_name


class NDVIAnomalyNER(NDVIAnomaly):
    tiles = tiles_ner = ["x19y08", "x19y09", "x20y07", "x20y08", "x20y09", "x21y07", "x21y08"]
    name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_NER
    product_name = EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_NER

    def get_group(self):
        return self.product_name
