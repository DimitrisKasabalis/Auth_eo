from django.utils.datetime_safe import datetime
from itemloaders import ItemLoader
from scrapy import Selector

from eo_engine.models import EOSourceGroupChoices
from eo_scraper.items import RemoteSourceItem
from eo_scraper.spiders.abstract_spiders import AfricultureCrawlerMixin
from scrapy.spiders import Spider


class ETAv5Spider(Spider, AfricultureCrawlerMixin):
    name = EOSourceGroupChoices.S06P04_ETAnom_5KM_M_GLOB_SSEBOP

    start_urls = [
        'https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/global/monthly/etav5/anomaly/downloads/'
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.crawler_settings = self.get_crawler_settings()
        self.group_settings = self.get_group_settings()

    def should_process_filename(self, filename: str) -> bool:
        datetime_ref = self.date_reference_from_filename(filename)
        print(datetime_ref)
        return datetime_ref >= self.from_date

    def parse(self, response, **kwargs):
        selector: Selector
        for idx, selector in enumerate(response.xpath('//a')):
            filename = selector.xpath('.//@href').get()  # get the href attribute value

            if not self.is_expected_filename(filename):
                continue

            loader = ItemLoader(item=RemoteSourceItem(), selector=selector)
            loader.add_value('filename', filename)
            loader.add_value('datetime_seen', datetime.utcnow())
            loader.add_value('domain', response.url)
            # these two are Joined
            loader.add_value('url', response.url)
            loader.add_xpath('url', filename)

            yield loader.load_item()
