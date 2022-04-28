from eo_engine.models import EOSourceGroupChoices, CrawlerConfiguration
from eo_engine.tests import BaseTest
from eo_engine.tasks import task_init_spider


class TestNDVI300mV2Spider(BaseTest):

    def test_scrapper(self):
        task_init_spider(spider_name='c_gsl_ndvi300-v2-glob-spider')


class TestNDVI1kmV3Spider(BaseTest):

    def test_scrapper(self):
        task_init_spider(spider_name='ndvi-1km-v3-spider')


class TestFtpGlobalLandWB100m(BaseTest):
    spider_name = 'ftp-spider-wb300'

    def test_scrapper(self):
        task_init_spider(spider_name=self.spider_name)

        print('--done--')


class TestFtpGlobalLandWB300m(BaseTest):
    spider_name = 'ftp-spider-wb300'

    def test_scrapper(self):
        task_init_spider(spider_name=self.spider_name)

        print('--done--')


class TestFtpFloodLightViiRScrapper(BaseTest):
    spider_name = 'ftp-floodlight-viir'

    def test_scrapper(self):
        task_init_spider(spider_name=self.spider_name)

        print('--done--')


class ETAv5Spider(BaseTest):
    spider_name = 'S06P04_ETAnom_5KM_M_GLOB_SSEBOP'

    def test_scrapper(self):
        from datetime import datetime
        CrawlerConfiguration.objects.create(
            group=self.spider_name,
            from_date=datetime(2015, 1, 1).date()
        )
        task_init_spider(spider_name=self.spider_name, from_date=datetime(2015, 1, 1).date())

        print('--done--')
