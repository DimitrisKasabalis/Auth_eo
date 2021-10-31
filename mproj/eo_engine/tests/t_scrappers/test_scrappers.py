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
