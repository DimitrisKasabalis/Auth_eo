from eo_engine.tests import BaseTest
from eo_engine.tasks import task_init_spider


class TestNDVIScrapper(BaseTest):

    def test_scrapper(self):
        task_init_spider(spider_name='ndvi-1km-v3-spider')


class TestFtpFloodLightViiRScrapper(BaseTest):
    spider_name = 'ftp-floodlight-viir'

    def test_scrapper(self):
        task_init_spider(spider_name=self.spider_name)

        print('--done--')
