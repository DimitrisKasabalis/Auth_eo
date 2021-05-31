from . import BaseTest


class TestNDVIScrapper(BaseTest):

    def test_ndvi(self):
        from eo_engine.tasks import task_start_scrape

        s = task_start_scrape(spider_name='ndvi-1km-v3-spider')
