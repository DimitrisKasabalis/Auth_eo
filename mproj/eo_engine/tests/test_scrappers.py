from . import BaseTest


class TestNDVIScrapper(BaseTest):

    def test_ndvi(self):
        from eo_engine.tasks import task_init_spider

        s = task_init_spider(spider_name='ndvi-1km-v3-spider')
