import functools
from typing import List

from eo_engine.common.tasks import is_process_task
from eo_engine.errors import AfriCultuReSError


def get_spider_loader():
    """Returns a list of all the available spiders."""
    # requires SCRAPY_SETTINGS_MODULE env variableX

    from scrapy.spiderloader import SpiderLoader
    from scrapy.utils.project import get_project_settings
    # currently it's set in DJ's manage.py
    scrapy_settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(scrapy_settings)

    return spider_loader


def get_crawler_process():
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    scrapy_settings = get_project_settings()
    return CrawlerProcess(scrapy_settings)


def list_spiders() -> List[str]:
    spider_loader = get_spider_loader()

    return spider_loader.list()


#  https://realpython.com/primer-on-python-decorators/#a-few-real-world-examples
def check_params(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if is_process_task(func.__name__) and 'eo_product_pk' not in kwargs.keys():
            raise AfriCultuReSError('eo_product_pk param is missing from the task. Did you forget it? ')
        return func(*args, **kwargs)

    return wrapper
