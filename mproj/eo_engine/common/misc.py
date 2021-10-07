from typing import List


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
