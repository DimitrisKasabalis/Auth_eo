# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
from urllib.parse import urlsplit

import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst


# from w3lib.html import remo

def get_extension(value: str):
    return value.split('.')[-1]


def get_domain_of_url(value: str):
    o = urlsplit(value)
    return o.netloc


def drop_query_from_url(value: str):
    return value.split('?')[0]


def parse_datetime_str(value: str):
    from dateutil.parser import parse

    return parse(value)


class VitoEODataItem(scrapy.Item):
    product_name = scrapy.Field(
        output_processor=TakeFirst())
    filename = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst())
    extension = scrapy.Field(
        input_processor=MapCompose(str.strip, get_extension),
        output_processor=TakeFirst())
    size = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst())
    domain = scrapy.Field(
        input_processor=MapCompose(get_domain_of_url),
        output_processor=TakeFirst())
    datetime_uploaded = scrapy.Field(
        input_processor=MapCompose(str.strip, parse_datetime_str),
        output_processor=TakeFirst())
    datetime_scrapped = scrapy.Field(output_processor=TakeFirst())
    url = scrapy.Field(
        input_processor=MapCompose(drop_query_from_url),
        output_processor=Join(separator='')
    )
