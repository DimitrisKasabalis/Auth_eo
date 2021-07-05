# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import re
from urllib.parse import urlsplit

import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst

units = {"B": 1, "KB": 2 ** 10, "MB": 2 ** 20, "GB": 2 ** 30, "TB": 2 ** 40}


def parse_size(size):
    """

    ('1024b', 1024)
    ('10.43 KB', 10680)
    ('11 GB', 11811160064)
    ('343.1 MB', 359766425)
    ('10.43KB', 10680)
    ('11GB', 11811160064)
    ('343.1MB', 359766425)
    ('10.43 kb', 10680)
    ('11 gb', 11811160064)
    ('343.1 mb', 359766425)
    ('10.43kb', 10680)
    ('11gb', 11811160064)
    ('343.1mb', 359766425)
    """
    try:
        return int(size)
    except ValueError:
        pass
    try:
        return float(size)
    except ValueError:
        pass

    size = size.upper()
    # print("parsing size ", size)
    if not re.match(r' ', size):
        size = re.sub(r'([KMGT]?B)', r' \1', size)
    number, unit = [string.strip() for string in size.split()]
    return int(float(number) * units[unit])


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


class RemoteSourceItem(scrapy.Item):
    filename: str = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst())
    size: str = scrapy.Field(
        input_processor=MapCompose(str.strip, parse_size),
        output_processor=TakeFirst())
    domain: str = scrapy.Field(
        input_processor=MapCompose(get_domain_of_url),
        output_processor=TakeFirst())
    datetime_seen: str = scrapy.Field(output_processor=TakeFirst())
    url: str = scrapy.Field(
        input_processor=MapCompose(drop_query_from_url),
        output_processor=Join(separator='')
    )
