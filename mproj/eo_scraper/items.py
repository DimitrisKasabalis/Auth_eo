# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import re
import scrapy
from celery.utils.log import get_task_logger
from datetime import datetime, date as dt_date
from itemloaders.processors import Join, MapCompose, TakeFirst
from pytz import timezone
from typing import Optional, Union
from urllib.parse import urlsplit

units = {"B": 1, "KB": 2 ** 10, "MB": 2 ** 20, "GB": 2 ** 30, "TB": 2 ** 40}

logger = get_task_logger(__name__)
utc = timezone('UTC')


def parse_size(str_token: str):
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
        return int(str_token)
    except ValueError:
        pass
    try:
        return float(str_token)
    except ValueError:
        pass

    str_token = str_token.upper()
    # print("parsing size ", size)
    try:
        if not re.match(r' ', str_token):
            str_token = re.sub(r'([KMGT]?B)', r' \1', str_token)
        number, unit = [string.strip() for string in str_token.split()]
    except ValueError:
        # couldn't parse the value
        # logger.warning('SCRAPER:ITEM-PROCESSING:parse_size Could not parse string to bytes.')
        return -1
    return int(float(number) * units[unit])


def get_extension(value: str):
    return value.split('.')[-1]


def get_domain_of_url(value: str):
    o = urlsplit(value)
    return o.netloc


def drop_query_from_url(value: str):
    return value.split('?')[0]


def set_utc_timezone(dt_object: Union[datetime, dt_date]):
    if isinstance(dt_object, dt_date):
        return dt_object
    return dt_object.replace(tzinfo=utc)


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
    datetime_reference: Optional[datetime] = scrapy.Field(
        input_processor=MapCompose(set_utc_timezone),
        output_processor=TakeFirst())
    datetime_seen: str = scrapy.Field(output_processor=TakeFirst())
    url: str = scrapy.Field(
        input_processor=MapCompose(drop_query_from_url),
        output_processor=Join(separator='')
    )
