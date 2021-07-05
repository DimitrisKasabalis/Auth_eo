# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from datetime import datetime
from typing import Union
from fnmatch import fnmatch
from pytz import utc
from scrapy.exceptions import DropItem

from eo_engine.common import parse_copernicus_name
from eo_engine.models import EOSource, Credentials, EOSourceStatusChoices
from eo_scraper.items import RemoteSourceItem


class DefaultPipeline:

    def process_item(self, item: RemoteSourceItem, spider):

        domain = item['domain']
        filename = item['filename']
        if fnmatch(filename.lower(), 'c_gls*.nc'):
            product_elements = parse_copernicus_name(filename)
        else:
            raise
        try:  # don't process (drop) duplicates

            # if not found it will raise an exception
            EOSource.objects.get(filename=filename)
            raise DropItem()
        except EOSource.DoesNotExist:

            cred_obj = Credentials.objects.get(domain=domain)
            EOSource.objects.create(
                status=EOSourceStatusChoices.availableRemotely,
                product=spider.product_name,
                file=None,
                filename=filename,  # unique acts as id
                domain=domain,
                datetime_reference=product_elements.datetime,
                filesize_reported=item.get('size'),
                datetime_seen=item.get('datetime_seen', datetime.utcnow().replace(tzinfo=utc)),
                url=item.get('url'),
                credentials=cred_obj
            )

            return item
