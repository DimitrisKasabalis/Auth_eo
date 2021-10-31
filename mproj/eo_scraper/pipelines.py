# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from datetime import datetime

from pytz import utc
from scrapy.exceptions import DropItem

from eo_engine.models import EOSource, Credentials, EOSourceStateChoices
from eo_scraper.items import RemoteSourceItem


def try_parse_reference_date(token):
    """ Through heuristics, try to extract a datetime object from the token string.
    Usually the filename. If uncessful, return none"""

    return None


class DefaultPipeline:

    def process_item(self, item: RemoteSourceItem, spider):

        domain = item['domain']
        filename = item['filename']

        try:  # don't process (drop) duplicates
            EOSource.objects.get(filename=filename)
            raise DropItem()
        except EOSource.DoesNotExist:

            try:
                cred_obj = Credentials.objects.get(domain=domain)
            except Credentials.DoesNotExist:
                cred_obj = None
            EOSource.objects.create(
                state=EOSourceStateChoices.AvailableRemotely,
                group=spider.product_name,
                file=None,
                filename=filename,  # unique acts as id
                domain=domain,
                datetime_reference=None,
                filesize_reported=item.get('size'),
                datetime_seen=item.get('datetime_seen', datetime.utcnow().replace(tzinfo=utc)),
                url=item.get('url'),
                credentials=cred_obj
            )

            return item
