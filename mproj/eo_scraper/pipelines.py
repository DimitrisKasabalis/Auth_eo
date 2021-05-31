# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from scrapy import Item
from scrapy.exceptions import DropItem

from eo_engine.models import EOSource, Credentials, EOSourceStatusChoices


class DoNothingPipeline:
    def process_item(self, item: Item, spider):

        try:
            # drop duplicates
            EOSource.objects.get(filename=item['filename'])
            raise DropItem()
        except EOSource.DoesNotExist:
            #  new entry

            cred_obj = Credentials.objects.get(domain=item['domain'])
            EOSource.objects.create(
                status=EOSourceStatusChoices.availableRemotely,
                product_group=spider.product_group,
                product=spider.product_name,
                file=None,
                filename=item['filename'],  # unique acts as id
                domain=item['domain'],
                datetime_uploaded=item['datetime_uploaded'],
                datetime_seen=item['datetime_scrapped'],
                url=item['url'],
                credentials=cred_obj
            )

            return item
