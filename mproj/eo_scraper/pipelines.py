# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from datetime import datetime
from fnmatch import fnmatch
from scrapy.exceptions import DropItem

from eo_engine.models import EOSource, Credentials, EOSourceStateChoices, EOSourceGroupChoices, EOSourceGroup
from eo_scraper.items import RemoteSourceItem


def try_parse_reference_date(token):
    """ Through heuristics, try to extract a datetime object from the token string.
    Usually the filename. If uncessful, return none"""

    return None


class DefaultPipeline:

    # noinspection PyMethodMayBeStatic
    def process_item(self, item: RemoteSourceItem, spider):

        domain = item['domain']
        filename = item['filename']

        if not spider.is_expected_filename(filename):
            raise DropItem()

        if not spider.should_process_filename(filename):
            raise DropItem()

        try:  # don't process (drop) duplicates
            EOSource.objects.get(filename=filename)
            raise DropItem()
        except EOSource.DoesNotExist:

            try:
                cred_obj = Credentials.objects.get(domain=domain)
            except Credentials.DoesNotExist:
                cred_obj = None

            eo_source_group = EOSourceGroup.objects.get(name=spider.name)
            datetime_ref = spider.date_reference_from_filename(filename)
            eo_source_object = EOSource.objects.create(
                state=EOSourceStateChoices.AVAILABLE_REMOTELY,
                group=eo_source_group,
                file=None,
                filename=filename,  # unique, acts as id
                domain=domain,
                reference_date=datetime_ref,
                filesize_reported=item.get('size', -1),
                datetime_seen=item.get('datetime_seen', datetime.utcnow()),
                url=item.get('url'),
                credentials=cred_obj,
            )

            return item
