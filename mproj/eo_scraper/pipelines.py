# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from datetime import datetime
from fnmatch import fnmatch

from pytz import utc
from scrapy.exceptions import DropItem

from eo_engine.models import EOSource, Credentials, EOSourceStateChoices, EOSourceGroupChoices
from eo_scraper.items import RemoteSourceItem


def try_parse_reference_date(token):
    """ Through heuristics, try to extract a datetime object from the token string.
    Usually the filename. If uncessful, return none"""

    return None


class DefaultPipeline:

    def process_item(self, item: RemoteSourceItem, spider):

        domain = item['domain']
        filename = item['filename']

        # special rules
        # if this thing starts to bloat up, maybe consider spliting into different
        # pipelines
        if spider.product_name == EOSourceGroupChoices.C_GLS_LAI_300M_V1_GLOB:
            # filenames: c_gls_LAI300-RT0_202012200000_GLOBE_OLCI_V1.1.1.nc
            # Only process LAI300-RTx_202104300000_GLOBE_OLCI_V1.1.y.nc (x=2 or 6, y=1 or 2)
            if fnmatch(filename, 'c_gls_LAI300-RT[2,6]_????????0000_GLOBE_OLCI_V1.1.[1,2].nc'):
                pass
            else:
                raise DropItem()

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
