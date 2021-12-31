from logging import Logger

from celery.utils.log import get_task_logger
from django.utils.timezone import now
from typing import TypedDict, Union

from eo_engine.common.misc import str_to_date
from eo_engine.common import RemoteFile
from eo_engine.errors import AfriCultuReSFileDoesNotExist, AfriCultuReSFileInUse
from eo_engine.models import CrawlerConfiguration
from eo_engine.models import Credentials, EOSourceGroup
from eo_engine.models import EOProduct, EOProductStateChoices
from eo_engine.models import EOSource, EOSourceStateChoices

DeletedReport = TypedDict('DeletedReport', {'eo_source': int, 'eo_product': int})

logger: Logger = get_task_logger(__name__)


def delete_eo_product(eo_product_pk: int) -> DeletedReport:
    """Delete a EOProduct. Check up and down if there's anything else to delete.
        This function most probably will run from the WEB platform.
    """

    self = EOProduct.objects.get(pk=eo_product_pk)
    deb_eo_products = EOProduct.objects.filter(eo_products_inputs=self)

    # Step 1. Sanity check. Does this file exists? Is this file is 'locked'?

    # file is missing
    if not bool(self.file):
        raise AfriCultuReSFileDoesNotExist("The file does not exist to delete.")

    # is self being 'generated'?
    if self.state in (EOProductStateChoices.GENERATING, EOProductStateChoices.SCHEDULED):
        raise AfriCultuReSFileInUse(
            'This file is scheduled to generation, or is made now. Cannot delete at this moment')

    # Are any other products that scheduled/generated need this file?
    active_eo_products = deb_eo_products.filter(
        state__in=(EOProductStateChoices.SCHEDULED,
                   EOProductStateChoices.GENERATING))
    if active_eo_products.exists():
        raise AfriCultuReSFileInUse('Cannot delete the file. It is needed for a scheduled/active procedure')

    # Step 2 The file exists and safe to delete. Is there anything else that we can delete?

    safe_to_delete_deb_eo_products = deb_eo_products.filter(
        state__in=(EOProductStateChoices.FAILED,
                   EOProductStateChoices.AVAILABLE)
    )
    deleted_eo_products = safe_to_delete_deb_eo_products.count()
    if safe_to_delete_deb_eo_products.exists():
        safe_to_delete_deb_eo_products.delete()

    # deal with self
    # Are all inputs present and available
    input_eo_source = self.eo_sources_inputs.all()
    input_eo_product = self.eo_products_inputs.all()

    safe_self_to_remove_row = False

    if input_eo_source.exists():  # Has self made by an EOSource?
        if input_eo_source.count() == input_eo_source.filter(
                state=EOSourceStateChoices.AVAILABLE_LOCALLY).count():
            # keep safe_self_to_remove_row false
            pass
        else:
            safe_self_to_remove_row = True

    if input_eo_product.exists():
        if input_eo_product.count() == input_eo_product.filter(
                state=EOProductStateChoices.READY).count():
            # keep safe_self_to_remove_row false
            pass
        else:
            safe_self_to_remove_row = True

    # remove actual file
    self.file.delete()
    if safe_self_to_remove_row:
        self.delete()
    else:
        self.state = EOProductStateChoices.IGNORE
        self.save()

    return {"eo_source": 1,
            # +1 because we delete self
            "eo_product": deleted_eo_products + 1
            }


def delete_eo_source(eo_source_pk: int) -> DeletedReport:
    """Delete EOSource file if made, and mark the row as IGNORED.
    Take care for depended EOProducts,

    Raises errors when file does not exists or a dep file is used
    """

    eo_source = EOSource.objects.get(pk=eo_source_pk)
    deb_eo_products = EOProduct.objects.filter(eo_sources_inputs=eo_source)
    if not bool(eo_source.file):
        raise AfriCultuReSFileDoesNotExist("The file does not exist to delete.")

    active_eo_products = deb_eo_products.filter(
        state__in=(EOProductStateChoices.SCHEDULED,
                   EOProductStateChoices.GENERATING))
    to_delete_eo_products = deb_eo_products.filter(
        state__in=(EOProductStateChoices.AVAILABLE, EOProductStateChoices.FAILED)
    )

    if active_eo_products.exists():
        raise AfriCultuReSFileInUse('Cannot delete the file. It is being used for a scheduled/on-going procedure')

    deleted_eo_products = to_delete_eo_products.count()
    to_delete_eo_products.delete()

    eo_source.file.delete(save=False)
    eo_source.state = EOSourceStateChoices.IGNORE
    eo_source.save()

    return {"eo_source": 1, "eo_product": deleted_eo_products}


def add_to_db(remote_file: RemoteFile, eo_source_group: Union[str, EOSourceGroup]):
    """ Adds entry in the database. Checks if entry exists based on filename"""
    if isinstance(eo_source_group, EOSourceGroup):
        group = eo_source_group
    else:
        group = EOSourceGroup.objects.get(name=eo_source_group)
    reference_date = str_to_date(remote_file.filename, group.date_regex)

    obj, created = EOSource.objects.get_or_create(
        filename=remote_file.filename,
        defaults={
            'domain': remote_file.domain,
            'url': remote_file.url,
            'reference_date': reference_date,
            'credentials': Credentials.objects.get(domain=remote_file.domain),
            'datetime_seen': now(),
            'filesize_reported': remote_file.filesize_reported,
        }
    )

    obj.group.add(group)
