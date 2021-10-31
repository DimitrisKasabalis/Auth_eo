from django.utils.timezone import now

from . import Credentials
from .eo_source import EOSource, EOSourceGroupChoices, EOSourceStateChoices


def create_wapor_object(filename: str) -> EOSource:
    """ Throws IntegrityError if entry exists """
    from eo_engine.common.contrib.waporv2 import WAPORRemoteVariable
    var = WAPORRemoteVariable.from_filename(filename)
    group = getattr(EOSourceGroupChoices, f'WAPOR_{var.product_id}_{var.area.upper()}')

    return EOSource.objects.create(
        state=EOSourceStateChoices.AvailableRemotely,
        group=group,
        filename=filename,
        domain='wapor',
        datetime_seen=now(),
        filesize_reported=0,
        datetime_reference=var._start_date,
        url='wapor://',
        credentials=Credentials.objects.filter(domain='WAPOR').first(),
    )
