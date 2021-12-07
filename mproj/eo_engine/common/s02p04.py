# Django functions for S02P04 work package
from eo_engine.models import EOSource, EOSourceGroupChoices
from .patterns import GMOD09Q1_DATE_PATTERN


def is_GMOD09Q1_batch_complete(eo_source: EOSource) -> bool:
    """Retuens True if all tiles for that group/date is present """
    match = GMOD09Q1_DATE_PATTERN.match(eo_source.filename)
    group_dict = match.groupdict()
    year = group_dict['year']
    doy = group_dict['doy']
    qs = EOSource.objects.filter(group=eo_source.group)
    qs = qs.filter(filename__contains=f'{year}{doy}')

    # how many tiles should a date/group have?
    if eo_source.group == EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_ZAF:
        return qs.count() == 5
    if eo_source.group == EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_MOZ:
        return qs.count() == 4
    if eo_source.group == EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_TUN:
        return qs.count() == 4
    if eo_source.group == EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_KEN:
        return qs.count() == 4
    if eo_source.group == EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_GHA:
        return qs.count() == 5
    if eo_source.group == EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_RWA:
        return qs.count() == 1
    if eo_source.group == EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_ETH:
        return qs.count() == 5
    if eo_source.group == EOSourceGroupChoices.GMOD09Q1_NDVI_ANOM_NER:
        return qs.count() == 6

    return False
