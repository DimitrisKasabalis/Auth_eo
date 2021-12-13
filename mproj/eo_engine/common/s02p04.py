# Django functions for S02P04 work package
from eo_engine.models import EOSource, EOSourceGroupChoices, EOSourceStateChoices


def is_gmod09q1_batch_complete(eo_source: EOSource) -> bool:
    """Returns True if all tiles for that group/date is present """

    qs = EOSource.objects.filter(group=eo_source.group)
    qs = qs.filter(reference_date=eo_source.reference_date)
    qs = qs.filter(state=EOSourceStateChoices.AVAILABLE_LOCALLY)
    count = qs.count()

    # how many tiles should a date/group have?
    if eo_source.group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_ZAF_GMOD:
        return count == 5
    if eo_source.group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_MOZ_GMOD:
        return count == 4
    if eo_source.group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_TUN_GMOD:
        return count == 4
    if eo_source.group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_KEN_GMOD:
        return count == 4
    if eo_source.group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_GHA_GMOD:
        return count == 5
    if eo_source.group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_RWA_GMOD:
        return count == 1
    if eo_source.group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_ETH_GMOD:
        return count == 5
    if eo_source.group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_NER_GMOD:
        return count == 6

    return False
