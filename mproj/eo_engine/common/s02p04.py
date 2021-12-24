# Django functions for S02P04 work package
from eo_engine.models import EOSource, EOSourceGroupChoices, EOSourceStateChoices, EOSourceGroup


def is_gmod09q1_batch_complete_for_group(eo_source: EOSource, input_group: EOSourceGroup) -> bool:
    """Returns True if all tiles for that group/date is present """

    reference_date = eo_source.reference_date
    qs = EOSource.objects.filter(group=input_group)
    qs = qs.filter(reference_date=reference_date)
    qs = qs.filter(state=EOSourceStateChoices.AVAILABLE_LOCALLY)
    count = qs.count()

    # how many tiles should a date/group have?
    if input_group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_ZAF_GMOD:
        return count == 5
    if input_group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_MOZ_GMOD:
        return count == 4
    if input_group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_TUN_GMOD:
        return count == 4
    if input_group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_KEN_GMOD:
        return count == 4
    if input_group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_GHA_GMOD:
        return count == 4
    if input_group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_RWA_GMOD:
        return count == 1
    if input_group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_ETH_GMOD:
        return count == 5
    if input_group.name == EOSourceGroupChoices.S02P02_NDVIA_250M_NER_GMOD:
        return count == 7

    return False
