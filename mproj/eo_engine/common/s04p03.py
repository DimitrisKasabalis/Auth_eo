from eo_engine.models import EOSource, EOSourceGroup, EOSourceStateChoices


def is_s04p03_fld_complete_for_group(eo_source: EOSource, input_group: EOSourceGroup) -> bool:
    # this batch is comprised by 25 entries.

    reference_date = eo_source.reference_date
    qs = EOSource.objects.filter(group=input_group)
    qs = qs.filter(reference_date=reference_date)
    qs = qs.filter(state=EOSourceStateChoices.AVAILABLE_LOCALLY)
    count = qs.count()

    return count == 25
