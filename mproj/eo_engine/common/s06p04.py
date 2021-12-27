from eo_engine.models import EOSource, Pipeline, EOSourceStateChoices


def is_s06p04_wapor_batch_complete_for_group(eo_source: EOSource, pipeline: Pipeline) -> bool:
    reference_date = eo_source.reference_date
    # pipeline that has this group as input
    input_groups = pipeline.input_groups.all()

    qs = EOSource.objects.filter(group__in=input_groups, reference_date=reference_date,
                                 state=EOSourceStateChoices.AVAILABLE_LOCALLY)
    return qs.count() == input_groups.count()
