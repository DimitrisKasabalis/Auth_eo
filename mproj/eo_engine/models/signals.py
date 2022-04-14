from django.db.models.signals import post_save
from django.dispatch import receiver

from eo_engine.models import (
    EOSource,
    EOSourceStateChoices,
    EOProduct,
    EOProductStateChoices,
    Pipeline
)
from eo_engine.models.eo_product import should_create


@receiver(post_save, sender=EOSource, weak=False, dispatch_uid='eosource_post_save_handler')
def eosource_post_save_handler(instance: EOSource, **kwargs):
    """ Post save logic goes here. ie an asset is now available locally, are there products that can be made?"""

    eo_source = instance
    # if asset is not local, ignore
    if eo_source.state != EOSourceStateChoices.AVAILABLE_LOCALLY:
        return

    # it's used for the output filenames
    yyyymmdd = eo_source.reference_date.strftime('%Y%m%d')
    yyyy = eo_source.reference_date.strftime('%Y')

    # pipelines that are using this source as input:
    pipelines = Pipeline.objects.filter(input_groups__in=eo_source.group.all())

    for pipeline in pipelines:
        # a pipeline should have one output group
        output_group = pipeline.output_group
        # but pipelines could have multiple input groups
        for input_group in pipeline.input_groups.all():
            input_group_eosource = input_group.as_eosource_group()

            if input_group_eosource is None:
                from eo_engine.errors import AfriCultuReSError
                raise AfriCultuReSError('All eo_source group should be castable as eo_source_group')

            # so cast to Product grp
            if hasattr(output_group, 'as_eo_product_group'):
                output_group = output_group.as_eo_product_group()

            output_filename = pipeline.output_filename(**{'YYYYMMDD': yyyymmdd, 'YYYY': yyyy})

            prod, created = EOProduct.objects.get_or_create(
                filename=output_filename,
                group=output_group,
                reference_date=eo_source.reference_date
            )

            # generalised solution
            # are all the files that are supposed to be available ?
            all_of_this_kind = EOSource.objects.filter(group__in=pipeline.input_groups.all(),
                                                       reference_date=eo_source.reference_date)
            all_of_this_kind_that_are_available = all_of_this_kind.filter(state=EOSourceStateChoices.AVAILABLE_LOCALLY)
            if all_of_this_kind.count() == all_of_this_kind_that_are_available.count():
                prod.state = EOProductStateChoices.AVAILABLE
            else:
                prod.state = EOProductStateChoices.MISSING_SOURCE

            prod.save()


@receiver(post_save, sender=EOProduct, weak=False, dispatch_uid='eoproduct_post_save_handler')
def eoproduct_post_save_handler(instance: EOProduct, **kwargs):
    from eo_engine.models import Pipeline
    eo_product = instance
    # don't do anything is the product state is not READY
    if eo_product.state != EOProductStateChoices.READY:
        return

    yyyymmdd = eo_product.reference_date.strftime('%Y%m%d')
    yyyy = eo_product.reference_date.strftime('%Y')
    # find all the pipelines that use this product-group as input
    pipelines = Pipeline.objects.filter(input_groups__eoproduct=eo_product)
    for pipeline in pipelines:

        # apply special rules if this should be made or not
        if not should_create(pipeline, eo_product):
            continue

        # output templates could be YYYYMMDD or YYYY
        # they're defined when an pipeline is created
        output_filename = pipeline.output_filename(**{'YYYYMMDD': yyyymmdd, 'YYYY': yyyy})

        output_group = pipeline.output_group.as_eoproduct_group()

        prod, created = EOProduct.objects.get_or_create(
            filename=output_filename,
            group=output_group,
            reference_date=eo_product.reference_date
        )
        if created:
            prod.state = EOProductStateChoices.AVAILABLE
        prod.save()


__all__ = [
    'eosource_post_save_handler',
    'eoproduct_post_save_handler'
]
