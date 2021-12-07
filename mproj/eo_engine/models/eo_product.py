import os
from typing import Any, TypedDict

from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver


class EOProductGroupChoices(models.TextChoices):
    # NDVI Products
    AGRO_NDVI_1KM_V3_AFR = "AGRO_NDVI_1KM_V3_AFR", 'AUTH/AGRO/NDVI 1km, V3 Africa'
    AGRO_NDVI_300M_V3_AFR = "AGRO_NDVI_300M_V3_AFR", 'AUTH/AGRO/NDVI 300m, V3 Africa'

    # VCI
    AGRO_VCI_1KM_V2_AFR = 'AGRO_VCI_1KM_V2_AFR', 'AUTH/AGRO/VCI 1km, V2 Africa'

    # LAI
    AGRO_LAI_300M_V1_AFR = "AGRO_LAI_300M_V1_AFR", 'AUTH/AGRO/LAI 300m, V1 Africa'
    AGRO_LAI_1KM_V2_AFR = "AGRO_LAI_1KM_V2_AFR", 'AUTH/AGRO/LAI 1km, V2 Africa'

    # WB Products
    AGRO_WB_100M_ETH = 'AGRO_WB_100M_ETH', 'AGRO_WB_100_M_ETH'
    AGRO_WB_100M_GHA = 'AGRO_WB_100M_GHA', 'AGRO_WB_100_M_GHA'
    AGRO_WB_100M_KEN = 'AGRO_WB_100M_KEN', 'AGRO_WB_100_M_KEN'
    AGRO_WB_100M_MOZ = 'AGRO_WB_100M_MOZ', 'AGRO_WB_100_M_MOZ'
    AGRO_WB_100M_NER = 'AGRO_WB_100M_NER', 'AGRO_WB_100_M_NER'
    AGRO_WB_100M_RWA = 'AGRO_WB_100M_RWA', 'AGRO_WB_100_M_RWA'
    AGRO_WB_100M_TUN = 'AGRO_WB_100M_TUN', 'AGRO_WB_100_M_TUN'
    AGRO_WB_100M_ZAF = 'AGRO_WB_100M_ZAF', 'AGRO_WB_100_M_ZAF'

    AGRO_WB_300M_V2_AFR = 'AGRO_WB_300M_V2_AFR'

    # LSA-SAF
    MSG_3KM_AFR = 'MSG_3KM_AFR', 'LSA-SAF 3km Africa'

    # VIIS-1Day for S04P03
    VIIRS_1DAY_AFR = 'VIIRS_1DAY_AFR', 'VIIRS 1day AFR'

    # NDVIA Anomaly
    AGRO_NDVIA_TUN = 'AGRO_NDVIA_TUN', 'AGRO_NDVIA_TUN'
    AGRO_NDVIA_RWA = 'AGRO_NDVIA_RWA', 'AGRO_NDVIA_RWA'
    AGRO_NDVIA_ETH = 'AGRO_NDVIA_ETH', 'AGRO_NDVIA_ETH'
    AGRO_NDVIA_ZAF = 'AGRO_NDVIA_ZAF', 'AGRO_NDVIA_ZAF'
    AGRO_NDVIA_NER = 'AGRO_NDVIA_NER', 'AGRO_NDVIA_NER'
    AGRO_NDVIA_GHA = 'AGRO_NDVIA_GHA', 'AGRO_NDVIA_GHA'
    AGRO_NDVIA_MOZ = 'AGRO_NDVIA_MOZ', 'AGRO_NDVIA_MOZ'
    AGRO_NDVIA_KEN = 'AGRO_NDVIA_KEN', 'AGRO_NDVIA_KEN'


class EOProductStateChoices(models.TextChoices):
    Available = 'Available', "AVAILABLE for generation."
    Scheduled = "Scheduled", "SCHEDULED For generation."
    Failed = 'Failed', 'Generation was attempted but FAILED'
    Generating = 'Generating', "GENERATING..."
    Ignore = 'Ignore', "Skip generation (Ignored) ."
    Ready = 'Ready', "Product is READY."
    MISSING_SOURCE = 'MISSING_SOURCE', "Some or all EOSouce(s) Are Not Available"


def _upload_to(instance: 'EOProduct', filename):
    root_folder = 'products'
    full_path = f"{root_folder}/{instance.output_folder}/{filename}"

    # remove if exists, alternative solution would be to define another storage class
    actual_path = os.path.join(settings.MEDIA_ROOT, full_path)
    if os.path.exists(actual_path):
        os.remove(actual_path)

    return full_path


class EOProduct(models.Model):
    filename = models.TextField(unique=True)
    output_folder = models.TextField()
    group = models.CharField(max_length=255, choices=EOProductGroupChoices.choices)
    datetime_creation = models.DateTimeField(null=True)
    file = models.FileField(upload_to=_upload_to, null=True, max_length=2048)

    eo_sources_inputs = models.ManyToManyField(
        "eo_engine.EOSource",
        related_query_name='eo_product',
        related_name='eo_products',
        symmetrical=False  # A is input to B but B is not Input to A
    )
    eo_products_inputs = models.ManyToManyField(
        'self',
        related_name='depended_eo_product',
        related_query_name='depended_eo_products',
        symmetrical=False  # A is input to B but B is not Input to A
    )

    timestamp = models.DateTimeField(auto_now_add=True)
    state = models.CharField(max_length=255, choices=EOProductStateChoices.choices,
                             default=EOProductStateChoices.Available)
    task_name = models.CharField(max_length=255)
    task_kwargs = models.JSONField(default=dict)

    class Meta:
        ordering = ["group", "filename"]

    def __str__(self):
        return f"{self.__class__.__name__}/{self.filename}/{self.state}/{self.id}"

    def inputs(self) -> TypedDict('EO_SOURCE', {'eo_sources': Any, 'eo_product': Any}):
        """Return a dictionary of this products inputs. """
        return {
            'eo_sources': self.eo_sources_inputs.all(),
            'eo_product': self.eo_products_inputs.all()
        }

    def is_ignored(self) -> bool:
        # According to rules, is this products ignored?
        from eo_engine.models import FunctionalRules
        ignore_rules = FunctionalRules.objects.get(domain='ignore_rules').rules
        ignore_product_rules = ignore_rules['ignore_products']
        return self.group in ignore_product_rules

    def make_product(self, as_task: bool = False):
        from eo_engine import tasks
        func = getattr(tasks, self.task_name)  # get ref to task function using its name
        self.state = EOProductStateChoices.Generating
        self.save()
        if func is None:
            raise NotImplementedError('No generation method has been defined for this product.')

        if as_task:
            # we pass the the id of this project.
            task = func.s(eo_product_pk=self.pk, **self.task_kwargs)
            task_id = task.apply_async()
            # link to task
            return task_id
        else:
            try:
                # we pass the actual reference of this obj
                func(eo_product_pk=self, **self.task_kwargs)
                self.save()
            except Exception as e:
                self.state = EOProductStateChoices.Failed
                self.save()
                raise e

            return 'Done!'


@receiver(post_save, sender=EOProduct, weak=False, dispatch_uid='eoproduct_post_save_handler')
def eoproduct_post_save_handler(instance: EOProduct, **kwargs):
    if instance.state == EOProductStateChoices.Ready:
        # have another pass at the generate_products_from_source functions.
        # if it comes back with something,  process it
        from eo_engine.common.products import generate_products_from_source
        for product in generate_products_from_source(instance.filename):
            obj, created = EOProduct.objects.get_or_create(
                filename=product.filename,
                output_folder=product.output_folder,
                task_name=product.task_name,
                task_kwargs=product.task_kwargs,
                group=product.group
            )
            if created:
                # mark as available if this entry was just made now
                obj.state = EOProductStateChoices.Available
                # mark inputs
                obj.eo_products_inputs.set([instance, ])
                obj.save()
    return


__all__ = [
    "EOProduct",
    "EOProductGroupChoices",
    "EOProductStateChoices"
]
