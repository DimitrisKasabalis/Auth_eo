import os
from typing import Any, TypedDict

from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver


class EOProductGroupChoices(models.TextChoices):
    # NDVI Products
    a_agro_ndvi_1km_v3 = "agro_ndvi-1km-v3-afr", 'AUTH/AGRO/NDVI 1km, V3 Africa'
    a_agro_ndvi_300m_v3 = "agro_ndvi-300m-v3-afr", 'AUTH/AGRO/NDVI 300m, V3 Africa'

    # VCI
    a_agro_vci_1km_v2_afr = 'agro_vci-10km-v2-afr', 'AUTH/AGRO/VCI 1km, V2 Africa'

    # LAI
    a_agro_lai_300m_v1_afr = "agro_lai-300m-v1-afr", 'AUTH/AGRO/LAI 300m, V1 Africa'
    a_agro_lai_1km_v2_afr = "agro_lai_1km-v2-afr", 'AUTH/AGRO/LAI 1km, V2 Africa'

    # WB Products
    a_agro_wb_100m_tun = 'agro_wb-100m-tun'
    a_agro_wb_100m_rwa = 'agro_wb-100m-rwa'
    a_agro_wb_100m_eth = 'agro_wb-100m-eth'
    a_agro_wb_100m_moz = 'agro_wb-100m-moz'
    a_agro_wb_100m_zaf = 'agro_wb-100m-zaf'
    a_geop_wb_100m_gha = 'agro_wb-100m-gha'
    a_geop_wb_100m_ner = 'agro_wb-100m-ner'


class EOProductStatusChoices(models.TextChoices):
    Available = 'Available', "AVAILABLE for generation."
    Scheduled = "Scheduled", "SCHEDULED For generation."
    Failed = 'Failed', 'Generation was attempted but FAILED'
    Generating = 'Generating', "GENERATING..."
    Ignore = 'Ignore', "Skip generation (Ignored) ."
    Ready = 'Ready', "Product is READY."


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
    product_group = models.CharField(max_length=255, choices=EOProductGroupChoices.choices)
    datetime_creation = models.DateTimeField(null=True)
    file = models.FileField(upload_to=_upload_to, null=True, max_length=2048)

    eo_sources_inputs = models.ManyToManyField("eo_engine.EOSource")
    eo_products_inputs = models.ManyToManyField('self', symmetrical=False)

    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=255, choices=EOProductStatusChoices.choices,
                              default=EOProductStatusChoices.Available)
    task_name = models.CharField(max_length=255)
    task_kwargs = models.JSONField(default=dict)

    class Meta:
        ordering = ["product_group", "filename"]

    def __str__(self):
        return f"{self.__class__.__name__}/{self.filename}/{self.status}/{self.id}"

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
        return self.product_group in ignore_product_rules

    def make_product(self, as_task: bool = False):
        from eo_engine import tasks
        func = getattr(tasks, self.task_name)  # get ref to task function using its name
        self.status = EOProductStatusChoices.Generating
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
                self.status = EOProductStatusChoices.Failed
                self.save()
                raise e

            return 'Done!'


@receiver(post_save, sender=EOProduct, weak=False, dispatch_uid='eoproduct_post_save_handler')
def eoproduct_post_save_handler(instance: EOProduct, **kwargs):
    if instance.status == EOProductStatusChoices.Ready:
        if (
                instance.product_group == EOProductGroupChoices.a_agro_ndvi_300m_v3 or
                instance.product_group == EOProductGroupChoices.a_agro_ndvi_1km_v3
        ):
            from eo_engine.common import generate_products_from_source
            for product in generate_products_from_source(instance.filename):
                obj, created = EOProduct.objects.get_or_create(
                    filename=product.filename,
                    output_folder=product.output_folder,
                    task_name=product.task_name,
                    task_kwargs=product.task_kwargs,
                    product_group=product.group

                )
                if created:
                    # mark as available if this entry was just made now
                    obj.status = EOProductStatusChoices.Available
                    # mark inputs
                    obj.eo_products_inputs.set([instance, ])
                    obj.save()


__all__ = [
    "EOProduct",
    "EOProductGroupChoices",
    "EOProductStatusChoices"
]
