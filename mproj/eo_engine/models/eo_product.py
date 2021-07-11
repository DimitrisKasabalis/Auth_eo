import os

from django.conf import settings
from django.db import models


class EOProductGroupChoices(models.TextChoices):
    # NDVI Products
    a_agro_ndvi_1km_v3 = "agro_ndvi-1km-v3-afr"
    a_agro_ndvi_300m_v2 = "agro_ndvi-300m-v3-afr"

    # LAI
    a_agro_lai_300m_v1_afr = "agro_lai-300m-v1-afr",
    a_agro_lai_1km_v2_afr = "agro_lai_1km-v2-afr",

    # WB Products
    a_agro_wb_100m_tun = 'agro_wb-100m-tun'
    a_agro_wb_100m_rwa = 'agro_wb-100m-rwa'
    a_agro_wb_100m_eth = 'agro_wb-100m-eth'
    a_agro_wb_100m_moz = 'agro_wb-100m-moz'
    a_agro_wb_100m_zaf = 'agro_wb-100m-zaf'
    a_geop_wb_100m_gha = 'agro_wb-100m-gha'
    a_geop_wb_100m_ner = 'agro_wb-100m-ner'


class EOProductStatusChoices(models.TextChoices):
    Available = 'Available', "Available for generation."
    Scheduled = "Scheduled", "Scheduled For generation."
    Failed = 'Failed', 'Generation was attempted but failed'
    Generating = 'Generating', "Product is being generated."
    Ignore = 'Ignore', "Ignore."
    Ready = 'Ready', "Product is Ready."


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
    inputs = models.ManyToManyField("eo_engine.EOSource")

    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=255, choices=EOProductStatusChoices.choices,
                              default=EOProductStatusChoices.Available)
    task_name = models.CharField(max_length=255)
    task_kwargs = models.JSONField()

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


__all__ = [
    "EOProduct",
    "EOProductGroupChoices",
    "EOProductStatusChoices"
]
