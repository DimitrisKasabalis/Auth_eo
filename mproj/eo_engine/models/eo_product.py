from django.core.serializers.json import DjangoJSONEncoder
from django.db import models


class EOProductsChoices(models.TextChoices):
    a_agro_ndvi_1km_v3 = "agro-ndvi-1km-v3"
    a_agro_wb_100m_tun = 'WB-100m-TUN'
    a_agro_wb_100m_rwa = 'WB-100m-RWA'
    a_agro_wb_100m_eth = 'WB-100m-ETH'
    a_agro_wb_100m_moz = 'WB-100m-MOZ'
    a_agro_wb_100m_zaf = 'WB-100m-ZAF'
    a_geop_wb_100m_gha = 'WB-100m-GHA'
    a_geop_wb_100m_ner = 'WB-100m-NER'


class EOProductStatusChoices(models.TextChoices):
    Available = 'Available', "Available for generation."
    Scheduled = "Scheduled", "Scheduled For generation."
    Failed = 'Failed', 'Generation was attempted but failed'
    Generating = 'Generating', "Product is being generated."
    Ready = 'Ready', "Product is Ready."


def _upload_to(instance: 'EOProduct', filename):
    root_folder = 'products'

    return f"{root_folder}/{instance.output_folder}/{filename}"


class EOProduct(models.Model):
    filename = models.TextField(unique=True)
    output_folder = models.TextField()
    product = models.CharField(max_length=255, choices=EOProductsChoices.choices)
    datetime_creation = models.DateTimeField(null=True)
    file = models.FileField(upload_to=_upload_to, null=True, max_length=2048)
    inputs = models.ManyToManyField("eo_engine.EOSource")

    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=255, choices=EOProductStatusChoices.choices,
                              default=EOProductStatusChoices.Available)

    task_name = models.CharField(max_length=255)
    task_kwargs = models.JSONField()

    # link to task

    def make_product(self, as_task: bool = False):
        from eo_engine import tasks
        func = getattr(tasks, self.task_name)  # get ref to task function using its name

        if func is None:
            raise NotImplementedError('No generation method has been defined for this product.')

        if as_task:
            task = func.s(output_pk=self.pk, **self.task_kwargs)
            task_id = task.apply_async()
            # link to task
            return task_id
        else:
            func(output_pk=self.pk, **self.task_kwargs)
            return 'Done!'


__all__ = [
    "EOProduct",
    "EOProductsChoices",
    "EOProductStatusChoices"
]
