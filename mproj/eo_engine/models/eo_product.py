from django.core.serializers.json import DjangoJSONEncoder
from django.db import models


class EOProductsChoices(models.TextChoices):
    agro_ndvi_1km_v3 = "agro-ndvi-1km-v3", "ndvi 1km v3 (lab-made)"


class EOProductStatusChoices(models.TextChoices):
    AVAILABLE = 'AVAILABLE', "Available for production"
    PROCESSING = 'PROCESSING', "Processing..."
    READY = 'READY', "Ready"


def _upload_to(instance: 'EOProduct', filename):
    root_folder = 'agro_products'
    group = str(instance.product)

    return f"{root_folder}/{group}"


class EOProduct(models.Model):
    filename = models.TextField(unique=True)
    status = models.CharField(max_length=255, choices=EOProductStatusChoices.choices,
                              default=EOProductStatusChoices.AVAILABLE)
    product = models.CharField(max_length=255, choices=EOProductsChoices.choices)
    datetime_creation = models.DateTimeField(null=True)
    file = models.FileField(upload_to=_upload_to, null=True, max_length=2048)
    timestamp = models.DateTimeField(auto_now_add=True)
    process = models.JSONField(default=dict, encoder=DjangoJSONEncoder)
    inputs = models.ManyToManyField("eo_engine.EOSource")

    def make_product(self, as_task: bool = False, force=False):
        if self.status in [EOProductStatusChoices.READY, EOProductStatusChoices.PROCESSING]:
            # its done or being made
            # log ...
            return

        func = None
        kwargs = {}
        args = []
        if self.product == EOProductsChoices.agro_ndvi_1km_v3:
            from eo_engine.tasks import task_make_agro_ndvi_1km_v3
            func = task_make_agro_ndvi_1km_v3
            kwargs.update({
                "product_pk": self.id
            })

        if func is None:
            raise NotImplementedError('No generation method has been defined for this product.')

        if as_task:
            task = func.s(*args, **kwargs)
            job = task.apply_async()
        else:
            func(*args, **kwargs)


__all__ = [
    "EOProduct",
    "EOProductsChoices",
    "EOProductStatusChoices"
]
