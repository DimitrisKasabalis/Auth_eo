import os

from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver




class EOProductStateChoices(models.TextChoices):
    AVAILABLE = 'AVAILABLE', "AVAILABLE for generation."
    SCHEDULED = 'SCHEDULED', "SCHEDULED For generation."
    FAILED = 'FAILED', 'Generation was attempted but FAILED'
    GENERATING = 'GENERATING', "GENERATING..."
    IGNORE = 'IGNORE', "Skip generation (Ignored) ."
    READY = 'READY', "Product is READY."
    MISSING_SOURCE = 'MISSING_SOURCE', "Some or all EOSource(s) Are Not Available"


def _upload_to(instance: 'EOProduct', filename):
    root_folder = 'products'
    output_folder = instance.group.pipelines_from_output.first().output_folder
    full_path = f"{root_folder}/{output_folder}/{filename}"

    # remove if target file exists
    # (alternative solution would be to define another storage class)
    final_path = os.path.join(settings.MEDIA_ROOT, full_path)
    if os.path.exists(final_path):
        os.remove(final_path)

    return full_path


class EOProduct(models.Model):
    filename = models.TextField(unique=True)
    group = models.ForeignKey('EOGroup', on_delete=models.DO_NOTHING)
    datetime_creation = models.DateTimeField(null=True)
    reference_date = models.DateField()
    file = models.FileField(upload_to=_upload_to, null=True, max_length=2048)
    timestamp = models.DateTimeField(auto_now_add=True)
    state = models.CharField(max_length=255,
                             choices=EOProductStateChoices.choices,
                             default=EOProductStateChoices.AVAILABLE)

    @property
    def reference_date_iso_str(self) -> str:
        return self.reference_date.isoformat()

    class Meta:
        ordering = ["group", "filename"]
        unique_together = ['reference_date', 'group']

    def __str__(self):
        return f"{self.__class__.__name__}/{self.filename}/{self.state}/{self.id}"


def should_create(pipeline: 'Pipeline', eo_product: EOProduct) -> bool:
    from eo_engine.models import Pipeline
    if pipeline == Pipeline.objects.get(task_name='task_s04p03_floods10m'):
        reference_date = eo_product.reference_date
        mid_august = reference_date.replace(day=14, month=8)
        mid_october = reference_date.replace(day=16, month=10)
        if mid_august <= eo_product.reference_date <= mid_october:
            return True
        return False
    return True


@receiver(post_save, sender=EOProduct, weak=False, dispatch_uid='eoproduct_post_save_handler')
def eoproduct_post_save_handler(instance: EOProduct, **kwargs):
    from eo_engine.models import Pipeline
    eo_product = instance
    if eo_product.state != EOProductStateChoices.READY:
        return

    yyyymmdd = eo_product.reference_date.strftime('%Y%m%d')
    yyyy = eo_product.reference_date.strftime('%Y')
    pipelines = Pipeline.objects.filter(input_groups__eoproduct=eo_product)
    for pipeline in pipelines:

        # apply special rules if this should made or not
        if not should_create(pipeline, eo_product):
            continue

        # output templates could be YYYYMMDD or YYYY
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
    "EOProduct",
    "EOProductStateChoices"
]
