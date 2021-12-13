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
    MISSING_SOURCE = 'MISSING_SOURCE', "Some or all EOSouce(s) Are Not Available"


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
    group = models.ForeignKey('EOProductGroup', on_delete=models.DO_NOTHING)
    datetime_creation = models.DateTimeField(null=True)
    reference_date = models.DateField()
    file = models.FileField(upload_to=_upload_to, null=True, max_length=2048)

    timestamp = models.DateTimeField(auto_now_add=True)
    state = models.CharField(max_length=255, choices=EOProductStateChoices.choices,
                             default=EOProductStateChoices.AVAILABLE)

    class Meta:
        ordering = ["group", "filename"]
        unique_together = ['reference_date', 'group']

    def __str__(self):
        return f"{self.__class__.__name__}/{self.filename}/{self.state}/{self.id}"


__all__ = [
    "EOProduct",
    "EOProductStateChoices"
]
