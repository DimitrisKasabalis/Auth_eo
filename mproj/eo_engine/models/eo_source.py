import os
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlsplit

from eo_engine.models.eo_group import EOSourceGroupChoices

logger = get_task_logger(__name__)


def _file_storage_path(instance: 'EOSource', filename: str):
    o = urlsplit(instance.url)
    if o.scheme.lower() == 'wapor':
        from eo_engine.models.factories import wapor_from_filename
        var = wapor_from_filename(filename)
        product_id = var.product_id
        # wapor/<product_id>/<filename>
        full_path = f"{o.scheme}/{product_id}/{filename}"
    else:
        full_path = f'{instance.domain}/{o.path[1:] if o.path.startswith(r"/") else o.path}'

        final_path = os.path.join(settings.MEDIA_ROOT, full_path)
        if os.path.exists(final_path):
            os.remove(final_path)

        return full_path


class EOSourceStateChoices(models.TextChoices):
    AVAILABLE_REMOTELY = "AVAILABLE_REMOTELY", 'Available on the Remote Server'
    SCHEDULED_FOR_DOWNLOAD = "SCHEDULED_FOR_DOWNLOAD", "Scheduled For Download"
    AVAILABLE_LOCALLY = "AVAILABLE_LOCALLY", 'File is Available Locally'
    DOWNLOADING = 'DOWNLOADING', 'Downloading File...'
    DOWNLOAD_FAILED = 'DOWNLOAD_FAILED', 'Download Failed'
    IGNORE = "IGNORE", 'Action on this file has been canceled (Ignored/Revoked Action)'
    DEFERRED = 'DEFERRED', 'Download has been deferred for later'  # it's a file on request, and it's being made


class EOSource(models.Model):
    # EO-Inputs
    """ A ledger for known files """

    # status of file.
    state = models.CharField(max_length=255,
                             choices=EOSourceStateChoices.choices,
                             default=EOSourceStateChoices.AVAILABLE_REMOTELY)

    group = models.ForeignKey('EOSourceGroup', on_delete=models.DO_NOTHING)
    # physical file. Read about DJANGO media files
    file = models.FileField(upload_to=_file_storage_path,
                            editable=False,
                            null=True,
                            max_length=2_048)
    # filename, including extension. Must be unique
    filename = models.CharField(max_length=255, unique=True)
    # net location of resource
    domain = models.CharField(max_length=200)
    # reported filesize, bytes?
    filesize_reported = models.BigIntegerField(validators=(MinValueValidator(0),))
    # product reference datetime
    reference_date = models.DateField(null=True, help_text="product reference date")
    # when did we see it?, change to date
    datetime_seen = models.DateTimeField(auto_created=True, help_text="datetime of when it was seen")
    # full url to resource.
    #  eg ftp://ftp.globalland.cls.fr/path/filename.nc
    url = models.URLField(help_text="Resource URL")
    # username/password of resource
    credentials = models.ForeignKey("Credentials", on_delete=models.SET_NULL, null=True)

    class Meta:
        ordering = ["group", "-reference_date"]

    def __str__(self):
        return f"{self.__class__.__name__}/{self.filename}/{self.state}/{self.id}"

    @property
    def local_path(self) -> Path:
        return Path(self.file.path)

    def delete_local_file(self) -> bool:
        """ Delete local file """
        raise NotImplemented("yet?")

    @property
    def exist(self) -> bool:
        """ True if the a file exists in the local storage area """
        return self.local_path.is_file()

    @property
    def get_credentials(self) -> Optional[Tuple[str, str]]:
        """ Returns credentials to download this file."""
        try:
            return self.credentials.username, self.credentials.password
        except AttributeError:
            return None

    def set_status(self, status: str):
        self.state = status
        self.save()


@receiver(post_save, sender=EOSource, weak=False, dispatch_uid='eosource_post_save_handler')
def eosource_post_save_handler(instance: EOSource, **kwargs):
    """ Post save logic goes here. ie an asset is now available locally, are there products that can be made?"""
    from eo_engine.common.s02p04 import is_gmod09q1_batch_complete
    from eo_engine.models import Pipeline
    from eo_engine.models import EOProduct, EOProductStateChoices, EOProductGroup

    eo_source = instance
    # if asset is not local, ignore
    if eo_source.state != EOSourceStateChoices.AVAILABLE_LOCALLY:
        return
        # pass

    # it's used for the output filenames
    yyyymmdd = eo_source.reference_date.strftime('%Y%m%d')

    # pipelines that are using this source as input:
    pipelines = eo_source.group.pipelines_from_input.all()

    for pipeline in pipelines:
        # a pipeline should have one output group
        output_group = EOProductGroup.objects.get(pipelines_from_output=pipeline)

        output_filename = pipeline.output_filename(YYYYMMDD=yyyymmdd)

        prod, created = EOProduct.objects.get_or_create(
            filename=output_filename,
            group=output_group,
            reference_date=eo_source.reference_date
        )
        if created:
            prod.state = EOProductStateChoices.AVAILABLE

        # these bellow, are a batch, multiple inputs create one
        if [EOSourceGroupChoices.S02P02_NDVIA_250M_ETH_GMOD, EOSourceGroupChoices.S02P02_NDVIA_250M_GHA_GMOD,
            EOSourceGroupChoices.S02P02_NDVIA_250M_KEN_GMOD, EOSourceGroupChoices.S02P02_NDVIA_250M_MOZ_GMOD,
            EOSourceGroupChoices.S02P02_NDVIA_250M_NER_GMOD, EOSourceGroupChoices.S02P02_NDVIA_250M_RWA_GMOD,
            EOSourceGroupChoices.S02P02_NDVIA_250M_TUN_GMOD, EOSourceGroupChoices.S02P02_NDVIA_250M_ZAF_GMOD
            ].count(eo_source.group.name):
            if is_gmod09q1_batch_complete(eo_source):
                prod.state = EOProductStateChoices.AVAILABLE
            else:
                prod.state = EOProductStateChoices.MISSING_SOURCE

        # # mark it's inputs
        # prod.eo_sources_inputs.add(eo_source)
        prod.save()


__all__ = [
    "EOSource",
    "EOSourceStateChoices"
]
