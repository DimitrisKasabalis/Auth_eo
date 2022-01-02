from logging import Logger

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

logger: Logger = get_task_logger(__name__)


def _file_storage_path(instance: 'EOSource', filename: str):
    o = urlsplit(instance.url)
    if o.scheme.lower() == 'wapor':
        from eo_engine.models.factories import wapor_from_filename
        var = wapor_from_filename(filename)
        product_id = var.product_id
        # wapor/<product_id>/<filename>
        full_path = f"{o.scheme}/{product_id}"
    elif o.scheme.lower() == 'sentinel':
        # sentinel/<UUID>.zip
        full_path = 'sentinel'
        if instance.group == EOSourceGroupChoices.S06P01_S1_10M_KZN:
            full_path = 'sentinel/KZN'
        if instance.group == EOSourceGroupChoices.S06P01_S1_10M_BAG:
            full_path = 'sentinel/BAG'

    else:
        # .parent removes the filename (for normalisation)
        full_path = Path(f'{instance.domain}/{o.path[1:] if o.path.startswith(r"/") else o.path}').parent.as_posix()

    # where to save
    final_path = os.path.join(full_path, filename)

    # clean up if exists, not the same as above, as this one is absolute
    absolute_path = os.path.join(settings.MEDIA_ROOT, final_path)
    if os.path.exists(absolute_path):
        logger.warning('Found existing file from previous iteration. Removing')
        os.remove(absolute_path)

    return final_path


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

    # Reason of M2M is that ndvi-anomaly tiles are used for multiple groups.
    group = models.ManyToManyField('EOSourceGroup')
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
    # product reference datetime, cannot be null
    reference_date = models.DateField(help_text="product reference date")
    # when did we see it?, change to date
    datetime_seen = models.DateTimeField(auto_created=True, help_text="datetime of when it was seen")
    # full url to resource.
    #  eg ftp://ftp.globalland.cls.fr/path/filename.nc
    url = models.URLField(help_text="Resource URL")
    # username/password of resource
    credentials = models.ForeignKey("Credentials", on_delete=models.SET_NULL, null=True)

    class Meta:
        ordering = ["filename", "-reference_date", "group__name"]

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
    from eo_engine.models import EOProduct, EOProductStateChoices, Pipeline

    eo_source = instance
    # if asset is not local, ignore
    if eo_source.state != EOSourceStateChoices.AVAILABLE_LOCALLY:
        return

    # it's used for the output filenames
    yyyymmdd = eo_source.reference_date.strftime('%Y%m%d')

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

            output_filename = pipeline.output_filename(YYYYMMDD=yyyymmdd)

            prod, created = EOProduct.objects.get_or_create(
                filename=output_filename,
                group=output_group,
                reference_date=eo_source.reference_date
            )
            # generalised solution
            # are all the files that are supposed to be available ?
            all_of_this_kind = EOSource.objects.filter(group=input_group_eosource,
                                                       reference_date=eo_source.reference_date)
            all_of_this_kind_that_are_available = all_of_this_kind.filter(state=EOSourceStateChoices.AVAILABLE_LOCALLY)
            if all_of_this_kind.count() == all_of_this_kind_that_are_available.count():
                prod.state = EOProductStateChoices.AVAILABLE
            else:
                prod.state = EOProductStateChoices.MISSING_SOURCE

            prod.save()


__all__ = [
    "EOSource",
    "EOSourceStateChoices"
]
