from pathlib import Path
from urllib.parse import urlsplit

from django.core.files import File
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver


def _file_storage_path(instance: 'EOSource', filename: str):
    o = urlsplit(instance.url)
    local_path = o.path[1:] if o.path.startswith(r'/') else o.path
    return f"{instance.domain}/{local_path}"


class EOSourceStatusChoices(models.TextChoices):
    availableRemotely = "availableRemotely"
    availableLocally = "availableLocally"
    beingDownloaded = 'beingDownloaded'
    ignore = "ignore"


class EOSourceProductGroupChoices(models.TextChoices):
    NDVI = "ndvi", "NDVI"
    LAI = "lai", "LAI"
    FCOVER = "fcover", "FCOVER"
    FAPAR = "fapar", "FAPAR"
    DMP = "dmp", "DMP"
    GDMP = "gdmp", "GDMP"
    OTHER = 'other', "Other"


class EOSourceProductChoices(models.TextChoices):
    # add mode products here
    ndvi_300m_v1 = "ndvi-300m-v1", "ndvi 300m v1"
    ndvi_300m_v2 = "ndvi-300m-v2", "ndvi 300m v2"
    ndvi_1km_v3 = "ndvi-1km-v3", "ndvi 1km v3"
    ndvi_1km_v2 = "ndvi-1km-v2", "ndvi 1km v2"
    ndvi_1km_v1 = "ndvi-1km-v1", "ndvi 1km v1"
    lai_300m_v1 = "lai-300m-v1", "lai 300m v1"
    lai_1km_v1 = "lai-1km-v1", "lai 1km v1"
    lai_1km_v2 = "lai-1km-v2", "lai 1km v2"
    vc1_v1 = "vc1-v1", "vc1 v1"
    wb_africa_v1 = "wb-africa-v1", "wb africa V1"


class EOSource(models.Model):
    # EO-Inputs
    """ A ledger for known files """

    status = models.CharField(max_length=255,
                              choices=EOSourceStatusChoices.choices,
                              default=EOSourceStatusChoices.availableRemotely)
    product_group = models.CharField(max_length=10, editable=True, choices=EOSourceProductGroupChoices.choices)
    product = models.CharField(max_length=255, choices=EOSourceProductChoices.choices)
    file = models.FileField(upload_to=_file_storage_path, editable=False, null=True, max_length=2_048)
    filename = models.CharField(max_length=255, unique=True)
    domain = models.CharField(max_length=200)
    datetime_uploaded = models.DateTimeField()
    datetime_seen = models.DateTimeField(auto_created=True)
    url = models.URLField()
    credentials = models.ForeignKey("Credentials", on_delete=models.SET_NULL, null=True)

    def __str__(self):
        return f"{self.__class__.__name__}/{self.filename}/{self.status}"

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
    def get_credentials(self) -> (str, str):
        """ Returns credentials to download this file."""
        return self.credentials.username, self.credentials.password

    def download(self):
        import requests
        from tempfile import TemporaryFile

        remote_url = self.url
        credentials = self.get_credentials

        response = requests.get(
            url=remote_url,
            auth=credentials,
            stream=True
        )
        response.raise_for_status()

        self.status = EOSourceStatusChoices.beingDownloaded
        self.save()

        with TemporaryFile(mode='w+b') as file_handle:
            for chunk in response.iter_content(chunk_size=2 * 1024):
                file_handle.write(chunk)
                file_handle.flush()

            content = File(file_handle)
            print(self.filename)
            self.file.save(name=self.filename, content=content, save=False)
            self.filesize = self.file.size
            self.status = EOSourceStatusChoices.availableLocally

        self.save()



@receiver(post_save, sender=EOSource, weak=False, dispatch_uid='eosource_post_save_handler')
def eosource_post_save_handler(instance: EOSource, **kwargs):
    " Post save logic goes here. ie an asset is now available locally, are there products that can be made?"

    eo_source = instance
    # if asset is local
    if eo_source.status == EOSourceStatusChoices.availableLocally:
        from eo_engine.models import EOProduct, EOProductsChoices
        from eo_engine.common import generate_prod_filename
        # if asset is ndvi-300m-v2
        if eo_source.product == EOSourceProductChoices.ndvi_300m_v2 and eo_source.product == EOSourceProductChoices.ndvi_300m_v1:
            prod = EOProduct.objects.create(
                product=EOProductsChoices.agro_ndvi_1km_v3,
                filename=generate_prod_filename(eo_source),
            )
            prod.inputs.set([eo_source, ])
            prod.save()


__all__ = [
    "EOSource",
    "EOSourceProductChoices",
    "EOSourceProductGroupChoices",
    "EOSourceStatusChoices"

]
