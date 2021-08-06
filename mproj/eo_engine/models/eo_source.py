from pathlib import Path
from urllib.parse import urlsplit

from django.core.files import File
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver


def _file_storage_path(instance: 'EOSource', filename: str):
    o = urlsplit(instance.url)
    local_path = o.path[1:] if o.path.startswith(r'/') else o.path
    return f"{instance.domain}/{local_path}"


class EOSourceStatusChoices(models.TextChoices):
    availableRemotely = "availableRemotely", 'Available on the Remote Server'
    scheduledForDownload = "scheduledForDownload", "Scheduled For Download"
    availableLocally = "availableLocally", 'File available locally'
    beingDownloaded = 'beingDownloaded', 'File is Being downloaded'
    ignore = "Ignore", 'IgnoreFile'


class EOSourceProductChoices(models.TextChoices):
    # add mode products here
    c_gsl_ndvi300_v2_glob = 'c_gsl_ndvi300-v2-glob', "Copernicus Global Land Service NDVI 300m v2"
    c_gsl_ndvi1km_v3_glob = 'c_gsl_ndvi1km-v3-glob', "Copernicus Global Land Service NDVI 1km v3"
    a_agro_ndvi300_v3_glob = 'a_agro_ndvi300-v3-afr', "AuthAgro Service NDVI 1km v3"
    c_gsl_lai300_v1_glob = 'c_gsl_lai300-v1-glob', "Copernicus Global Land Service LAI 300m v1"

    # https://land.copernicus.eu/global/sites/cgls.vito.be/files/products/CGLOPS2_PUM_WB100m_V1_I1.10.pdf
    c_gls_WB100_v1_glob = 'c_gls_wb100-v1-glob', "Copernicus Global Land Service Water Bodies Collection 100m Version 1"

    # ndvi_300m_v1 = "ndvi-300m-v1", "ndvi 300m v1"
    # ndvi_300m_v2 = "ndvi-300m-v2", "ndvi 300m v2"
    # ndvi_1km_v3 = "ndvi-1km-v3", "ndvi 1km v3"
    # ndvi_1km_v2 = "ndvi-1km-v2", "ndvi 1km v2"
    # ndvi_1km_v1 = "ndvi-1km-v1", "ndvi 1km v1"
    # lai_300m_v1 = "lai-300m-v1", "lai 300m v1"
    # lai_1km_v1 = "lai-1km-v1", "lai 1km v1"
    # lai_1km_v2 = "lai-1km-v2", "lai 1km v2"
    # vc1_v1 = "vc1-v1", "vc1 v1"
    # wb_africa_v1 = "wb-africa-v1", "wb africa V1"
    # sirs_nrt_300 = "sirs-nrt-300", "SIRS WB NRT 300m"
    # sirs_nrt_100 = "sirs-nrt-100", "SIRS WB NRT 100m"


class EOSource(models.Model):
    # EO-Inputs
    """ A ledger for known files """

    # status of file.
    status = models.CharField(max_length=255,
                              choices=EOSourceStatusChoices.choices,
                              default=EOSourceStatusChoices.availableRemotely)

    # what product is it?
    product = models.CharField(max_length=255, choices=EOSourceProductChoices.choices)
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
    datetime_reference = models.DateTimeField(null=True, help_text="product reference datetime ")
    # when did we see it?
    datetime_seen = models.DateTimeField(auto_created=True, help_text="datetime of when it was seen")
    # full url to resource.
    url = models.URLField(help_text="Resource URL")
    # username/password of resource
    credentials = models.ForeignKey("Credentials", on_delete=models.SET_NULL, null=True)

    class Meta:
        ordering = ["product", "-datetime_reference"]

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

    def set_status(self, status: str):
        self.status = status
        self.save()

    def download(self):
        from urllib.parse import urlparse
        url_parse = urlparse(self.url)
        scheme = url_parse.scheme
        if scheme.startswith('ftp'):
            from eo_engine.common import download_ftp_eosource
            return download_ftp_eosource(self)
        if scheme.startswith('http'):
            from eo_engine.common import download_http_eosource
            return download_http_eosource(self)

        raise Exception(f'There was no defined method for scheme: {scheme}')


@receiver(post_save, sender=EOSource, weak=False, dispatch_uid='eosource_post_save_handler')
def eosource_post_save_handler(instance: EOSource, **kwargs):
    """ Post save logic goes here. ie an asset is now available locally, are there products that can be made?"""
    from eo_engine.common import generate_products_from_source
    from eo_engine.models import EOProduct, EOProductStatusChoices
    eo_source = instance
    # if asset is local
    if eo_source.status == EOSourceStatusChoices.availableLocally:
        # pass
        products = generate_products_from_source(eo_source)

        for product in products:

            prod, created = EOProduct.objects.get_or_create(
                filename=product.filename,
                output_folder=product.output_folder,
                product_group=product.group,
                task_name=product.task_name,
                task_kwargs=product.task_kwargs
            )
            # mark if the scheduler should ignore it
            if prod.is_ignored():
                prod.status = EOProductStatusChoices.Ignore

            # mark it's inputs
            prod.eo_sources_inputs.set([eo_source, ])
            prod.save()


__all__ = [
    "EOSource",
    "EOSourceProductChoices",
    "EOSourceStatusChoices"
]
