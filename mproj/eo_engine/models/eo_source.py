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
    availableRemotely = "availableRemotely", 'NO_LABEL'
    scheduledForDownload = "scheduledForDownload", "Scheduled For Download"
    availableLocally = "availableLocally", 'NO_LABEL'
    beingDownloaded = 'beingDownloaded', 'NO_LABEL'
    ignore = "Ignore", 'NO_LABEL'


class EOSourceProductChoices(models.TextChoices):
    # add mode products here
    c_gsl_ndvi300_v2_glob = 'c_gsl_NDVI300-V2-GLOB', "Copernicus Global Land Service NDVI 300m v2"
    c_gsl_ndvi1km_v3_glob = 'c_gsl_NDVI1km-V3-GLOB', "Copernicus Global Land Service NDVI 1km v3"
    a_agro_ndvi300_v3_glob = 'a_agro_NDVI300-V3-AFR', "AuthAgro Service NDVI 1km v3"
    c_gsl_lai300_v1_glob = 'c_gsl_LAI300-V1-GLOB', "Copernicus Global Land Service LAI 300m v1"

    # https://land.copernicus.eu/global/sites/cgls.vito.be/files/products/CGLOPS2_PUM_WB100m_V1_I1.10.pdf
    c_gls_WB100_v1_glob = 'c_gls_WB100-V1-GLOB', "Copernicus Global Land Service Water Bodies Collection 100m Version 1"

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
        headers = response.headers
        FILE_LENGTH = headers.get('Content-Length', None)

        self.status = EOSourceStatusChoices.beingDownloaded
        self.save()

        with TemporaryFile(mode='w+b') as file_handle:
            # TemporaryFile has noname, and will cease to exist when it is closed.

            for chunk in response.iter_content(chunk_size=2 * 1024):
                self.refresh_from_db()  # ping to keep db connection alive
                file_handle.write(chunk)
                file_handle.flush()

            content = File(file_handle)
            print(self.filename)
            from django.db import connections
            for conn in connections.all():
                conn.close_if_unusable_or_obsolete()
            self.refresh_from_db()
            self.file.save(name=self.filename, content=content, save=False)

        self.filesize = self.file.size
        self.status = EOSourceStatusChoices.availableLocally

        self.save()


@receiver(post_save, sender=EOSource, weak=False, dispatch_uid='eosource_post_save_handler')
def eosource_post_save_handler(instance: EOSource, **kwargs):
    """ Post save logic goes here. ie an asset is now available locally, are there products that can be made?"""
    from eo_engine.common import generate_products
    eo_source = instance
    # if asset is local
    if eo_source.status == EOSourceStatusChoices.availableLocally:
        # pass
        products = generate_products(eo_source)

        for product in products:
            from eo_engine.models import EOProduct, EOProductsChoices
            prod = EOProduct.objects.create(
                product=product.group,
                output_folder=product.output_folder,
                filename=product.filename,
                task_name=product.task_name,
                task_kwargs=product.task_kwargs
            )
            prod.inputs.set([eo_source, ])
            prod.save()


__all__ = [
    "EOSource",
    "EOSourceProductChoices",
    "EOSourceStatusChoices"

]
