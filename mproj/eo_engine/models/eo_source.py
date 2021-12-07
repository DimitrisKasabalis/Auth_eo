from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlsplit

from django.core.validators import MinValueValidator
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver


def _file_storage_path(instance: 'EOSource', filename: str):
    o = urlsplit(instance.url)
    if o.scheme.lower() == 'wapor':
        from eo_engine.models.factories import wapor_from_filename
        var = wapor_from_filename(filename)
        product_id = var.product_id
        # wapor/<product_id>/<filename>
        return f"{o.scheme}/{product_id}/{filename}"

    local_path = o.path[1:] if o.path.startswith(r'/') else o.path
    return f"{instance.domain}/{local_path}"


class EOSourceStateChoices(models.TextChoices):
    AvailableRemotely = "availableRemotely", 'Available on the Remote Server'
    ScheduledForDownload = "ScheduledForDownload", "Scheduled For Download"
    AvailableLocally = "availableLocally", 'File is Available Locally'
    BeingDownloaded = 'BeingDownloaded', 'Downloading File...'
    FailedToDownload = 'FailedToDownload', 'Failed to Download'
    Ignore = "Ignore", 'Action on this file has been canceled (Ignored/Revoked Action)'
    Defered = 'Defer', 'Download has been deferred for later'


# USE SCREAMING_CASE
class EOSourceGroupChoices(models.TextChoices):
    # add mode products here

    # AGRO = AUth
    # C_GLS = Copernicus Global Land Service
    # WB = Water bodies
    #  ines... help me out!!

    C_GLS_LAI_300M_V1_GLOB = 'C_GLS_LAI_300M_V1_GLOB', "Copernicus Global Land Service LAI 300m v1"
    AGRO_LAI_300M_V1_AFR = 'AGRO_LAI_300M_V1_AFR', "AuthAgro LAI 300M V1"

    C_GLS_NDVI_1KM_V3_GLOB = 'C_GlS_NDVI_1KM_V3_GLOB', "Copernicus Global Land Service NDVI 1km v3"
    C_GLS_NDVI_300M_V2_GLOB = 'C_GLS_NDVI_300M_V2_GLOB', "Copernicus Global Land Service NDVI 300m v2"

    # AGRO NDVI
    A_AGRO_NDVI_300M_V3_AFR = 'A_AGRO_NDVI_300M_V3_AFR', "AuthAgro Service NDVI 300M v3"
    A_AGRO_NDVI_1KM_V3_AFR = 'A_AGRO_NDVI_1KM_V3_AFR', "AuthAgro Service NDVU 1KM V3 Africa"

    # https://land.copernicus.eu/global/sites/cgls.vito.be/files/products/CGLOPS2_PUM_WB100m_V1_I1.10.pdf
    C_GLS_WB_100M_V1_GLOB = 'C_GLS_WB_100M_V1_GLOB', "Copernicus Global Land Service Water Bodies Collection 100m Version 1"
    C_GLS_WB_300M_V2_GLOB = 'C_GLS_WB_300M_V2_GLOB', "Copernicus Global Land Service Water Bodies Collection 300m Version 2"

    # LSASAF
    MSG_3KM_GLOB = 'MSG_3KM_GLOB', 'LSA-SAF Global ET product 3Km'

    # WAPOR
    ## AFRICA
    WAPOR_L1_AETI_D_AFRICA = 'WAPOR_L1_AETI_D_AFRICA', 'WAPOR: L1 AETI D AFRICA'
    WAPOR_L1_QUAL_LST_D_AFRICA = 'WAPOR_L1_QUAL_LST_D_AFRICA', 'WAPOR: L1_QUAL_LST_D_AFRICA'
    WAPOR_L1_QUAL_NDVI_D_AFRICA = 'WAPOR_L1_QUAL_NDVI_D_AFRICA', 'WAPOR: L1_QUAL_NDVI_D_AFRICA'

    ## TUN
    WAPOR_L2_AETI_D_TUN = 'WAPOR_L2_AETI_D_TUN', 'WAPOR: L2_AETI_D_TUN'
    WAPOR_L2_QUAL_LST_D_TUN = 'WAPOR_L2_QUAL_LST_D_TUN', 'WAPOR: L2_QUAL_LST_D_TUN'
    WAPOR_L2_QUAL_NDVI_D_TUN = 'WAPOR_L2_QUAL_NDVI_D_TUN', 'WAPOR: L2_QUAL_NDVI_D_TUN'

    ## KEN
    WAPOR_L2_AETI_D_KEN = 'WAPOR_L2_AETI_D_KEN', 'WAPOR: L2_AETI_D_KEN'
    WAPOR_L2_QUAL_LST_D_KEN = 'WAPOR_L2_QUAL_LST_D_KEN', 'WAPOR: L2_QUAL_LST_D_KEN'
    WAPOR_L2_QUAL_NDVI_D_KEN = 'WAPOR_L2_QUAL_NDVI_D_KEN', 'WAPOR: L2_QUAL_NDVI_D_KEN'

    ## MOZ
    WAPOR_L2_AETI_D_MOZ = 'WAPOR_L2_AETI_D_MOZ', 'WAPOR: L2_AETI_D_MOZ'
    WAPOR_L2_QUAL_LST_D_MOZ = 'WAPOR_L2_QUAL_LST_D_MOZ', 'WAPOR: L2_QUAL_LST_D_MOZ'
    WAPOR_L2_QUAL_NDVI_D_MOZ = 'WAPOR_L2_QUAL_NDVI_D_MOZ', 'WAPOR: L2_QUAL_NDVI_D_MOZ'

    ## RWA
    WAPOR_L2_AETI_D_RWA = 'WAPOR_L2_AETI_D_RWA', 'WAPOR: L2_AETI_D_DRWA'
    WAPOR_L2_QUAL_LST_D_RWA = 'WAPOR_L2_QUAL_LST_D_RWA', 'WAPOR: L2_QUAL_LST_D_RWA'
    WAPOR_L2_QUAL_NDVI_D_RWA = 'WAPOR_L2_QUAL_NDVI_D_RWA', 'WAPOR: L2_QUAL_NDVI_D_RWA'

    ## ETH
    WAPOR_L2_AETI_D_ETH = 'WAPOR_L2_AETI_D_ETH', 'WAPOR: WAPOR_L2_AETI_D_ETH'
    WAPOR_L2_QUAL_LST_D_ETH = 'WAPOR_L2_QUAL_LST_D_ETH', 'WAPOR: L2_QUAL_LST_D_ETH'
    WAPOR_L2_QUAL_NDVI_D_ETH = 'WAPOR_L2_QUAL_NDVI_D_ETH', 'WAPOR: L2_QUAL_NDVI_D_ETH'

    ## GHA
    WAPOR_L2_AETI_D_GHA = 'WAPOR_L2_AETI_D_GHA', 'WAPOR: L2_AETI_GHA'
    WAPOR_L2_QUAL_LST_D_GHA = 'WAPOR_L2_QUAL_LST_D_GHA', 'WAPOR: L2_QUAL_LST_D_GHA'
    WAPOR_L2_QUAL_NDVI_D_GHA = 'WAPOR_L2_QUAL_NDVI_GHA', 'WAPOR: L2_QUAL_NDVI_D_GHA'

    # VIIRS-1day-xxx
    FLOODLIGHT_VIIRS_1_DAY = 'FLOODLIGHT_VIIRS_1_DAY', 'VIIRS-1day'

    # NDVI_ANOM
    GMOD09Q1_NDVI_ANOM_TUN = 'GMOD09Q1_NDVI_ANOM_TUN', 'GMOD09Q1_NDVI_ANOM_TUN'
    GMOD09Q1_NDVI_ANOM_RWA = 'GMOD09Q1_NDVI_ANOM_RWA', 'GMOD09Q1_NDVI_ANOM_RWA'
    GMOD09Q1_NDVI_ANOM_ETH = 'GMOD09Q1_NDVI_ANOM_ETH', 'GMOD09Q1_NDVI_ANOM_ETH'
    GMOD09Q1_NDVI_ANOM_ZAF = 'GMOD09Q1_NDVI_ANOM_ZAF', 'GMOD09Q1_NDVI_ANOM_ZAF'
    GMOD09Q1_NDVI_ANOM_NER = 'GMOD09Q1_NDVI_ANOM_NER', 'GMOD09Q1_NDVI_ANOM_NER'
    GMOD09Q1_NDVI_ANOM_GHA = 'GMOD09Q1_NDVI_ANOM_GHA', 'GMOD09Q1_NDVI_ANOM_GHA'
    GMOD09Q1_NDVI_ANOM_MOZ = 'GMOD09Q1_NDVI_ANOM_MOZ', 'GMOD09Q1_NDVI_ANOM_MOZ'
    GMOD09Q1_NDVI_ANOM_KEN = 'GMOD09Q1_NDVI_ANOM_KEN', 'GMOD09Q1_NDVI_ANOM_KEN'


class EOSource(models.Model):
    # EO-Inputs
    """ A ledger for known files """

    # status of file.
    state = models.CharField(max_length=255,
                             choices=EOSourceStateChoices.choices,
                             default=EOSourceStateChoices.AvailableRemotely)

    # what product is it?
    group = models.CharField(max_length=255, choices=EOSourceGroupChoices.choices)
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
    #  eg ftp://ftp.globalland.cls.fr/path/filename.nc
    url = models.URLField(help_text="Resource URL")
    # username/password of resource
    credentials = models.ForeignKey("Credentials", on_delete=models.SET_NULL, null=True)
    rule = models.ForeignKey(to='EOSourceMeta', on_delete=models.DO_NOTHING, null=True)

    class Meta:
        ordering = ["group", "-datetime_reference"]

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
    from eo_engine.common.products import generate_products_from_source
    from eo_engine.models import EOProduct, EOProductStateChoices
    eo_source = instance
    # if asset is local
    if eo_source.state != EOSourceStateChoices.AvailableLocally:
        return
        # pass
    products = generate_products_from_source(eo_source.filename)

    for product in products:

        prod, created = EOProduct.objects.get_or_create(
            filename=product.filename,
            output_folder=product.output_folder,
            group=product.group,
            task_name=product.task_name,
            task_kwargs=product.task_kwargs
        )

        if eo_source.group.startswith('GMOD09Q1_NDVI_ANOM'):
            from eo_engine.common.s02p04 import is_GMOD09Q1_batch_complete
            if is_GMOD09Q1_batch_complete(eo_source):
                prod.state = EOProductStateChoices.Available
            else:
                prod.state = EOProductStateChoices.MISSING_SOURCE

        # mark if the scheduler should Ignore it
        if prod.is_ignored():
            print('this entry is marked as ignored.')
            prod.state = EOProductStateChoices.Ignore

        # mark it's inputs
        prod.eo_sources_inputs.add(eo_source)
        prod.save()


__all__ = [
    "EOSource",
    "EOSourceGroupChoices",
    "EOSourceStateChoices"
]
