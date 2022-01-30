from django.db import models
from django.shortcuts import reverse
from django.utils.functional import cached_property

from typing import Optional
from urllib.parse import urlencode


class EOProductGroupChoices(models.TextChoices):
    # S02P02
    S02P02_NDVI_1KM_V3_AFR = "S02P02_NDVI_1KM_V3_AFR", 'AUTH/AGRO/NDVI 1km, V3 Africa'
    S02P02_NDVI_300M_V3_AFR = "S02P02_NDVI_300M_V3_AFR", 'AUTH/AGRO/NDVI 300m, V3 Africa'
    S02P02_NDVIA_250M_TUN = 'S02P02_NDVIA_250M_TUN', 'NDVI Anomaly 250M (TUN)'
    S02P02_NDVIA_250M_RWA = 'S02P02_NDVIA_250M_RWA', 'NDVI Anomaly 250M (RWA)'
    S02P02_NDVIA_250M_ETH = 'S02P02_NDVIA_250M_ETH', 'NDVI Anomaly 250M (ETH)'
    S02P02_NDVIA_250M_ZAF = 'S02P02_NDVIA_250M_ZAF', 'NDVI Anomaly 250M (ZAF)'
    S02P02_NDVIA_250M_NER = 'S02P02_NDVIA_250M_NER', 'NDVI Anomaly 250M (NER)'
    S02P02_NDVIA_250M_GHA = 'S02P02_NDVIA_250M_GHA', 'NDVI Anomaly 250M (GHA)'
    S02P02_NDVIA_250M_MOZ = 'S02P02_NDVIA_250M_MOZ', 'NDVI Anomaly 250M (MOZ)'
    S02P02_NDVIA_250M_KEN = 'S02P02_NDVIA_250M_KEN', 'NDVI Anomaly 250M (KEN)'

    S02P02_LAI_300M_V1_AFR = "S02P02_LAI_300M_V1_AFR", 'AUTH/AGRO/LAI 300m, V1 Africa'
    S02P02_LAI_1KM_V2_AFR = "S02P02_LAI_1KM_V2_AFR", 'AUTH/AGRO/LAI 1km, V2 Africa'

    AGRO_VCI_1KM_V2_AFR = 'AGRO_VCI_1KM_V2_AFR', 'AUTH/AGRO/VCI 1km, V2 Africa'

    # S04P03
    S04P03_FLD_375M_1D_AFR = 'S04P03_FLD_375M_1D_AFR', 'FLD 375M 1D AFR'
    S04P03_FLD_10m_BAG = 'S04P03_FLD_10m_BAG', 'FLD 10m BAG'

    # S04P01
    S04P01_LULC_500M_AFR = 'S04P01_LULC_500M_AFR', 'Land Cover 500m Africa'

    # S06P01
    S06P01_WB_100M_V1_ETH = 'S06P01_WB_100M_V1_ETH', 'WaterBodies 100Mv1 (ETH)'
    S06P01_WB_100M_V1_GHA = 'S06P01_WB_100M_V1_GHA', 'WaterBodies 100Mv1 (GHA)'
    S06P01_WB_100M_V1_KEN = 'S06P01_WB_100M_V1_KEN', 'WaterBodies 100Mv1 (KEN)'
    S06P01_WB_100M_V1_MOZ = 'S06P01_WB_100M_V1_MOZ', 'WaterBodies 100Mv1 (MOZ)'
    S06P01_WB_100M_V1_NER = 'S06P01_WB_100M_V1_NER', 'WaterBodies 100Mv1 (NER)'
    S06P01_WB_100M_V1_RWA = 'S06P01_WB_100M_V1_RWA', 'WaterBodies 100Mv1 (RWA)'
    S06P01_WB_100M_V1_TUN = 'S06P01_WB_100M_V1_TUN', 'WaterBodies 100Mv1 (TUN)'
    S06P01_WB_100M_V1_ZAF = 'S06P01_WB_100M_V1_ZAF', 'WaterBodies 100Mv1 (ZAF)'
    S06P01_WB_300M_V2_AFR = 'S06P01_WB_300M_V2_AFR', 'WaterBodies 300Mv2 (AFR)'
    S06P01_WB_10M_KZN = 'S06P01_WB_10M_KZN', 'WaterBodies 10m (KZN)'
    S06P01_WB_10M_BAG = 'S06P01_WB_10M_BAG', 'WaterBodies 10m (BAG)'

    # S06P04
    S06P04_ET_3KM_AFR = 'S06P04_ET_3KM_AFR', 'LSA-SAF 3km Africa'
    S06P04_ETAnom_5KM_M_AFR = 'S06P04_ETAnom_5KM_M_AFR', 'S06P04_ETAnom_5KM_M_AFR'
    S06P04_AETI_250M_D_AFR = 'S06P04_AETI_250M_D_AFR', 'S06P04_AETI_250M_D_AFR'
    S06P04_AETI_100M_D_TUN = 'S06P04_AETI_100M_D_TUN', 'S06P04_AETI_100M_D_TUN'
    S06P04_AETI_100M_D_KEN = 'S06P04_AETI_100M_D_KEN', 'S06P04_AETI_100M_D_KEN'
    S06P04_AETI_100M_D_MOZ = 'S06P04_AETI_100M_D_MOZ', 'S06P04_AETI_100M_D_MOZ'
    S06P04_AETI_100M_D_RWA = 'S06P04_AETI_100M_D_RWA', 'S06P04_AETI_100M_D_RWA'
    S06P04_AETI_100M_D_ETH = 'S06P04_AETI_100M_D_ETH', 'S06P04_AETI_100M_D_ETH'
    S06P04_L1_AETI_D_GHA = 'S06P04_L1_AETI_D_GHA', 'S06P04_L1_AETI_D_GHA'


class EOSourceGroupChoices(models.TextChoices):
    # Naming convention:
    # <PACKAGE_NAME>_OTHER
    #  S02P02
    S02P02_NDVI_300M_V2_GLOB_CGLS = 'S02P02_NDVI_300M_V2_GLOB_CGLS', "Copernicus Global Land Service NDVI 300m v2"
    S02P02_NDVI_300M_V3_AFR = 'S02P02_NDVI_300M_V3_AFR', "Generated NDVI  300M v3"
    S02P02_NDVI_1KM_V3_AFR = 'S02P02_NDVI_1KM_V3_AFR', "Generated  NDVI 1KM V3 Africa"
    S02P02_LAI_300M_V1_GLOB_CGLS = 'S02P02_LAI_300M_V1_GLOB_CGLS', "Copernicus Global Land Service LAI 300m v1"
    S02P02_LAI_300M_V1_AFR = 'S02P02_LAI_300M_V1_AFR', "Generated LAI 300M V1"

    S02P02_NDVIA_250M_TUN_GMOD = 'S02P02_NDVIA_250M_TUN_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (TUN)'
    S02P02_NDVIA_250M_RWA_GMOD = 'S02P02_NDVIA_250M_RWA_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (RWA)'
    S02P02_NDVIA_250M_ETH_GMOD = 'S02P02_NDVIA_250M_ETH_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (ETH)'
    S02P02_NDVIA_250M_ZAF_GMOD = 'S02P02_NDVIA_250M_ZAF_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (ZAF)'
    S02P02_NDVIA_250M_NER_GMOD = 'S02P02_NDVIA_250M_NER_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (NER)'
    S02P02_NDVIA_250M_GHA_GMOD = 'S02P02_NDVIA_250M_GHA_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (GHA)'
    S02P02_NDVIA_250M_MOZ_GMOD = 'S02P02_NDVIA_250M_MOZ_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (MOZ)'
    S02P02_NDVIA_250M_KEN_GMOD = 'S02P02_NDVIA_250M_KEN_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (KEN)'

    # S04P01
    S04P01_LULC_500M_MCD12Q1_v6 = 'S04P01_LULC_500M_MCD12Q1_v6', '(MODIS S04P01) Land Cover 500M v6'

    #  S04P03
    S04P03_FLD_375M_1D_VIIRS = 'S04P03_FLD_375M_1D_VIIRS', 'FLD 375M 1D VIIRS'
    S04P03_WB_10m_BAG = 'S04P03_WB_10m_BAG', 'WB 10m BAG'

    #  S06P01
    S06P01_WB_100M_V1_GLOB_CGLS = 'S06P01_WB_100M_V1_GLOB_CGLS', "Copernicus Global Land Service Water Bodies Collection 100m Version 2"
    S06P01_WB_300M_V2_GLOB_CGLS = 'S06P01_WB_300M_V2_GLOB_CGLS', "Copernicus Global Land Service Water Bodies Collection 300m Version 2"

    S06P01_S1_10M_KZN = 'S06P01_S1_10M_KZN', 'Sentinel 10m KZN'
    S06P01_S1_10M_BAG = 'S06P01_S1_10M_BAG', 'Sentinel 10m BAG'

    # S06P04
    S06P04_ETAnom_5KM_M_GLOB_SSEBOP = 'S06P04_ETAnom_5KM_M_GLOB_SSEBOP', 'S06P04: ETAnom 5KM M GLOB SSEBOP'
    S06P04_ET_3KM_GLOB_MSG = 'S06P04_ET_3KM_GLOB_MSG', 'S06P04: LSAF 3KM DMET'

    S06P04_WAPOR_L1_AETI_D_AFRICA = 'S06P04_WAPOR_L1_AETI_D_AFRICA', 'WAPOR: L1 AETI D AFRICA'
    S06P04_WAPOR_L1_QUAL_LST_D_AFRICA = 'S06P04_WAPOR_L1_QUAL_LST_D_AFRICA', 'WAPOR: L1_QUAL_LST_D_AFRICA'
    S06P04_WAPOR_L1_QUAL_NDVI_D_AFRICA = 'S06P04_WAPOR_L1_QUAL_NDVI_D_AFRICA', 'WAPOR: L1_QUAL_NDVI_D_AFRICA'

    S06P04_WAPOR_L2_AETI_D_ETH = 'S06P04_WAPOR_L2_AETI_D_ETH', 'WAPOR: WAPOR_L2_AETI_D_ETH'
    S06P04_WAPOR_L2_AETI_D_GHA = 'S06P04_WAPOR_L2_AETI_D_GHA', 'WAPOR: L2_AETI_GHA'
    S06P04_WAPOR_L2_AETI_D_KEN = 'S06P04_WAPOR_L2_AETI_D_KEN', 'WAPOR: L2_AETI_D_KEN'
    S06P04_WAPOR_L2_AETI_D_MOZ = 'S06P04_WAPOR_L2_AETI_D_MOZ', 'WAPOR: L2_AETI_D_MOZ'
    S06P04_WAPOR_L2_AETI_D_RWA = 'S06P04_WAPOR_L2_AETI_D_RWA', 'WAPOR: L2_AETI_D_DRWA'
    S06P04_WAPOR_L2_AETI_D_TUN = 'S06P04_WAPOR_L2_AETI_D_TUN', 'WAPOR: L2_AETI_D_TUN'

    S06P04_WAPOR_L2_QUAL_LST_D_ETH = 'S06P04_WAPOR_L2_QUAL_LST_D_ETH', 'WAPOR: L2_QUAL_LST_D_ETH'
    S06P04_WAPOR_L2_QUAL_LST_D_GHA = 'S06P04_WAPOR_L2_QUAL_LST_D_GHA', 'WAPOR: L2_QUAL_LST_D_GHA'
    S06P04_WAPOR_L2_QUAL_LST_D_KEN = 'S06P04_WAPOR_L2_QUAL_LST_D_KEN', 'WAPOR: L2_QUAL_LST_D_KEN'
    S06P04_WAPOR_L2_QUAL_LST_D_MOZ = 'S06P04_WAPOR_L2_QUAL_LST_D_MOZ', 'WAPOR: L2_QUAL_LST_D_MOZ'
    S06P04_WAPOR_L2_QUAL_LST_D_RWA = 'S06P04_WAPOR_L2_QUAL_LST_D_RWA', 'WAPOR: L2_QUAL_LST_D_RWA'
    S06P04_WAPOR_L2_QUAL_LST_D_TUN = 'S06P04_WAPOR_L2_QUAL_LST_D_TUN', 'WAPOR: L2_QUAL_LST_D_TUN'

    S06P04_WAPOR_L2_QUAL_NDVI_D_ETH = 'S06P04_WAPOR_L2_QUAL_NDVI_D_ETH', 'WAPOR: L2_QUAL_NDVI_D_ETH'
    S06P04_WAPOR_L2_QUAL_NDVI_D_GHA = 'S06P04_WAPOR_L2_QUAL_NDVI_D_GHA', 'WAPOR: L2_QUAL_NDVI_D_GHA'
    S06P04_WAPOR_L2_QUAL_NDVI_D_KEN = 'S06P04_WAPOR_L2_QUAL_NDVI_D_KEN', 'WAPOR: L2_QUAL_NDVI_D_KEN'
    S06P04_WAPOR_L2_QUAL_NDVI_D_MOZ = 'S06P04_WAPOR_L2_QUAL_NDVI_D_MOZ', 'WAPOR: L2_QUAL_NDVI_D_MOZ'
    S06P04_WAPOR_L2_QUAL_NDVI_D_RWA = 'S06P04_WAPOR_L2_QUAL_NDVI_D_RWA', 'WAPOR: L2_QUAL_NDVI_D_RWA'
    S06P04_WAPOR_L2_QUAL_NDVI_D_TUN = 'S06P04_WAPOR_L2_QUAL_NDVI_D_TUN', 'WAPOR: L2_QUAL_NDVI_D_TUN'


class EOGroup(models.Model):
    description = models.TextField(default='No-Description')

    # noinspection PyUnresolvedReferences,PyStatementEffect
    def discover_url(self) -> Optional[dict]:
        raise NotImplementedError()

    def as_eosource_group(self):
        try:
            return EOSourceGroup.objects.get(pk=self.pk)
        except EOSourceGroup.DoesNotExist:
            return None

    def as_eoproduct_group(self):
        try:
            return EOProductGroup.objects.get(pk=self.pk)
        except EOProductGroup.DoesNotExist:
            return None


class EOProductGroup(EOGroup):
    name = models.TextField(choices=EOSourceGroupChoices.choices, unique=True)

    def discover_url(self) -> Optional[dict]:
        return None

    def submit_schedule_for_generation(self) -> Optional[dict]:
        from eo_engine.tasks import task_utils_generate_eoproducts_for_eo_product_group as task
        return {
            'label': f'Generate All',
            'url': '?'.join((
                reverse('eo_engine:submit-task'),
                urlencode({'task_name': task.__name__,
                           'eo_product_id': self.id})
            ))
        }


class EOSourceGroup(EOGroup):
    class CrawlerTypeChoices(models.TextChoices):
        NONE = 'NONE', 'Not using crawling'
        OTHER_PYMODIS = 'OTHER (PYMODIS)', 'PYMODIS API'
        OTHER_SENTINEL = 'OTHER (SENTINEL)', 'Sentinel'
        OTHER_SFTP = 'OTHER (SFTP)', 'SFTP Crawler'
        OTHER_WAPOR = 'OTHER (WAPOR)', 'Wapor on demand'
        SCRAPY_SPIDER = 'SCRAPY_SPIDER', 'Scrappy Spider'

    name = models.TextField(choices=EOSourceGroupChoices.choices, unique=True)
    date_regex = models.TextField(
        help_text='RegEx that extracts the date element (as the yymmdd or yyyymmdd named group). '
                  'If not provided the ref date field must have a way to be populated')
    crawler_type = models.TextField(choices=CrawlerTypeChoices.choices, default=CrawlerTypeChoices.NONE)

    def submit_schedule_for_download(self) -> Optional[dict]:
        from eo_engine.tasks import task_utils_download_eo_sources_for_eo_source_group
        return {
            'label': f'Download Remote Files',
            'url': '?'.join((
                reverse('eo_engine:submit-task'),
                urlencode({'task_name': task_utils_download_eo_sources_for_eo_source_group.__name__,
                           'eo_source_group_id': self.id})
            ))
        }

    def discover_url(self) -> Optional[dict]:
        if self.crawler_type == self.CrawlerTypeChoices.SCRAPY_SPIDER:
            return {
                'label': 'Start Spider',
                'url': reverse('eo_engine:crawler-configure', kwargs={
                    'group_name': self.name})
            }

        if self.crawler_type == self.CrawlerTypeChoices.OTHER_WAPOR:
            return {
                'label': 'Generate WAPOR Entry',
                'url': reverse('eo_engine:create-wapor', kwargs={'group_name': self.name})
            }
        if self.crawler_type == self.CrawlerTypeChoices.OTHER_SFTP:
            return {
                'label': 'Start SFTP Crawl',
                'url': reverse('eo_engine:crawler-configure', kwargs={'group_name': self.name})
            }
        if self.crawler_type == self.CrawlerTypeChoices.OTHER_SENTINEL:
            return {
                'label': 'Generate Sentinel Entry',
                'url': reverse('eo_engine:create-sentinel', kwargs={'group_name': self.name})
            }

    def as_eo_product_group(self) -> Optional[EOProductGroup]:
        return EOProductGroup.objects.get(pk=self.pk)

    @cached_property
    def date_regex_cached(self):
        return self.date_regex
