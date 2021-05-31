from eo_engine.models import EOSourceProductChoices, EOSourceProductGroupChoices
from eo_scraper.spiders.abstract_spiders import CopernicusVgtDatapool


class LAISpider(CopernicusVgtDatapool):
    product_group = EOSourceProductGroupChoices.LAI

class LAI300mV1Spider(LAISpider):
    name = "lai-300m-v1-spider"
    product_name = EOSourceProductChoices.lai_300m_v1
    start_urls = [
        f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Properties/LAI_300m_V1/"
    ]


class LAI1kmGlobalV1Spider(LAISpider):
    name = "lai-1km-global-v1-spider"
    product_name = EOSourceProductChoices.lai_1km_v1
    start_urls = [
        f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Properties/LAI_1km_Global_V1/"
    ]


class LAI1kmV2Spider(LAISpider):
    name = "lai-1km-v2-spider"
    product_name = EOSourceProductChoices.lai_1km_v2
    start_urls = [
        f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Properties/LAI_1km_V2/"
    ]
