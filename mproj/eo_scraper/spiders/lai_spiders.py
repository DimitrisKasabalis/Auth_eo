from eo_engine.models import EOSourceGroupChoices
from eo_scraper.spiders.abstract_spiders import CopernicusVgtDatapool


class LAISpider(CopernicusVgtDatapool):
    pass
    # product_group = EOSourceProductGroupChoices.LAI


class LAI300mV1Spider(LAISpider):
    name = EOSourceGroupChoices.C_GLS_LAI_300M_V1_GLOB
    product_name = EOSourceGroupChoices.C_GLS_LAI_300M_V1_GLOB
    start_urls = [
        f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Properties/LAI_300m_V1/"
    ]
#
#
# class LAI1kmGlobalV1Spider(LAISpider):
#     name = "lai-1km-global-v1-spider"
#     product_name = EOSourceProductChoices.lai_1km_v1
#     start_urls = [
#         f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Properties/LAI_1km_Global_V1/"
#     ]
#
#
# class LAI1kmV2Spider(LAISpider):
#     name = "lai-1km-v2-spider"
#     product_name = EOSourceProductChoices.lai_1km_v2
#     start_urls = [
#         f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Properties/LAI_1km_V2/"
#     ]
