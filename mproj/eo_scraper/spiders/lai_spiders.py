from eo_engine.models import EOSourceGroupChoices
from eo_scraper.spiders.abstract_spiders import CopernicusVgtDatapool

# special rules
# if this thing starts to bloat up, maybe consider spliting into different
# pipelines


class CGLS_LAISpider(CopernicusVgtDatapool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class LAI300mV1Spider(CGLS_LAISpider):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    name = EOSourceGroupChoices.S02P02_LAI_300M_V1_GLOB_CGLS
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
