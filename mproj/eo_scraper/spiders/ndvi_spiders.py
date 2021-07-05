from eo_scraper.spiders.abstract_spiders import CopernicusVgtDatapool

from eo_engine.models import EOSourceProductChoices


class NDVISpider(CopernicusVgtDatapool):
    pass
    # product_group = EOSourceProductGroupChoices.NDVI


# class NDVI300mV1Spider(NDVISpider):
#     name = "ndvi-300m-v1-spider"
#     product_name = EOSourceProductChoices.???
#     # product_group = EOSourceProductGroupChoices.NDVI
#     start_urls = [
#         f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Indicators/NDVI_300m_V1/"
#     ]


class NDVI300mV2Spider(NDVISpider):
    name = "ndvi-300m-v2-spider"
    product_name = EOSourceProductChoices.c_gsl_ndvi300_v2_glob
    start_urls = [
        f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Indicators/NDVI_300m_V2/"
    ]


class NDVI1kmV3Spider(NDVISpider):
    name = "ndvi-1km-v3-spider"
    product_name = EOSourceProductChoices.c_gsl_ndvi1km_v3_glob
    start_urls = [
        f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Indicators/NDVI_1km_V3/"
    ]


# class NDVI1kmV2Spider(NDVISpider):
#     name = "ndvi-1km-v2-spider"
#     product_name = EOSourceProductChoices.ndvi_1km_v2
#     start_urls = [
#         f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Indicators/NDVI_1km_V2/"
#     ]


# class NDVI1kmV1Spider(NDVISpider):
#     name = "ndvi-1km-v1-spider"
#     product_name = EOSourceProductChoices.ndvi_1km_v1
#     start_urls = [
#         f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Indicators/NDVI_1km_V1/"
#     ]
