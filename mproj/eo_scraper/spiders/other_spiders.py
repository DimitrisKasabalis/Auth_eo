from eo_engine.models import EOSourceProductChoices
from eo_scraper.spiders.abstract_spiders import CopernicusVgtDatapool

from twisted.protocols.ftp import FTPFileListProtocol


# class OtherSpiders(CopernicusVgtDatapool):
#     product_group = EOSourceProductGroupChoices.OTHER
#
#
# class VCIV1Spider(OtherSpiders):
#     name = "vci-v1-spider"
#     product_name = EOSourceProductChoices.vc1_v1
#     product_group = ''
#     start_urls = [
#         f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Indicators/VCI_V1/"
#     ]
#
#
# class WBAfricaV1Spider(OtherSpiders):
#     name = "wb-africa-v1-spider"
#     product_name = EOSourceProductChoices.wb_africa_v1
#     start_urls = [
#         f"https://land.copernicus.vgt.vito.be/PDF/datapool/Water/Water_Bodies/WB_Africa_V1/"
#     ]
