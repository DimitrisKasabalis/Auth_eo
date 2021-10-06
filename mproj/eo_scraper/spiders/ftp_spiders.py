from eo_scraper.spiders.abstract_spiders import FtpSpider, AnonFtpRequest
from eo_engine.models import EOSourceGroupChoices


#  it's not FTP, it's SFTP so it doesn't work. I will leave it here
# as reference how to implement an FTP spider!
# class LSASAFProducts(FtpSpider):
#     custom_settings = {
#         'ROBOTSTXT_OBEY': False
#     }
#     ftp_root_url = 'ftp://safmil.ipma.pt/home/safpt/OperationalChain/LSASAF_Products/DMET'
#     allowed_domains = ['safmil.ipma.pt', ]
#     name = 'LSASAF-ET-3000M'
#     product_name = EOSourceGroupChoices.LSASAF_ET_3000M


# https://github.com/laserson/ftptree/blob/master/ftptree_crawler/spiders.py
class FtpGlobalLand(FtpSpider):

    name = 'ftp-spider-wb100'
    product_name = EOSourceGroupChoices.c_gls_WB100_v1_glob
    allowed_domains = ['ftp.globalland.cls.fr', ]

    ftp_root_url: str = 'ftp://ftp.globalland.cls.fr/home/glbland_ftp/Core/SIRS/dataset-sirs-wb-nrt-100m'
