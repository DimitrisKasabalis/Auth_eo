from eo_scraper.spiders.abstract_spiders import FtpSpider
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
class FtpGlobalLandWB100m(FtpSpider):
    name = 'ftp-spider-wb100'
    product_name = EOSourceGroupChoices.c_gls_WB100_v1_glob
    allowed_domains = ['ftp.globalland.cls.fr', ]

    ftp_root_url: str = 'ftp://ftp.globalland.cls.fr/home/glbland_ftp/Core/SIRS/dataset-sirs-wb-nrt-100m'


class FtpGlobalLandWB300m(FtpSpider):
    name = 'ftp-spider-wb300'
    product_name = EOSourceGroupChoices.WB_300m_v2_GLOB
    allowed_domains = ['ftp.globalland.cls.fr', ]

    ftp_root_url: str = 'ftp://ftp.globalland.cls.fr/home/glbland_ftp/Core/SIRS/dataset-sirs-wb-nrt-300m'


class FtpFloodLightViiR(FtpSpider):
    name = 'ftp-floodlight-viir'
    product_name = EOSourceGroupChoices.VIIRS_1day
    allowed_domain = ['floodlight.ssec.wisc.edu']

    ftp_root_url: str = 'ftp://floodlight.ssec.wisc.edu/composite'

    def filename_filter(self, filename):
        import re
        from eo_engine.common.parsers import parse_dt_from_generic_string as parse_dt
        # 'RIVER-FLDglobal-composite1_' + date + '_000000.part' + tile + '.tif'
        pat = re.compile(r'RIVER-FLDglobal-composite1_(?P<date>[0-9]{1,8})_000000.part(?P<tile>[0-9]{1,3}).tif')
        match = pat.match(filename)
        mytiles = ["070", "071", "072", "073", "082", "083", "084", "085", "086", "093", "094",
                   "095", "096", "097", "098", "104", "105", "106", "107", "113", "114", "115",
                   "116", "121", "122"]

        if match is not None \
                and match.groupdict()['tile'] in mytiles \
                and parse_dt(match.groupdict()['date']) >= parse_dt('20210901'):
            return True

        return False
