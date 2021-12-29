from eo_scraper.spiders.abstract_spiders import FtpSpider
from eo_engine.models import EOSourceGroupChoices


# https://github.com/laserson/ftptree/blob/master/ftptree_crawler/spiders.py
class FtpGlobalLandWB100m(FtpSpider):
    name = EOSourceGroupChoices.S06P01_WB_100M_V1_GLOB_CGLS
    allowed_domains = ['ftp.globalland.cls.fr', ]

    ftp_root_url: str = 'ftp://ftp.globalland.cls.fr/home/glbland_ftp/Core/SIRS/dataset-sirs-wb-nrt-100m'


class FtpGlobalLandWB300m(FtpSpider):
    name = EOSourceGroupChoices.S06P01_WB_300M_V2_GLOB_CGLS
    allowed_domains = ['ftp.globalland.cls.fr', ]

    ftp_root_url: str = 'ftp://ftp.globalland.cls.fr/home/glbland_ftp/Core/SIRS/dataset-sirs-wb-nrt-300m'


class FtpFloodLightViiR(FtpSpider):
    name = EOSourceGroupChoices.S04P03_FLD_375M_1D_VIIRS

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
