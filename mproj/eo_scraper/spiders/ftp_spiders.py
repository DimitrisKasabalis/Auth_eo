import re
from datetime import datetime

from eo_engine.models import EOSourceGroupChoices
from eo_scraper.spiders.abstract_spiders import FtpSpider


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

    tiles = ["070", "071", "072", "073", "082", "083", "084", "085", "086", "093", "094",
             "095", "096", "097", "098", "104", "105", "106", "107", "113", "114", "115",
             "116", "121", "122"]

    def should_process_filename(self, filename: str) -> bool:
        pattern = re.compile(r'RIVER-FLDglobal-composite1_(?P<YYYYMMDD>\d{8})_000000.part(?P<tile>\d{1,3}).tif',
                             re.IGNORECASE)
        match = pattern.match(filename)
        if match is None:
            self.logger.warn(
                f'Potential BUG REPORT. Filename +{filename}+ ws not captured by the tile regex: +{pattern.pattern}+')
            return False
        groupdict = match.groupdict()

        def check_tile() -> bool:
            tile = groupdict['tile']
            return bool(self.tiles.count(tile))

        def check_date():
            date_str = groupdict['YYYYMMDD']
            date = datetime.strptime(date_str, '%Y%m%d').date()
            return date >= self.from_date

        return check_tile() & check_date()
