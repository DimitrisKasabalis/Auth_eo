from datetime import date
from re import Match
from scrapy.http.response import Response
from typing import Optional

from eo_engine.models import EOSourceGroupChoices
from eo_scraper.spiders.abstract_spiders import CopernicusVgtDatapool


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

from datetime import date as dt_date


class NDVI300mV2Spider(NDVISpider):
    def datetime_reference_from_filename(self, filename) -> Optional[dt_date]:
        from eo_engine.common.patterns import S02P02_GCL_NDVI_V2_300M_FILENAME_PARSE
        match = S02P02_GCL_NDVI_V2_300M_FILENAME_PARSE.match(filename)
        if match:
            groupdict = match.groupdict()
            year = int(groupdict['year'])
            month = int(groupdict['month'])
            day = int(groupdict['day'])

            date: dt_date = dt_date(year=year, month=month, day=day)
            return date
        else:
            self.logger.warn(f'The date time pattern failed, returning None. The filename was {filename}')
            return None

    name = EOSourceGroupChoices.C_GLS_NDVI_300M_V2_GLOB
    product_name = EOSourceGroupChoices.C_GLS_NDVI_300M_V2_GLOB
    start_urls = [
        f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Indicators/NDVI_300m_V2/"
    ]

    def pre_parse_check(self, response: Response) -> bool:
        from eo_engine.common.patterns import S02P02_GCL_NDVI_V2_300M_DATE_MONTH as pattern
        url = response.url
        match: Match = pattern.match(url)
        if match:
            groupdict = match.groupdict()
            year = int(groupdict['year'])
            month = int(groupdict['month'])
            directory_date = date(year, month, 1)
            self.logger.info(f'Comparing dates!:  {directory_date <= self.from_date}')
            return directory_date <= self.from_date
        else:
            self.logger.error(f"The pattern didn't match for this url. The url was: {url}")
            raise Exception


class NDVI1kmV3Spider(NDVISpider):
    name = EOSourceGroupChoices.C_GLS_NDVI_1KM_V3_GLOB
    product_name = EOSourceGroupChoices.C_GLS_NDVI_1KM_V3_GLOB
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
