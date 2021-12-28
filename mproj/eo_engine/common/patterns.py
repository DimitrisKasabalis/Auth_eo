# place regex patterns here
import re

# GMOD09Q1, s02p02
# https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Indicators/NDVI_300m_V2/2015/01/
S02P02_GCL_NDVI_V2_300M_DATE_MONTH_FOLDER_PATTERN = re.compile(
    r'https://land\.copernicus\.vgt\.vito\.be/PDF/datapool/Vegetation/Indicators/NDVI_300m_V2/(?P<year>\d{4})/(?P<month>\d{2})/',
    re.IGNORECASE)

# c_gls_NDVI300_202009110000_GLOBE_OLCI_V2.0.1.nc
# c_gls_NDVI300_202109010000_GLOBE_OLCI_V2.0.2.nc
S02P02_GCL_NDVI_V2_300M_FILENAME_PARSE = re.compile(
    r'c_gls_NDVI300_(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})0000_GLOBE_OLCI_V2\.0\.[1|2]\.nc', re.IGNORECASE
)

GMOD09Q1_DATE_PATTERN = re.compile(r'.*A((?P<year>\d{4})(?P<doy>\d{3})).*gz$', re.IGNORECASE)
GMOD09Q1_PAGE_PATTERN = re.compile(
    r'^https://gimms\.gsfc\.nasa\.gov/MODIS/std/GMOD09Q1/tif/NDVI_anom_S2001-2018(/(?P<year>\d{4})?(/(?P<doy>\d{3}))?)?/$')

RIVER_FLD_GLOBAL = re.compile(
    r'RIVER-FLDglobal-composite1_(?P<YYYYMMDD>[0-9]{1,8})_000000.part(?P<tile>[0-9]{1,3}).tif')
