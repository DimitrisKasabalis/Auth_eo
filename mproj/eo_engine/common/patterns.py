# place regex patterns here
import re

# GMOD09Q1, s02p02
GMOD09Q1_DATE_PATTERN = re.compile(r'.*A((?P<year>\d{4})(?P<doy>\d{3})).*gz$', re.IGNORECASE)
GMOD09Q1_PAGE_PATTERN = re.compile(r'^https://gimms\.gsfc\.nasa\.gov/MODIS/std/GMOD09Q1/tif/NDVI_anom_S2001-2015(/(?P<year>\d{4})?(/(?P<doy>\d{3}))?)?/$')

# GMOD09Q1.A2019057.08d.latlon.x01y04.6v1.NDVI_anom_S2001-2015.tif.gz
GMOD09Q1_FILE_REGEX = re.compile(r'^GMOD09Q1\.A((?P<year>\d{4})(?P<doy>\d{3})).*(?P<tile>(x\d{1,2}y\d{1,2}))\.6v1.NDVI_anom_S2001-2015\.tif\.gz$')

RIVER_FLD_GLOBAL = re.compile(r'RIVER-FLDglobal-composite1_(?P<YYYYMMDD>[0-9]{1,8})_000000.part(?P<tile>[0-9]{1,3}).tif')
