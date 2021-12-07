# place regex patterns here
import re

GMOD09Q1_DATE_PATTERN = re.compile(r'.*A((?P<year>\d{4})(?P<doy>\d{3})).*gz$', re.IGNORECASE)
GMOD09Q1_FILE_REGEX = re.compile(r'.*\.(?P<tile>(x\d{1,2}y\d{1,2})).*\.tif\.gz')

RIVER_FLD_GLOBAL = re.compile(r'RIVER-FLDglobal-composite1_(?P<YYYYMMDD>[0-9]{1,8})_000000.part(?P<tile>[0-9]{1,3}).tif')
