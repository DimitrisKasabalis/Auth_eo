from datetime import datetime
from functools import cache
from pathlib import Path
import string
import re

from dateutil.parser import parse

from eo_engine.common.copernicus import copernicus_name_elements, copernicus_parse_dt, copernicus_parse_version

punctuation_chars = re.escape(string.punctuation)


@cache
def parse_dt_from_generic_string(timestr: str) -> datetime:
    """ Tries clean a string and then parse it as datetime element """
    timestr = timestr.lower()  # its a copy

    # The final path component, without a suffix if any:
    timestr = Path(timestr).stem

    # remove toxic sub-strings
    TOXIC_WORDS = [
        'hdf5',
    ]
    # convert punctuation_chars to whitespaces
    timestr = re.sub(r'[' + punctuation_chars + ']', ' ', timestr)

    # delete any TOXIC words
    for word in TOXIC_WORDS:
        timestr = timestr.replace(word, '')

    return parse(timestr, fuzzy=True)


def parse_copernicus_name(filename: str) -> copernicus_name_elements:
    # c_gls_LAI_<YYYYMMDDHHMM>_<AREA>_<SENSOR>_V<VERSION>

    # info at
    # https://land.copernicus.eu/global/sites/cgls.vito.be/files/products/CGLOPS1_PUM_LAI1km-VGT-V1_I1.20.pdf
    # page 24 from 44

    parts = filename.split('_')
    product = '_'.join(parts[:3])
    dt = copernicus_parse_dt(parts[3])
    area = parts[4]  # should be always global
    sensor = parts[5]
    version = copernicus_parse_version(parts[6])

    return copernicus_name_elements(product=product, datetime=dt, area=area, sensor=sensor, version=version)
