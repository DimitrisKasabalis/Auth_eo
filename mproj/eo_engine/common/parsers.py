import re
import string
from datetime import datetime
from functools import cache
from pathlib import Path
from pytz import utc
from dateutil.parser import parse

from eo_engine.common.copernicus import copernicus_name_elements

punctuation_chars = re.escape(string.punctuation)

# https://land.copernicus.eu/global/sites/cgls.vito.be/files/products/CGLOPS1_PUM_LAI1km-VGT-V1_I1.20.pdf
COPERNICUS_REGEX = re.compile(
    r'c_gls_'
    r'(?P<PRODUCT>[A-Za-z0-9].+?)_'
    r'(?P<YYYYMMDDHHMM>[0-9]{12})_'
    r'(?P<AREA>[\w]+)_'
    r'(?P<SENSOR>[A-Z0-9]+)_'
    r'V(?P<VERSION>\d\.\d\.\d).+',
    re.IGNORECASE)


@cache
def copernicus_parse_dt(token) -> datetime:
    """returns dt object, utc timezone"""
    return datetime.strptime(token, '%Y%m%d%H%S').replace(tzinfo=utc)


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
    match = COPERNICUS_REGEX.match(filename)
    if match:
        match_dict = match.groupdict()
        return copernicus_name_elements(
            product=match_dict['PRODUCT'],
            datetime=parse_dt_from_generic_string(match_dict['YYYYMMDDHHMM']),
            area=match_dict['AREA'],
            version=match_dict['VERSION'],
            sensor=match_dict['SENSOR']
        )
