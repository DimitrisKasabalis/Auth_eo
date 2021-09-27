from datetime import datetime
from typing import NamedTuple, Literal

from pytz import utc


def copernicus_parse_dt(token) -> datetime:
    """returns dt object, utc timezone"""
    return datetime.strptime(token, '%Y%m%d%H%S').replace(tzinfo=utc)


def copernicus_parse_version(token: str) -> str:
    return '.'.join(token.lower().split('.')[:-1]).replace('v', '')


copernicus_name_elements = NamedTuple('copernicus_name_elements',
                                      [('product', str),
                                       ('datetime', datetime),
                                       ('area', Literal['GLOBAL']),
                                       ('sensor', str),
                                       ('version', str)
                                       ])


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
