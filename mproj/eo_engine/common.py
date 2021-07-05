from datetime import datetime
from fnmatch import fnmatch
from pytz import utc
from typing import Literal, List, Union, Dict, Any, Optional

from eo_engine.models import EOSource

from typing import NamedTuple

copernicus_name_elements = NamedTuple('copernicus_name_elements',
                                      [('product', str),
                                       ('datetime', datetime),
                                       ('area', Literal['GLOBAL']),
                                       ('sensor', str),
                                       ('version', str)
                                       ])

product_output = NamedTuple('product_output',
                            [('output_folder', str),
                             ('filename', str),
                             ('group', str),
                             ('task_name', str),
                             ('task_kwargs', Optional[Dict[str, Any]])
                             ])


def copernicus_parse_dt(token) -> datetime:
    """returns dt object, utc timezone"""
    return datetime.strptime(token, '%Y%m%d%H%S').replace(tzinfo=utc)


def copernicus_parse_version(token: str) -> str:
    return '.'.join(token.lower().split('.')[:-1]).replace('v', '')


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


def generate_products(eo_source: EOSource) -> List[product_output]:
    """ MOAR """
    # check https://docs.google.com/spreadsheets/d/1C59BF349gMW-HxnEoWT1Pnxj0tuHOwX7/edit#gid=1748752542
    filename = eo_source.filename

    if fnmatch(filename.lower(), 'c_gls_ndvi300*.nc'):
        name_elements = parse_copernicus_name(filename)
        date_str_YYYYMMDD = name_elements.datetime.date().strftime('%Y%m%d')
        return [
            product_output('S2_PO2/NDVI_300', f"{date_str_YYYYMMDD}_SE3_AFR_0300m_0010_NDVI.nc", 'NDVI-300m-v2-AFR',
                           'task_s02p02_ndvi_vci_clip_ndvi', {'aoi': [-30, 40, 60, -40]}),
            product_output('S2_P02/NDVI_1km', f"{date_str_YYYYMMDD}_SE3_AFR_1000m_0010_NDVI.nc", "NDVI-1km-v3-AFR",
                           'task_c_gls_nvdi_resample_from_300m_to_1km', {'aoi': [-30, 40, 60, -40]})
        ]

    elif fnmatch(filename.lower(), 'c_gls_LAI300-RT1*.nc'):
        name_elements = parse_copernicus_name(filename)
        date_str_YYYYMMDD = name_elements.datetime.date().strftime('%Y%m%d')
        return [
            product_output('S2_P02/LAI_300', f"{date_str_YYYYMMDD}_SE3_AFR_0300m_0010_LAI.nc", "LAI-300m-v1-AFR"),
            product_output('S2_P02/LAI_1km', f"{date_str_YYYYMMDD}_SE3_AFR_1000m_0010_LAI.nc", 'LAI-1km-v2-AFR'),
        ]

    elif fnmatch(filename, 'c_gls_WB100*V1.0.1.nc'):
        name_elements = parse_copernicus_name(filename)
        YYYYMM = name_elements.datetime.date().strftime('%Y%m')
        return [
            product_output('S6_P01/WB_100/TUN', f"{YYYYMM}_SE2_TUN_0100m_0030_WBMA.nc", 'WB-100m-TUN'),
            product_output('S6_P01/WB_100/RWA', f"{YYYYMM}_SE2_RWA_0100m_0030_WBMA.nc", 'WB-100m-RWA'),
            product_output('S6_P01/WB_100/ETH', f"{YYYYMM}_SE2_ETH_0100m_0030_WBMA.nc", 'WB-100m-ETH'),
            product_output('S6_P01/WB_100/MOZ', f"{YYYYMM}_SE2_MOZ_0100m_0030_WBMA.nc", 'WB-100m-MOZ'),
            product_output('S6_P01/WB_100/ZAF', f"{YYYYMM}_SE2_ZAF_0100m_0030_WBMA.nc", 'WB-100m-ZAF'),
            product_output('S6_P01/WB_100/GHA', f"{YYYYMM}_SE2_GHA_0100m_0030_WBMA.nc", 'WB-100m-GHA'),
            product_output('S6_P01/WB_100/NER', f"{YYYYMM}_SE2_NER_0100m_0030_WBMA.nc", 'WB-100m-NER'),
        ]

    return []


def file_is_valid(response, filepath) -> bool:
    return True
