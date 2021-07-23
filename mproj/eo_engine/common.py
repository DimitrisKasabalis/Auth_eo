from tempfile import NamedTemporaryFile, TemporaryFile
from datetime import datetime
from fnmatch import fnmatch
from typing import Literal, List, Dict, Any, Optional
from typing import NamedTuple
from pathlib import Path

from django.core.files import File

from eo_engine.models.eo_product import EOProductGroupChoices

from pytz import utc

from eo_engine.models import EOSource

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


def generate_products_from_source(eo_source: EOSource) -> List[product_output]:
    """ MOAR """
    # check https://docs.google.com/spreadsheets/d/1C59BF349gMW-HxnEoWT1Pnxj0tuHOwX7/edit#gid=1748752542
    filename = eo_source.filename

    # is a source filename starts with the following, these two products can be made.
    if fnmatch(filename.lower(), 'c_gls_ndvi300*.nc'):
        name_elements = parse_copernicus_name(filename)
        date_str_YYYYMMDD = name_elements.datetime.date().strftime('%Y%m%d')
        return [
            product_output('S2_P02/NDVI_300',  # folder
                           f"{date_str_YYYYMMDD}_SE3_AFR_0300m_0010_NDVI.nc",  # filename
                           EOProductGroupChoices.a_agro_ndvi_300m_v2,  # group
                           'task_s02p02_c_gls_ndvi_300_clip',  # task_name
                           {
                               'aoi': [-30, 40, 60, -40]  # kwargs
                           })
        ]

    # 2md derivative product
    if fnmatch(filename.lower(), '????????_SE3_AFR_0300m_0010_NDVI.nc'.lower()):
        date_str_YYYYMMDD = filename.split('_')[0]
        print(filename)
        return [
            product_output('S2_P02/NDVI_1km',
                           f"{date_str_YYYYMMDD}_SE3_AFR_1000m_0010_NDVI.nc",
                           EOProductGroupChoices.a_agro_ndvi_1km_v3,
                           'task_s02p02_agro_nvdi_300_resample_to_1km',
                           task_kwargs={})
        ]

    if fnmatch(filename.lower(), 'c_gls_LAI300-RT1*.nc'):
        name_elements = parse_copernicus_name(filename)
        date_str_YYYYMMDD = name_elements.datetime.date().strftime('%Y%m%d')
        return [
            product_output('S2_P02/LAI_300', f"{date_str_YYYYMMDD}_SE3_AFR_0300m_0010_LAI.nc",
                           EOProductGroupChoices.a_agro_lai_300m_v1_afr,
                           'task_MISSING', {}),
            product_output('S2_P02/LAI_1km', f"{date_str_YYYYMMDD}_SE3_AFR_1000m_0010_LAI.nc",
                           EOProductGroupChoices.a_agro_lai_1km_v2_afr,
                           'task_MISSING', {}),
        ]

    if fnmatch(filename, 'c_gls_WB100*V1.0.1.nc'):
        name_elements = parse_copernicus_name(filename)
        YYYYMM = name_elements.datetime.date().strftime('%Y%m')
        return [
            product_output('S6_P01/WB_100/TUN', f"{YYYYMM}_SE2_TUN_0100m_0030_WBMA.nc", 'WB-100m-TUN',
                           'task_MISSING', {}),
            product_output('S6_P01/WB_100/RWA', f"{YYYYMM}_SE2_RWA_0100m_0030_WBMA.nc", 'WB-100m-RWA',
                           'task_MISSING', {}),
            product_output('S6_P01/WB_100/ETH', f"{YYYYMM}_SE2_ETH_0100m_0030_WBMA.nc", 'WB-100m-ETH',
                           'task_MISSING', {}),
            product_output('S6_P01/WB_100/MOZ', f"{YYYYMM}_SE2_MOZ_0100m_0030_WBMA.nc", 'WB-100m-MOZ',
                           'task_MISSING', {}),
            product_output('S6_P01/WB_100/ZAF', f"{YYYYMM}_SE2_ZAF_0100m_0030_WBMA.nc", 'WB-100m-ZAF',
                           'task_MISSING', {}),
            product_output('S6_P01/WB_100/GHA', f"{YYYYMM}_SE2_GHA_0100m_0030_WBMA.nc", 'WB-100m-GHA',
                           'task_MISSING', {}),
            product_output('S6_P01/WB_100/NER', f"{YYYYMM}_SE2_NER_0100m_0030_WBMA.nc", 'WB-100m-NER',
                           'task_MISSING', {}),
        ]

    return []


def generate_products_from_products():
    # As above, but for products that made from EOProducts (lvl2) instead from sources
    # TODO: cases
    raise NotImplementedError()


def file_is_valid(filepath: Path) -> bool:
    # TODO: check if file is valid

    return True


def check_netcdf(filepath: Path) -> bool:
    try:
        import xarray as xr
        dataset = xr.open_dataset(filepath)
        dataset.load()
    except:
        return False


def get_task_ref_from_name(token: str):
    """ get ref to runction/task by name. raises AttributeError exception if not found """
    from eo_engine import tasks

    return getattr(tasks, token)


def download_http_eosource(eosource: 'EOSource') -> str:
    import requests
    from eo_engine.models import EOSourceStatusChoices

    remote_url = eosource.url
    credentials = eosource.get_credentials

    response = requests.get(
        url=remote_url,
        auth=credentials,
        stream=True
    )
    response.raise_for_status()
    headers = response.headers
    FILE_LENGTH = headers.get('Content-Length', None)

    eosource.set_status(EOSourceStatusChoices.beingDownloaded)

    with TemporaryFile(mode='w+b') as file_handle:
        # TemporaryFile has noname, and will cease to exist when it is closed.

        for chunk in response.iter_content(chunk_size=2 * 1024):
            eosource.refresh_from_db()  # ping to keep db connection alive
            file_handle.write(chunk)
            file_handle.flush()

        content = File(file_handle)
        print(eosource.filename)
        from django.db import connections
        for conn in connections.all():
            conn.close_if_unusable_or_obsolete()
        eosource.refresh_from_db()
        eosource.file.save(name=eosource.filename, content=content, save=False)

    eosource.filesize = eosource.file.size
    eosource.status = EOSourceStatusChoices.availableLocally

    eosource.save()

    return eosource.file.name


def download_ftp_eosource(eosource: 'EOSource') -> str:
    from urllib.parse import urlparse
    from eo_engine.models import EOSourceStatusChoices
    import ftputil
    # instructions for lib at https://ftputil.sschwarzer.net/trac/wiki/Documentation
    url_parse = urlparse(eosource.url)
    server: str = url_parse.netloc
    ftp_path = url_parse.path
    user: str = eosource.credentials.username
    password: str = eosource.credentials.password

    def progress_cb(chunk: bytearray):
        pass

    with ftputil.FTPHost(server, user, password) as ftp_host:
        with NamedTemporaryFile() as file_handle:
            ftp_host.download(source=ftp_path, target=file_handle.name, callback=progress_cb)
            content = File(file_handle)
            from django.db import connections
            for conn in connections.all():
                conn.close_if_unusable_or_obsolete()
            eosource.refresh_from_db()
            eosource.file.save(name=eosource.filename, content=content, save=False)
        eosource.filesize = eosource.file.size
        eosource.set_status(EOSourceStatusChoices.availableLocally)

        eosource.save()

        return eosource.file.name
