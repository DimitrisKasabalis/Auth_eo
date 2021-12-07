import re
from dateutil.relativedelta import relativedelta
from datetime import date as dt_date
from fnmatch import fnmatch
from typing import NamedTuple, Dict, Any, List, Union, Optional

from celery.utils.log import get_task_logger

from eo_engine import tasks as eo_tasks
from eo_engine.common.parsers import parse_copernicus_name, parse_dt_from_generic_string
from eo_engine.common.patterns import GMOD09Q1_DATE_PATTERN, RIVER_FLD_GLOBAL, GMOD09Q1_FILE_REGEX
from eo_engine.models import EOProductGroupChoices, EOSourceGroupChoices

logger = get_task_logger('eo_engine.products')

product_output = NamedTuple('product_output',
                            [('output_folder', str),
                             ('filename', str),
                             ('group', str),
                             ('task_name', str),
                             ('task_kwargs', Dict[str, Any])
                             ])

typeProductGroup = Union[EOProductGroupChoices, EOSourceGroupChoices]


def filename_to_product(filename: str) -> Optional[List[typeProductGroup]]:
    """ Check which products should come up form this filename. """
    filename = filename.lower()

    if fnmatch(filename, 'c_gls_ndvi300*.nc'.lower()):
        return [EOProductGroupChoices.AGRO_NDVI_300M_V3_AFR, ]

    if fnmatch(filename, '????????_SE3_AFR_0300m_0010_NDVI.nc'.lower()):
        return [EOProductGroupChoices.AGRO_NDVI_1KM_V3_AFR, ]

    if fnmatch(filename, '????????_SE3_AFR_1000m_0010_NDVI.nc'.lower()):
        return [EOProductGroupChoices.AGRO_VCI_1KM_V2_AFR, ]

    if fnmatch(filename, 'c_gls_LAI300-RT[2,6]_????????0000_GLOBE_OLCI_V1.1.[1,2].nc'.lower()):
        return [EOProductGroupChoices.AGRO_LAI_300M_V1_AFR, ]

    if fnmatch(filename, 'hdf5_lsasaf_msg_dmet_msg-disk_????????????.bz2'.lower()):
        return [EOProductGroupChoices.MSG_3KM_AFR, ]

    # not implemented yet
    if fnmatch(filename, 'c_gls_WB300_????????0000_GLOBE_S2_V2.0.1.nc'.lower()):
        return [EOProductGroupChoices.AGRO_WB_300M_V2_AFR, ]

    if fnmatch(filename, r'RIVER-FLDglobal-composite1_????????_*.part???.tif'.lower()):
        return [EOProductGroupChoices.VIIRS_1DAY_AFR, ]

    if fnmatch(filename, r'GMOD09Q1.A???????.08d.latlon.x??y??.6v1.NDVI_anom_S2001-2015.tif.gz'.lower()):
        pat = GMOD09Q1_FILE_REGEX.match(filename)
        if pat is None:
            logger.err(f'Unrecognized pattern match for {filename}')
            raise Exception(f'Unrecognized pattern match for {filename}')
        tile = pat.groupdict()['tile']
        tiles_zaf = ["x21y13", "x22y12", "x22y13", "x23y12", "x23y13"]
        tiles_moz = ["x23y11", "x23y12", "x24y11", "x24y12"]
        tiles_tun = ["x20y05", "x20y06", "x21y05", "x21y06"]
        tiles_ken = ["x23y09", "x23y10", "x24y09", "x24y10"]
        tiles_gha = ["x23y08", "x23y09", "x24y08", "x24y09", "x25y09"]
        tiles_rwa = ["x23y10"]
        tiles_eth = ["x23y08", "x23y09", "x24y08", "x24y09", "x25y09"]
        tiles_ner = ["x19y08", "x19y09", "x20y07", "x20y08", "x20y09", "x21y07", "x21y08"]
        if tile in tiles_zaf:
            return [EOProductGroupChoices.AGRO_NDVIA_ZAF, ]
        elif tile in tiles_moz:
            return [EOProductGroupChoices.AGRO_NDVIA_MOZ, ]
        elif tile in tiles_tun:
            return [EOProductGroupChoices.AGRO_NDVIA_TUN, ]
        elif tile in tiles_ken:
            return [EOProductGroupChoices.AGRO_NDVIA_KEN, ]
        elif tile in tiles_gha:
            return [EOProductGroupChoices.AGRO_NDVIA_GHA, ]
        elif tile in tiles_rwa:
            return [EOProductGroupChoices.AGRO_NDVIA_RWA, ]
        elif tile in tiles_eth:
            return [EOProductGroupChoices.AGRO_NDVIA_ETH, ]
        elif tile in tiles_ner:
            return [EOProductGroupChoices.AGRO_NDVIA_NER, ]

    logger.warn(f'No rule for derivative products for +{filename}+')
    return None


# TODO: HIGH PRIORITY: To move this logic in database
# TODO: Add more logic as it arrives
def generate_products_from_source(filename: str) -> List[product_output]:
    """  """
    # check https://docs.google.com/spreadsheets/d/1C59BF349gMW-HxnEoWT1Pnxj0tuHOwX7/edit#gid=1748752542
    product = filename_to_product(filename)

    # NDVI 100m -> {date_str_YYYYMMDD}_SE3_AFR_0300m_0010_NDVI.nc
    if fnmatch(filename.lower(), 'c_gls_ndvi300*.nc'.lower()):
        name_elements = parse_copernicus_name(filename)
        date_str_YYYYMMDD = name_elements.datetime.date().strftime('%Y%m%d')
        return [
            product_output('S2_P02/NDVI_300',  # folder
                           f"{date_str_YYYYMMDD}_SE3_AFR_0300m_0010_NDVI.nc",  # filename
                           EOProductGroupChoices.AGRO_NDVI_300M_V3_AFR,  # group
                           'task_s02p02_c_gls_ndvi_300_clip',  # task_name
                           {
                               'aoi': [-30, 40, 60, -40]  # kwargs
                           })
        ]

    # NDVI 1000m ->  {date_str_YYYYMMDD}_SE3_AFR_1000m_0010_NDVI.nc
    if fnmatch(filename.lower(), '????????_SE3_AFR_0300m_0010_NDVI.nc'.lower()):
        date_str_YYYYMMDD = filename.split('_')[0]
        return [
            product_output('S2_P02/NDVI_1km',
                           f"{date_str_YYYYMMDD}_SE3_AFR_1000m_0010_NDVI.nc",
                           EOProductGroupChoices.AGRO_NDVI_1KM_V3_AFR,
                           'task_s02p02_agro_nvdi_300_resample_to_1km',
                           task_kwargs={})
        ]

    # NDVI Anomaly
    # eg filename: GMOD09Q1.A2020001.08d.latlon.x23y10.6v1.NDVI_anom_S2001-2015.tif.gz
    if product and product.count(EOProductGroupChoices.AGRO_NDVIA_ZAF):
        iso = 'ZAF'
        match = GMOD09Q1_DATE_PATTERN.match(filename)
        group_dict = match.groupdict()
        year = int(group_dict['year'])
        doy = int(group_dict['doy'])
        date = dt_date(year, 1, 1) + relativedelta(days=doy - 1)
        return [
            product_output(
                output_folder='S2_P02/NDVI_anom',
                filename='{YYYYMMDD}_MOD_{ISOCODE}_0250m_0008_NDVI_anom.nc'.format(
                    ISOCODE=iso,
                    YYYYMMDD=date.strftime('%Y%m%d')),
                group=EOProductGroupChoices.AGRO_NDVIA_ZAF,
                task_name='task_s02p02_process_ndvia',
                task_kwargs={'iso': iso}
            )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_NDVIA_MOZ):
        iso = 'MOZ'
        match = GMOD09Q1_DATE_PATTERN.match(filename)
        group_dict = match.groupdict()
        year = int(group_dict['year'])
        doy = int(group_dict['doy'])
        date = dt_date(year, 1, 1) + relativedelta(days=doy - 1)
        return [
            product_output(
                output_folder='S2_P02/NDVI_anom',
                filename='{YYYYMMDD}_MOD_{ISOCODE}_0250m_0008_NDVI_anom.nc'.format(
                    ISOCODE=iso,
                    YYYYMMDD=date.strftime('%Y%m%d')),
                group=EOProductGroupChoices.AGRO_NDVIA_MOZ,
                task_name='task_s02p02_process_ndvia',
                task_kwargs={'iso': iso}
            )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_NDVIA_TUN):
        iso = 'TUN'
        match = GMOD09Q1_DATE_PATTERN.match(filename)
        group_dict = match.groupdict()
        year = int(group_dict['year'])
        doy = int(group_dict['doy'])
        date = dt_date(year, 1, 1) + relativedelta(days=doy - 1)
        return [
            product_output(
                output_folder='S2_P02/NDVI_anom',
                filename='{YYYYMMDD}_MOD_{ISOCODE}_0250m_0008_NDVI_anom.nc'.format(
                    ISOCODE=iso,
                    YYYYMMDD=date.strftime('%Y%m%d')),
                group=EOProductGroupChoices.AGRO_NDVIA_TUN,
                task_name='task_s02p02_process_ndvia',
                task_kwargs={'iso': iso}
            )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_NDVIA_KEN):
        iso = 'KEN'
        match = GMOD09Q1_DATE_PATTERN.match(filename)
        group_dict = match.groupdict()
        year = int(group_dict['year'])
        doy = int(group_dict['doy'])
        date = dt_date(year, 1, 1) + relativedelta(days=doy - 1)
        return [
            product_output(
                output_folder='S2_P02/NDVI_anom',
                filename='{YYYYMMDD}_MOD_{ISOCODE}_0250m_0008_NDVI_anom.nc'.format(
                    ISOCODE=iso,
                    YYYYMMDD=date.strftime('%Y%m%d')),
                group=EOProductGroupChoices.AGRO_NDVIA_KEN,
                task_name='task_s02p02_process_ndvia',
                task_kwargs={'iso': iso}
            )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_NDVIA_GHA):
        iso = 'GHA'
        match = GMOD09Q1_DATE_PATTERN.match(filename)
        group_dict = match.groupdict()
        year = int(group_dict['year'])
        doy = int(group_dict['doy'])
        date = dt_date(year, 1, 1) + relativedelta(days=doy - 1)
        return [
            product_output(
                output_folder='S2_P02/NDVI_anom',
                filename='{YYYYMMDD}_MOD_{ISOCODE}_0250m_0008_NDVI_anom.nc'.format(
                    ISOCODE=iso,
                    YYYYMMDD=date.strftime('%Y%m%d')),
                group=EOProductGroupChoices.AGRO_NDVIA_GHA,
                task_name='task_s02p02_process_ndvia',
                task_kwargs={'iso': iso}
            )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_NDVIA_RWA):
        iso = 'RWA'
        match = GMOD09Q1_DATE_PATTERN.match(filename)
        group_dict = match.groupdict()
        year = int(group_dict['year'])
        doy = int(group_dict['doy'])
        date = dt_date(year, 1, 1) + relativedelta(days=doy - 1)
        return [
            product_output(
                output_folder='S2_P02/NDVI_anom',
                filename='{YYYYMMDD}_MOD_{ISOCODE}_0250m_0008_NDVI_anom.nc'.format(
                    ISOCODE=iso,
                    YYYYMMDD=date.strftime('%Y%m%d')),
                group=EOProductGroupChoices.AGRO_NDVIA_RWA,
                task_name='task_s02p02_process_ndvia',
                task_kwargs={'iso': iso}
            )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_NDVIA_ETH):
        iso = 'ETH'
        match = GMOD09Q1_DATE_PATTERN.match(filename)
        group_dict = match.groupdict()
        year = int(group_dict['year'])
        doy = int(group_dict['doy'])
        date = dt_date(year, 1, 1) + relativedelta(days=doy - 1)
        return [
            product_output(
                output_folder='S2_P02/NDVI_anom',
                filename='{YYYYMMDD}_MOD_{ISOCODE}_0250m_0008_NDVI_anom.nc'.format(
                    ISOCODE=iso,
                    YYYYMMDD=date.strftime('%Y%m%d')),
                group=EOProductGroupChoices.AGRO_NDVIA_ETH,
                task_name='task_s02p02_process_ndvia',
                task_kwargs={'iso': iso}
            )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_NDVIA_NER):
        iso = 'NER'
        match = GMOD09Q1_DATE_PATTERN.match(filename)
        group_dict = match.groupdict()
        year = int(group_dict['year'])
        doy = int(group_dict['doy'])
        date = dt_date(year, 1, 1) + relativedelta(days=doy - 1)
        return [
            product_output(
                output_folder='S2_P02/NDVI_anom',
                filename='{YYYYMMDD}_MOD_{ISOCODE}_0250m_0008_NDVI_anom.nc'.format(
                    ISOCODE=iso,
                    YYYYMMDD=date.strftime('%Y%m%d')),
                group=EOProductGroupChoices.AGRO_NDVIA_NER,
                task_name='task_s02p02_process_ndvia',
                task_kwargs={'iso': iso}
            )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_LAI_300M_V1_AFR):
        name_elements = parse_dt_from_generic_string(filename)
        date_str_YYYYMMDD = name_elements.strftime('%Y%m%d')
        return [
            product_output('S2_P02/LAI_300',
                           f"{date_str_YYYYMMDD}_SE3_AFR_0300m_0010_LAI.nc",
                           EOProductGroupChoices.AGRO_LAI_300M_V1_AFR,
                           'task_s0p02_clip_lai300m_v1_afr', {}
                           ),
        ]

    if fnmatch(filename, 'c_gls_WB100*V1.0.1.nc'):
        name_elements = parse_copernicus_name(filename)
        YYYYMMDD = name_elements.datetime.date().strftime('%Y%m%d')

        return [
            product_output(
                'S6_P01/WB_100/TUN',
                f"{YYYYMMDD}_SE2_TUN_0100m_0030_WBMA.nc",
                EOProductGroupChoices.AGRO_WB_100M_TUN.value,
                eo_tasks.task_s0601_wb_100m.name,
                {
                    'wkt': "POLYGON((7.491 37.345,  11.583 37.345,  11.583 30.219, 7.491 30.219, 7.491 37.345, 7.491 37.345))",
                    'iso': 'TUN'
                }),
            product_output(
                'S6_P01/WB_100/RWA',
                f"{YYYYMMDD}_SE2_RWA_0100m_0030_WBMA.nc",
                EOProductGroupChoices.AGRO_WB_100M_RWA.value,
                eo_tasks.task_s0601_wb_100m.name,
                {
                    'wkt': "POLYGON((28.845 -1.052, 30.894 -1.052, 30.894 -2.827, 28.845 -2.827, 28.845 -1.052, 28.845 -1.052))",
                    'iso': 'RWA'
                }),
            product_output(
                'S6_P01/WB_100/KEN',
                f"{YYYYMMDD}_SE2_KEN_0100m_0030_WBMA.nc",
                EOProductGroupChoices.AGRO_WB_100M_KEN.value,
                eo_tasks.task_s0601_wb_100m.name,
                {
                    'wkt': "POLYGON((33.89 4.625, 41.917 4.625, 41.917 -4.671, 33.89 -4.671, 33.89 4.625, 33.89 4.625))",
                    'iso': 'KEN'
                }),
            product_output(
                'S6_P01/WB_100/ETH',
                f"{YYYYMMDD}_SE2_ETH_0100m_0030_WBMA.nc",
                EOProductGroupChoices.AGRO_WB_100M_ETH.value,
                eo_tasks.task_s0601_wb_100m.name,
                {
                    'wkt':
                        "POLYGON((32.98 14.93, 48.0 14.93, 48.0 3.37, 32.98 3.37, 32.98 14.93, 32.98 14.93))",
                    'iso': 'ETH'
                }),
            product_output(
                'S6_P01/WB_100/MOZ',
                f"{YYYYMMDD}_SE2_MOZ_0100m_0030_WBMA.nc",
                EOProductGroupChoices.AGRO_WB_100M_MOZ.value,
                eo_tasks.task_s0601_wb_100m.name,
                {
                    'wkt':
                        "POLYGON((30.208 -10.467, 40.849 -10.467, 40.849 -26.865, 30.208 -26.865, 30.208 -10.467, 30.208 -10.467))",
                    'iso': 'MOZ'
                }),
            product_output(
                'S6_P01/WB_100/ZAF',
                f"{YYYYMMDD}_SE2_ZAF_0100m_0030_WBMA.nc",
                EOProductGroupChoices.AGRO_WB_100M_ZAF.value,
                eo_tasks.task_s0601_wb_100m.name,
                {
                    'wkt':
                        "POLYGON((16 -22, 33 -22, 33 -35, 16 -35, 16 -22, 16 -22))",
                    'iso': 'ZAF'
                }),
            product_output(
                'S6_P01/WB_100/GHA',
                f"{YYYYMMDD}_SE2_GHA_0100m_0030_WBMA.nc",
                EOProductGroupChoices.AGRO_WB_100M_GHA.value,
                eo_tasks.task_s0601_wb_100m.name,
                {
                    'wkt':
                        "POLYGON((-3.25 12.16, 2.205 12.16, 2.205 4.7, -3.25 4.7, -3.25 12.16, -3.25 12.16))",
                    'iso': 'GHA'
                }),
            product_output(
                'S6_P01/WB_100/NER',
                f"{YYYYMMDD}_SE2_NER_0100m_0030_WBMA.nc",
                EOProductGroupChoices.AGRO_WB_100M_NER.value,
                eo_tasks.task_s0601_wb_100m.name,
                {
                    'wkt':
                        "POLYGON((0 23.8, 16.1 23.8, 16.1 11.5, 0 11.5, 0 23.8, 0 23.8))",
                    'iso': 'NER'
                }),

        ]

    # from here onwards I use a different logic:
    # 'if product.count' -> product is a list, and the subroutine checks in the list contains the parameter in it

    # g2_BIOPAR_VCI_??????_AFRI_OLCI_V2.0.nc
    if product and product.count(EOProductGroupChoices.AGRO_VCI_1KM_V2_AFR):
        date_str_YYYYMMDD = filename.split('_')[0]
        return [
            product_output('S2_P02/VCI/v2',
                           f"g2_BIOPAR_VCI_{date_str_YYYYMMDD}_AFRI_OLCI_V2.0.nc",
                           EOProductGroupChoices.AGRO_VCI_1KM_V2_AFR,
                           'task_s02p02_cgls_compute_vci_1km_v2', {}
                           )
        ]

    if product and product.count(EOProductGroupChoices.AGRO_WB_300M_V2_AFR):
        output_filenane_template = '{YYYYMMDD}_SE2_AFR_0300m_0030_WBMA.nc'
        YYYYMMDD = filename.split('_')[3][:8]
        return [
            product_output(
                output_folder='S6_P01/WB_300/v2',
                filename=output_filenane_template.format(YYYYMMDD=YYYYMMDD),
                group=EOProductGroupChoices.AGRO_WB_300M_V2_AFR,
                task_name=eo_tasks.task_s06p01_clip_to_africa.name,
                task_kwargs={}
            )

        ]

    if product and product.count(EOProductGroupChoices.MSG_3KM_AFR):
        output_filename_template = 'LSASAF_MSG_DMET_Africa_{YYYYMMDD}.nc'
        return [
            product_output(
                output_folder='S6_P04/ET_3km',
                filename=output_filename_template.format(
                    YYYYMMDD=(lambda: filename.removesuffix('.bz2').split('_')[-1][:-4])()
                ),
                group=EOProductGroupChoices.MSG_3KM_AFR,
                task_name='task_s06p04_et_3km',
                task_kwargs={}
            )
        ]

    if product and product.count(EOProductGroupChoices.VIIRS_1DAY_AFR):
        pat = re.compile(r'RIVER-FLDglobal-composite1_(?P<YYYYMMDD>[0-9]{1,8})_000000.part(?P<tile>[0-9]{1,3}).tif')
        match = pat.match(filename)
        match_groups = match.groupdict()
        return [
            product_output(
                output_folder='S4_P03/Floods_MR',
                filename='{YYYYMMDD}_VIIRS_{tile}_0375m_0001_FAMA.nc'.format(**match_groups),
                group=EOProductGroupChoices.VIIRS_1DAY_AFR,
                task_name='task_s04p03_convert_to_tiff',
                task_kwargs={
                    'tile': match_groups['tile']
                }
            )
        ]
    return []
