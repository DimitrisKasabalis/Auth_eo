import re
from fnmatch import fnmatch
from typing import NamedTuple, Dict, Any, List, Union, Tuple, Optional

from eo_engine.common.parsers import parse_copernicus_name
from eo_engine.models import EOProductGroupChoices, EOSourceGroupChoices
from eo_engine import tasks as eo_tasks
from celery.utils.log import get_task_logger

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

    if fnmatch(filename, 'c_gls_LAI300-RT1*.nc'.lower()):
        return [EOProductGroupChoices.AGRO_LAI_300M_V1_AFR,
                EOProductGroupChoices.AGRO_LAI_1KM_V2_AFR]

    if fnmatch(filename, 'hdf5_lsasaf_msg_dmet_msg-disk_????????????.bz2'.lower()):
        return [EOProductGroupChoices.MSG_3KM_AFR, ]

    # not implemented yet
    if fnmatch(filename, 'c_gls_WB300_????????0000_GLOBE_S2_V2.0.1.nc'.lower()):
        return [EOProductGroupChoices.AGRO_WB_300M_V2_AFR, ]

    if fnmatch(filename, r'RIVER-FLDglobal-composite1_????????_*.part???.tif'):
        return [EOProductGroupChoices.VIIRS_1DAY_AFR, ]

    logger.warn(f'No rule for +{filename}+')
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

    if fnmatch(filename.lower(), 'c_gls_LAI300-RT1*.nc'.lower()):
        name_elements = parse_copernicus_name(filename)
        date_str_YYYYMMDD = name_elements.datetime.date().strftime('%Y%m%d')
        return [
            product_output('S2_P02/LAI_300', f"{date_str_YYYYMMDD}_SE3_AFR_0300m_0010_LAI.nc",
                           EOProductGroupChoices.AGRO_LAI_300M_V1_AFR,
                           'task_MISSING', {}),
            product_output('S2_P02/LAI_1km', f"{date_str_YYYYMMDD}_SE3_AFR_1000m_0010_LAI.nc",
                           EOProductGroupChoices.AGRO_LAI_1KM_V2_AFR,
                           'task_MISSING', {}),
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
                           'task_s02p02_compute_vci', {}
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
