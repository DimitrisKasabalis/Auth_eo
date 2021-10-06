from fnmatch import fnmatch
from typing import NamedTuple, Dict, Any, List, Union, Tuple, Optional

from eo_engine.common.copernicus import parse_copernicus_name
from eo_engine.models import EOProductGroupChoices, EOSourceGroupChoices

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


def filename_to_product(filename: str) \
        -> Optional[Union[typeProductGroup, Tuple[typeProductGroup, typeProductGroup]]]:
    filename = filename.lower()

    if fnmatch(filename, 'c_gls_ndvi300*.nc'.lower()):
        return EOProductGroupChoices.a_agro_ndvi_300m_v3

    if fnmatch(filename, '????????_SE3_AFR_0300m_0010_NDVI.nc'.lower()):
        return EOProductGroupChoices.a_agro_ndvi_1km_v3

    if fnmatch(filename, '????????_SE3_AFR_1000m_0010_NDVI.nc'.lower()):
        return EOProductGroupChoices.a_agro_vci_1km_v2_afr

    if fnmatch(filename, 'c_gls_LAI300-RT1*.nc'.lower()):
        return (EOProductGroupChoices.a_agro_lai_300m_v1_afr,
                EOProductGroupChoices.a_agro_lai_1km_v2_afr)

    if fnmatch(filename, 'hdf5_lsasaf_msg_dmet_msg-disk_????????????.bz2'.lower()):
        return EOSourceGroupChoices.MSG_3km_GLOB

    # not implemented yet
    if fnmatch(filename, 'c_gls_WB100*V1.0.1.nc'):
        raise NotImplementedError

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
                           EOProductGroupChoices.a_agro_ndvi_300m_v3,  # group
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
                           EOProductGroupChoices.a_agro_ndvi_1km_v3,
                           'task_s02p02_agro_nvdi_300_resample_to_1km',
                           task_kwargs={})
        ]

    # g2_BIOPAR_VCI_??????_AFRI_OLCI_V2.0.nc
    if fnmatch(filename.lower(), '????????_SE3_AFR_1000m_0010_NDVI.nc'.lower()):
        date_str_YYYYMMDD = filename.split('_')[0]
        return [
            product_output('S2_P02/VCI/v2',
                           f"g2_BIOPAR_VCI_{date_str_YYYYMMDD}_AFRI_OLCI_V2.0.nc",
                           EOProductGroupChoices.a_agro_vci_1km_v2_afr,
                           'task_s02p02_compute_vci', {}
                           )
        ]

    if fnmatch(filename.lower(), 'c_gls_LAI300-RT1*.nc'.lower()):
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

    # from here onwards we use the new logic:

    if product == EOSourceGroupChoices.MSG_3km_GLOB:
        output_filename_template = 'LSASAF_MSG_DMET_Africa_{YYYYMMDD}.nc'
        return [
            product_output(
                output_folder='S6_P04/ET_3km',
                filename=output_filename_template.format(
                    YYYYMMDD=(lambda: filename.removesuffix('.bz2').split('_')[-1][:-4])()
                ),
                group=EOProductGroupChoices.MSG_3km_AFR,
                task_name='task_s06p04_et_3km',
                task_kwargs={}
            )
        ]

    return []
