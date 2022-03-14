from datetime import datetime
from functools import lru_cache
from logging import Logger
from pathlib import Path
import paramiko
from paramiko import AutoAddPolicy
from celery.utils.log import get_task_logger

from eo_engine.errors import AfriCultuReSFileDoesNotExist
from eo_engine.models import EOProduct, Pipeline
from typing import NamedTuple, List, Union, Tuple
from django.conf import settings

logger: Logger = get_task_logger(__name__)


@lru_cache
def get_file_by_name_from_aux_folder(file_path: Union[str, Path]) -> Path:
    """ Checks if path exists. If file throws error """
    file_path = Path(file_path)
    file_path = Path('/aux_files/') / file_path
    logger.info(f'Checking for {file_path.name} in {file_path.parent.name}.')

    if not file_path.exists() and not file_path.is_file():
        msg = f'The file +{file_path.name}+ at {file_path.as_posix()} was not found!.'
        raise AfriCultuReSFileDoesNotExist(msg)
    logger.info(f' {file_path.name} found at {file_path.as_posix()}.')
    return file_path


def draxis_upload_file(eo_product: EOProduct) -> str:
    rsa_key = get_file_by_name_from_aux_folder('/keys/id.rsa')
    domain = '18.159.85.240:2310'
    username = 'user1'
    localpath = Path(eo_product.file.path)
    remotepath = Path('/sftp/') / localpath.relative_to(settings.PRODUCTS_ROOT)

    with paramiko.SSHClient() as ssh_client:
        ssh_client.set_missing_host_key_policy(AutoAddPolicy)
        ssh_client.connect(
            hostname=domain.split(':')[0],
            port=int(domain.split(':')[1]),
            username=username,
            key_filename=rsa_key.as_posix(),
            look_for_keys=False,
        )
        with ssh_client.open_sftp() as sftp_client:
            logger.info(f'will put: {localpath.as_posix()}-> {remotepath.as_posix()}')
            # sftp_attributes: paramiko.SFTPAttributes = sftp_client.put(
            #     localpath=localpath.as_posix(),
            #     remotepath=remotepath.as_posix(),
            #     confirm=True
            # )

    return remotepath.as_posix()


def draxis_payload_generator(eo_product: EOProduct) -> dict:
    pipeline: Pipeline = eo_product.group.pipelines_from_output.first()

    # eg: S1
    service: str = pipeline.service
    # eg: P02
    product: str = pipeline.product
    # eg: "Normalized Difference Vegetation Index 1KM"
    indicator: str = eo_product.indicator
    # eg. Datetime iso format
    #  product reference date
    datetime_str: str = eo_product.reference_date_iso_str
    # https://africultures-thredds.draxis.gr/thredds/fileServer/services/ + path
    # "https://africultures-thredds.draxis.gr/thredds/fileServer/services/S2_P02/NDVI_1km/c_gls_NDVI_201912210000_GLOBE_PROBAV_V2.2.1.nc_subset.nc"
    url: str = f'https://africultures-thredds.draxis.gr/thredds/fileServer/services/{Path(eo_product.file.path).relative_to(settings.PRODUCTS_ROOT).as_posix()}'
    # to be determined
    variable: Tuple[list[Union[str, int]]] = ["1", 2, 3],
    # T/F
    incremental: bool = True
    return {
        "service": service,
        "product": product,
        "indicator": indicator,
        "datetime": datetime_str,
        "url": url,
        "variable": variable,
        "incremental": incremental
    }


RemoteFile = NamedTuple('RemoteFile', (
    ('domain', str),
    ('url', str),
    ('filename', str),
    ('filesize_reported', int)
))
CopernicusNameElements = NamedTuple(
    'CopernicusNameElements',
    [('product', str), ('datetime', datetime), ('area', str),
     ('sensor', str), ('version', str)]
)
