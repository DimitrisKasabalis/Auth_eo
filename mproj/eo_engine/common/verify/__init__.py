from logging import Logger

from pathlib import Path
from celery.utils.log import get_task_logger

from eo_engine.errors import AfriCultuReSFileDoesNotExist

logger: Logger = get_task_logger(__name__)


def check_file_exists(file_path: Path) -> None:
    """ Checks if path exists. If file throws error """
    logger.info(f'Checking for {file_path.name}.')
    if not file_path.exists() and not file_path.is_file():
        msg = f'The file +{file_path.name}+ at {file_path.as_posix()} was not found!.'
        raise AfriCultuReSFileDoesNotExist(msg)
    logger.info(f' {file_path.name} found at {file_path.as_posix()}.')
