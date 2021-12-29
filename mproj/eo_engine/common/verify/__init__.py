from pathlib import Path

from eo_engine.errors import AfriCultuReSFileDoesNotExist


def check_file_exists(file_path: Path) -> None:
    """ Checks if path exists. If not throws error """
    if not file_path.exists():
        msg = f'The file +{file_path.name}+ at {file_path.as_posix()} was not found!.'
        raise AfriCultuReSFileDoesNotExist(msg)
