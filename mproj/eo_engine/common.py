from pathlib import Path
from tempfile import mkstemp, TemporaryFile

import requests
from django.core.files.base import ContentFile, File

from eo_engine.models import EOSource, EOSourceStatusChoices


def generate_prod_filename(eo_source: EOSource) -> str:
    """ MOAR """
    product_group = eo_source.product_group
    product_date_from_name = eo_source.filename.split('_')[3]
    return f'CGLS_{product_group}_{product_date_from_name}_1KM_Resampled_Africa.nc'


def file_is_valid(response, filepath) -> bool:
    return True


def download_asset(eo_source: EOSource) -> EOSource:
    remote_url = eo_source.url
    credentials = eo_source.get_credentials

    # eo_source.local_path.parent.mkdir(exist_ok=True, parents=True)
    # print(f'Dest file {eo_source.local_path.as_posix()}')
    # temp_name = random_name_gen()

    response = requests.get(
        url=remote_url,
        auth=credentials,
        stream=True
    )
    response.raise_for_status()

    eo_source.status = EOSourceStatusChoices.beingDownloaded
    eo_source.save()

    with TemporaryFile(mode='w+b') as file_handle:
        for chunk in response.iter_content(chunk_size=2 * 1024):
            file_handle.write(chunk)
            file_handle.flush()
        # file_handle.seek(0)

        if file_is_valid(response, file_handle):
            pass

        content = File(file_handle)
        print(eo_source.filename)
        print(content)
        eo_source.file.save(name=eo_source.filename, content=content)
        eo_source.filesize = eo_source.file.size
        eo_source.status = EOSourceStatusChoices.availableLocally
        eo_source.save()

    return eo_source
