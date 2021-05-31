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
