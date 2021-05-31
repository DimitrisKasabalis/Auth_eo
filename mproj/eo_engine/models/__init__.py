from pathlib import Path

from django.conf import settings
from .c import *
from .eo_product import EOProductsChoices, EOProductStatusChoices, EOProduct
from .eo_source import EOSourceStatusChoices, EOSource, EOSourceProductChoices, EOSourceProductGroupChoices
from .other import Credentials

FILE_ROOT = Path(settings.FILE_ROOT)

__all__ = [
    "Credentials",
    "EOProduct",
    "EOSource",
    "EOSourceStatusChoices",
    ""
    "EOSourceProductChoices",
    "EOSourceProductGroupChoices",
    "EOProductsChoices"
]
