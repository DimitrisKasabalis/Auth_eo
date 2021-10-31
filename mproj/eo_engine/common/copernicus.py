from datetime import datetime
from typing import NamedTuple

copernicus_name_elements = NamedTuple(
    'copernicus_name_elements',
    [('product', str), ('datetime', datetime), ('area', str),
     ('sensor', str), ('version', str)]
)
