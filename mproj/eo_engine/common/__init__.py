from datetime import datetime

from typing import NamedTuple

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
