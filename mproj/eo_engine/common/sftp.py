from datetime import datetime
from pathlib import Path
from typing import NamedTuple
from tempfile import NamedTemporaryFile
import pysftp

from eo_engine.common.parsers import parse_dt_from_generic_string

SftpFile = NamedTuple('SftpFile', (
    ('domain', str),
    ('url', str),
    ('filename', str),
    ('filesize_reported', int),
    ('datetime_reference', datetime)
)
                      )


def sftp_connection(host, username, password) -> pysftp.Connection:
    cnopts = pysftp.CnOpts()

    # disable check against known hosts
    cnopts.hostkeys = None
    c = pysftp.Connection(
        host=host,
        username=username,
        password=password,
        cnopts=cnopts
    )

    return c


def list_dir_entries(remotepath: str, connection: pysftp.Connection):
    domain = connection.sftp_client.get_channel().get_transport().hostname
    url_template = '{schema}://{host}{path}'
    with connection as c:
        for entry in c.listdir_attr(remotepath):
            filename = entry.filename
            path = Path(remotepath) / filename
            yield SftpFile(
                domain=domain,
                filename=filename,
                filesize_reported=entry.st_size,
                datetime_reference=parse_dt_from_generic_string(filename),
                url=url_template.format(schema='sftp',host=domain,path=path)
            )
