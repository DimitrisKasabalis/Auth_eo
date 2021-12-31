import pysftp
from pathlib import Path

from eo_engine.common import RemoteFile


def sftp_connection(host, username, password) -> pysftp.Connection:
    cnopts = pysftp.CnOpts()

    # disable check against known hosts
    cnopts.hostkeys = None
    c = pysftp.Connection(
        host=host,
        username=username,
        password=password,
        cnopts=cnopts)

    return c


def list_dir_entries(remotepath: str, connection: pysftp.Connection):
    domain = connection.sftp_client.get_channel().get_transport().hostname
    url_template = '{schema}://{host}{path}'
    with connection as c:
        for entry in c.listdir_attr(remotepath):
            filename = entry.filename
            path = Path(remotepath) / filename
            yield RemoteFile(
                domain=domain,
                filename=filename,
                filesize_reported=entry.st_size,
                url=url_template.format(schema='sftp', host=domain, path=path)
            )
