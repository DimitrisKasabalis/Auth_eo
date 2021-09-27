from copy import copy
from tempfile import TemporaryFile, NamedTemporaryFile

from django.core.files import File

from eo_engine.models import EOSource


def download_http_eosource(pk_eosource: int) -> str:
    import requests
    from eo_engine.models import EOSourceStateChoices

    eo_source = EOSource.objects.get(pk=pk_eosource)
    remote_url = eo_source.url
    credentials = eo_source.get_credentials

    response = requests.get(
        url=remote_url,
        auth=credentials,
        stream=True
    )
    response.raise_for_status()
    headers = response.headers
    FILE_LENGTH = headers.get('Content-Length', None)

    eo_source.set_status(EOSourceStateChoices.BeingDownloaded)

    with TemporaryFile(mode='w+b') as file_handle:
        # TemporaryFile has noname, and will cease to exist when it is closed.

        for chunk in response.iter_content(chunk_size=2 * 1024):
            # eosource.refresh_from_db()  # ping to keep db connection alive
            file_handle.write(chunk)
            file_handle.flush()

        content = File(file_handle)

        # from django.db import connections
        # for conn in connections.all():
        #     conn.close_if_unusable_or_obsolete()
        # eosource.refresh_from_db()
        eosource = EOSource.objects.get(pk=pk_eosource)
        eosource.file.save(name=eosource.filename, content=content, save=False)

        eosource.filesize = eosource.file.size
        eosource.state = EOSourceStateChoices.AvailableLocally
        eosource.save()

    return eosource.file.name


def download_ftp_eosource(pk_eosource: int) -> str:
    from urllib.parse import urlparse
    from eo_engine.models import EOSourceStateChoices
    import ftputil
    # instructions for common at https://ftputil.sschwarzer.net/trac/wiki/Documentation

    eo_source = EOSource.objects.get(pk=pk_eosource)
    url_parse = urlparse(eo_source.url)
    server: str = url_parse.netloc
    ftp_path = url_parse.path
    user: str = copy(eo_source.credentials.username)
    password: str = copy(eo_source.credentials.password)

    def progress_cb(chunk: bytearray):
        # This function is called with a byte chunk.
        pass

    with ftputil.FTPHost(server, user, password) as ftp_host, \
            NamedTemporaryFile() as file_handle:
        ftp_host.download(source=ftp_path, target=file_handle.name, callback=progress_cb)
        content = File(file_handle)
        # recreate reference to db to refresh the database connection
        eo_source = EOSource.objects.get(pk=pk_eosource)
        eo_source.file.save(name=eo_source.filename, content=content, save=False)
        eo_source.filesize = eo_source.file.size
        eo_source.set_status(EOSourceStateChoices.AvailableLocally)

        eo_source.save()

    return eo_source.file.name
