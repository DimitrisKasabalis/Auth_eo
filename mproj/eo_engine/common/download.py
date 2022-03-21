import os
from copy import copy
from logging import Logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, TypedDict

from celery.utils.log import get_task_logger
from django.core.files import File
from django.core.files.temp import NamedTemporaryFile
from django.db import connections
from requests import HTTPError, Response
from sentinelsat import InvalidChecksumError

from eo_engine.errors import AfriCultuReSRetriableError, AfriCultuReSError
from eo_engine.models import EOSource, EOSourceStateChoices, EOSourceGroupChoices

logger: Logger = get_task_logger(__name__)


def download_http_eosource(pk_eosource: int) -> str:
    import requests

    eo_source = EOSource.objects.get(pk=pk_eosource)
    remote_url = eo_source.url

    class Manifest(TypedDict):
        primary_file: str
        auxiliary_files: List[str]

    manifest: Manifest = {
        'primary_file': remote_url,
        'auxiliary_files': []
    }
    if eo_source.group.filter(name=EOSourceGroupChoices.S04P01_LULC_500M_MCD12Q1_v6).exists():
        xml_file = eo_source.url + '.xml'
        logger.info(f'INFO: Adding auxiliary file {xml_file}')
        manifest['auxiliary_files'].append(xml_file)

    with requests.Session() as session:

        auth = None
        response = session.request('get', manifest['primary_file'], stream=True, auth=auth)
        try:
            response.raise_for_status()
        except HTTPError as e:
            err_response: Response = e.response

            if err_response.status_code == 401:
                auth = eo_source.get_credentials
                logger.info(f'LOG:INFO: HTTPError, retrying with authentication')
                logger.info(f'LOG:INFO: authentication: {auth}')
                response = session.request('get', err_response.url, auth=auth, stream=True)
            else:
                raise e
        response.raise_for_status()

        headers = response.headers
        FILE_LENGTH = headers.get('Content-Length', None)
        logger.info(f'LOG:INFO: File length is {FILE_LENGTH} bytes')

        eo_source.set_status(EOSourceStateChoices.DOWNLOADING)

        with NamedTemporaryFile() as file_handle:
            # TemporaryFile has noname, and will cease to exist when it is closed.

            bytes_processed = 0
            for chunk in response.iter_content(chunk_size=2 * 1024):
                bytes_processed += 2 * 1024

                file_handle.write(chunk)
                file_handle.flush()
            logger.info(f'LOG:INFO: Downloaded file {eo_source.filename} to a temporary file.')
            logger.info(f'LOG:INFO: Downloaded file has filesize {os.stat(file_handle.name).st_size}.')

            content = File(file_handle)
            for conn in connections.all():
                conn.close_if_unusable_or_obsolete()
            eo_source = EOSource.objects.get(pk=pk_eosource)
            eo_source.file.save(name=eo_source.filename, content=content, save=False)

            eo_source.filesize = eo_source.file.size
            eo_source.state = EOSourceStateChoices.AVAILABLE_LOCALLY
            eo_source.save()

        if len(manifest['auxiliary_files']) > 0:
            aux_file_url: str
            for aux_file_url in manifest['auxiliary_files']:
                dest_folder = Path(eo_source.file.path).parent
                filename = Path(aux_file_url).name
                destination_path = dest_folder / filename
                logger.info(f'LOG:INFO: Downloading auxiliary file: {filename}.')
                logger.info(f'LOG:INFO: downloading to  file: {destination_path.as_posix()}.')
                response = session.get(aux_file_url, stream=True)
                with destination_path.open('wb') as file_handle:
                    for chunk in response.iter_content():
                        file_handle.write(chunk)
                        file_handle.flush()

        return eo_source.file.name


def download_ftp_eosource(pk_eosource: int) -> str:
    from urllib.parse import urlparse
    import ftputil
    # instructions for common at https://ftputil.sschwarzer.net/trac/wiki/Documentation

    eo_source = EOSource.objects.get(pk=pk_eosource)
    url_parse = urlparse(eo_source.url)
    server: str = url_parse.netloc
    ftp_path = url_parse.path
    if eo_source.credentials:
        user: str = copy(eo_source.credentials.username)
        password: str = copy(eo_source.credentials.password)
    else:
        user: str = 'anonymous'
        password: str = 'anonymous@domain.com'

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
        eo_source.set_status(EOSourceStateChoices.AVAILABLE_LOCALLY)

        eo_source.save()

    return eo_source.file.name


def download_sftp_eosource(pk_eosource: int) -> str:
    from eo_engine.common.sftp import sftp_connection

    eo_source = EOSource.objects.get(pk=pk_eosource)
    credentials = eo_source.credentials
    connection = sftp_connection(host=eo_source.domain,
                                 username=credentials.username,
                                 password=credentials.password)

    remote_path = Path(eo_source.url).relative_to(f'sftp://{eo_source.domain}/').as_posix()
    with connection as c, NamedTemporaryFile() as temp_file:
        c.get('/' + remote_path, temp_file.name)
        content = File(temp_file)
        eo_source.file.save(name=eo_source.filename, content=content, save=False)
        eo_source.filesize = eo_source.file.size
        eo_source.set_status(EOSourceStateChoices.AVAILABLE_LOCALLY)

        eo_source.save()

    return eo_source.file.name


def download_wapor_eosource(pk_eosource: int) -> str:
    from eo_engine.models.factories import wapor_from_filename
    from eo_engine.common.contrib.waporv2 import WAPORv2Client, WAPORRemoteJob
    eo_source = EOSource.objects.get(pk=pk_eosource)
    with WAPORv2Client() as wp:
        # case I
        if eo_source.url == 'wapor://':
            remoteVariable = wapor_from_filename(eo_source.filename)
            remoteVariable.set_api_key(api_key=eo_source.credentials.api_key)
            remoteJob = remoteVariable.submit()
            eo_source.url = f'wapor://{remoteJob.job_id}'
            eo_source.save()

            raise AfriCultuReSRetriableError('Job Submitted. ')
        # case II
        uuid = eo_source.url.split(r'//')[1]
        remoteJob = WAPORRemoteJob.from_uuid(uuid)

        # too old uuid
        logger.info(f'remote job url: {remoteJob.job_url()}')
        logger.info(f'response_status:{remoteJob.response_status}')
        logger.info(f'job_status:{remoteJob.job_status}')
        if remoteJob.response_status == 404:
            eo_source.url = 'wapor://'
            eo_source.save()
            raise AfriCultuReSError('job was not found on remote server')
        elif remoteJob.response_status == 200 and remoteJob.job_status == 'RUNNING':
            raise AfriCultuReSRetriableError('Job is currently Running')
        elif remoteJob.response_status == 200 and remoteJob.job_status == 'WAITING':
            raise AfriCultuReSRetriableError('Job is currently Waiting')
        elif remoteJob.response_status == 200 and remoteJob.job_status == 'COMPLETED WITH ERRORS':
            error_log: List[str] = remoteJob.process_log()
            eo_source.url = 'wapor://'
            eo_source.save()
            error_log.insert(0, f'--WAPOR ERROR LOG--\n{remoteJob.job_url()}')
            raise AfriCultuReSError('\n'.join(error_log))
        elif remoteJob.response_status == 200 and remoteJob.job_status == 'COMPLETED':
            with NamedTemporaryFile() as file_handle:
                eo_source.set_status(EOSourceStateChoices.DOWNLOADING)
                eo_source.save()

                url = remoteJob.download_url()
                response = wp.get(url, stream=True)
                for chunk in response.iter_content(chunk_size=2 * 1024):
                    file_handle.write(chunk)
                    file_handle.flush()

                logger.info('LOG:INFO:File downloaded in a temp file.')
                content = File(file_handle)
                eosource = EOSource.objects.get(pk=pk_eosource)
                eosource.file.save(name=eosource.filename, content=content, save=False)

                eosource.filesize = eosource.file.size
                eosource.state = EOSourceStateChoices.AVAILABLE_LOCALLY
                eosource.save()

                return eo_source.filename
        else:
            eo_source.url = 'wapor://'
            eo_source.save()
            raise AfriCultuReSError(f'Unhandled case!!. Job_url: {remoteJob.job_url()}')


def download_sentinel_resource(pk_eosource: int) -> str:
    from sentinelsat import SentinelAPI
    eo_source = EOSource.objects.get(pk=pk_eosource)
    credentials = eo_source.credentials
    username = credentials.username
    password = credentials.password

    api = SentinelAPI(username, password, show_progressbars=False)
    # download the file to a temp dir, and then save it properly
    with TemporaryDirectory() as temp_dir:
        try:
            uuid = eo_source.url.split(r'//')[1]
            res = api.download(uuid, temp_dir)
        except InvalidChecksumError as e:
            logger.error('The download file failed the checksum')
            raise AfriCultuReSError('Failed to download') from e
        except BaseException as e:
            raise AfriCultuReSError(f'Unhandled case!!.')
        # close all the connections as they could be stale.
        connections.close_all()
        logger.info('LOG:INFO:File downloaded in a temp file.')
        file_handle = res["path"]
        content = File(open(file_handle, 'rb'))
        eosource = EOSource.objects.get(pk=pk_eosource)
        eosource.file.save(name=eosource.filename, content=content, save=False)

        eosource.filesize = eosource.file.size
        eosource.state = EOSourceStateChoices.AVAILABLE_LOCALLY
        eosource.save()

    return eo_source.filename
