import datetime
import re
from datetime import date as dt_date
from logging import Logger
from pathlib import Path
from typing import Literal, List

from celery import shared_task, group
from celery.exceptions import MaxRetriesExceededError
from celery.result import GroupResult
from celery.utils.log import get_task_logger
from django.utils import timezone
from more_itertools import collapse
from scrapy import Spider
from sentinelsat import SentinelAPI, geojson_to_wkt, read_geojson

from eo_engine.common import RemoteFile
from eo_engine.common.db_ops import add_to_db
from eo_engine.common.tasks import get_task_ref_from_name
from eo_engine.common.verify import check_file_exists
from eo_engine.errors import AfriCultuReSError, AfriCultuReSRetriableError
from eo_engine.models import EOProduct, Upload, EOSourceGroupChoices, Credentials, EOSourceGroup, EOSource, \
    EOSourceStateChoices, Pipeline, CrawlerConfiguration, EOProductGroup, EOProductStateChoices
from eo_engine.task_managers import BaseTaskWithRetry

logger: Logger = get_task_logger(__name__)

now = timezone.now()


@shared_task
def task_upload_eo_product(eo_product_id: int):
    eo_product = EOProduct.objects.get(id=eo_product_id)
    upload_entry = Upload.objects.create(eo_product=eo_product)
    try:
        logger.info('MOCK UPLOAD')
    except AfriCultuReSError as e:
        upload_entry.upload_traceback_error = e
        raise e

    try:
        logger.info('MOCK SEND NOTIFICATION')
    except AfriCultuReSError as e:
        upload_entry.notification_traceback_error = e
        # or raise AfriCultuReSError from e
        raise e
    # upload
    raise NotImplementedError()


@shared_task
def task_init_spider(spider_name):
    from eo_engine.common.misc import get_spider_loader
    from eo_engine.common.misc import get_crawler_process
    # from scrapy.spiderloader import SpiderLoader
    # from scrapy.crawler import CrawlerProcess
    # from scrapy.utils.project import get_project_settings
    #
    # # # requires SCRAPY_SETTINGS_MODULE env variable
    # # # currently it's set in DJ's manage.py
    # # scrapy_settings = get_project_settings()
    # # spider_loader = SpiderLoader.from_settings(scrapy_settings)
    spider_loader = get_spider_loader()
    spider: Spider = spider_loader.load(spider_name)
    # print(scrapy_settings)

    # check if this spider is enabled:
    from eo_engine.models.other import CrawlerConfiguration
    rule_qs = CrawlerConfiguration.objects.filter(group=spider.name)
    if not rule_qs.exists():
        msg = 'This spider is not configured. Please Configured it using the web gui'
        logger.error(msg)
        raise AfriCultuReSError(msg)
    rule = rule_qs.get()
    if not rule.enabled:
        msg = 'This spider is not enabled. Exiting without an error'
        logger.info(msg)
        return

    process = get_crawler_process()
    process.crawl(spider)

    # # block until the crawling is finished
    process.start()

    return "Spider {} finished crawling".format(spider_name)


@shared_task()
def task_utils_create_wapor_entry(wapor_group_name: str, from_date: dt_date) -> str:
    from dateutil.rrule import rrule, DAILY
    from eo_engine.models.factories import create_or_get_wapor_object_from_filename
    if isinstance(from_date, str):
        from_date = datetime.datetime.fromisoformat(from_date).date()

    to_date = dt_date.today()
    pat = re.compile(
        r'S06P04_WAPOR_(?P<LEVEL>(L1|L2))_(?P<PROD>(AETI|QUAL_LST|QUAL_NDVI))_D_(?P<LOCATION>(AFRICA|\w{3}|))',
        re.IGNORECASE)
    match = pat.match(wapor_group_name)
    if match is None:
        raise
    group_dict = match.groupdict()
    product_level = group_dict['LEVEL']
    product_name = group_dict['PROD']
    location = group_dict['LOCATION']
    product_dimension = 'D'
    obj_created = 0
    obj_exist = 0
    dt: datetime
    for idx, dt in enumerate(rrule(DAILY, dtstart=from_date, until=to_date), 1):
        year = dt.year
        month = dt.month
        day = dt.day
        from eo_engine.common.time import month_dekad_to_running_decad
        from eo_engine.common.time import day2dekad
        yearly_dekad = month_dekad_to_running_decad(month, day2dekad(day))
        YYKK = f'{str(year)[2:]}{yearly_dekad}'  # last two digits of the year + running dekad
        if location:
            filename = f'{product_level}_{product_name}_{product_dimension}_{YYKK}_{location}.tif'
        else:
            # if location is missing, it's AFRICA
            filename = f'{product_level}_{product_name}_{product_dimension}_{YYKK}.tif'
        obj, created = create_or_get_wapor_object_from_filename(filename)

        if created:
            obj_created += 1

    return f'created {obj_created} entries for {wapor_group_name}'


@shared_task
def task_scan_sentinel_hub(
        from_date: dt_date = None,
        to_date: dt_date = None,
        group_name: Literal[
            EOSourceGroupChoices.S06P01_S1_10M_BAG,
            EOSourceGroupChoices.S06P01_S1_10M_KZN
        ] = None,
        **kwargs):
    credentials = Credentials.objects.get(domain='sentinel')
    if group_name == EOSourceGroupChoices.S06P01_S1_10M_KZN:
        geojson_path = Path('/aux_files/Geojson/KZN_extent_forS1.geojson')
        eo_source_group = EOSourceGroup.objects.get(name=EOSourceGroupChoices.S06P01_S1_10M_KZN)
    elif group_name == EOSourceGroupChoices.S06P01_S1_10M_BAG:
        geojson_path = Path('/aux_files/Geojson/BAG_extent_forS1.geojson')
        eo_source_group = EOSourceGroup.objects.get(name=EOSourceGroupChoices.S06P01_S1_10M_BAG)
    else:
        raise AfriCultuReSError(f'Unknown Group: {group_name}')
    check_file_exists(geojson_path)

    area = geojson_to_wkt(read_geojson(geojson_path))
    api_kwargs = {
        'platformname': 'Sentinel-1',
        'date': (from_date, to_date),
        'swathidentifier': 'IW',
        'producttype': 'GRD',
        'orbitdirection': 'ASCENDING'
    }
    api_kwargs.update(kwargs)
    api = SentinelAPI(user=credentials.username, password=credentials.password)
    for uuid, payload in api.query(
            area=area, **api_kwargs
    ).items():
        remote_file = RemoteFile(
            domain='sentinel',
            url=f'sentinel://{uuid}',
            # XXX: number exists, but need to parse it
            filesize_reported=-1,
            filename=payload['identifier'] + '.zip'
        )
        add_to_db(remote_file=remote_file, eo_source_group=eo_source_group)


@shared_task
def task_sftp_parse_remote_dir(group_name: str):
    # assign remote_urls based on the group
    # urls are in this form: sftp://adf.adf.com/asdf/aa'
    group = EOSourceGroup.objects.get(name=group_name)
    print(group.name)
    if group.name == EOSourceGroupChoices.S06P04_ET_3KM_GLOB_MSG:
        remote_dir = 'sftp://safmil.ipma.pt/home/safpt/OperationalChain/LSASAF_Products/DMET'
    if isinstance(remote_dir, List):
        remote_dir = next(collapse(remote_dir))

    from urllib.parse import urlparse
    from eo_engine.common.sftp import list_dir_entries, sftp_connection
    from eo_engine.common.db_ops import add_to_db
    o = urlparse(remote_dir)
    domain = o.netloc
    path = o.path
    credentials = Credentials.objects.get(domain=domain)
    sftp_connection = sftp_connection(host=domain,
                                      username=credentials.username,
                                      password=credentials.password)
    for entry in list_dir_entries(remotepath=path, connection=sftp_connection):
        add_to_db(entry, eo_source_group=group_name)


@shared_task
def task_utils_download_eo_sources_for_eo_source_group(eo_source_group_id: int) -> str:
    """Schedule to download remote sources from eo_source groups"""
    eo_source_group = EOSourceGroup.objects.get(pk=eo_source_group_id)

    qs = EOSource.objects.filter(
        group=eo_source_group,
        state=EOSourceStateChoices.AVAILABLE_REMOTELY)
    if qs.exists():
        logger.info(f'Found {qs.count()} EOProducts that are ready to download')
        tasks = [task_download_file.s(eo_source_pk=eo_source.pk) for eo_source in qs]
        qs.update(state=EOSourceStateChoices.SCHEDULED_FOR_DOWNLOAD)
        job = group(tasks)

        result: GroupResult = job.apply_async()

        return result.id


@shared_task
def task_utils_discover_eo_sources_for_pipeline(
        pipeline_pk: int,
        eager=False):
    pipeline = Pipeline.objects.get(pk=pipeline_pk)
    input_groups = pipeline.input_groups.all()
    for input_group in input_groups:
        task = task_utils_discover_inputs_for_eo_source_group.s(eo_source_group_id=input_group.pk)
        if eager:
            task.apply()
        else:
            task.apply_async()


@shared_task
def task_utils_download_eo_sources_for_pipeline(
        pipeline_pk: int,
        eager=False):
    """ Use eager to do apply the task locally """
    pipeline = Pipeline.objects.get(pk=pipeline_pk)
    input_groups = pipeline.input_groups.all()
    for input_group in input_groups:
        task = task_utils_download_eo_sources_for_eo_source_group.s(eo_source_group_id=input_group.pk)
        if eager:
            task.apply()
        else:
            task.apply_async()


@shared_task
def task_utils_discover_inputs_for_eo_source_group(
        eo_source_group_pk: int,
        from_date: dt_date,
        eager: bool = False
) -> str:
    try:
        eo_source_group = EOSourceGroup.objects.get(pk=eo_source_group_pk)
    except EOSourceGroup.DoesNotExist:
        logger.info('this PK does not correspond to an EOSOURCE group. If it\'s EOPRODUCTGroup its ok')
        return ''
    group_name = eo_source_group.name
    crawler_type_choices = EOSourceGroup.CrawlerTypeChoices
    crawler_type = eo_source_group.crawler_type
    to_date = dt_date.today()

    task: BaseTaskWithRetry
    if crawler_type == crawler_type_choices.SCRAPY_SPIDER:
        crawler_config, created = CrawlerConfiguration.objects.get_or_create(group=group_name,
                                                                             defaults={'from_date': from_date})
        if not created:
            crawler_config.from_date = from_date
            crawler_config.save()
        task = task_init_spider.s(spider_name=group_name)

    elif crawler_type == crawler_type_choices.OTHER_SFTP:
        task = task_sftp_parse_remote_dir.s(group_name=group_name)

    elif crawler_type == crawler_type_choices.OTHER_SENTINEL:
        task = task_scan_sentinel_hub(from_date=from_date, to_date=to_date, group_name=group_name)
    elif crawler_type == crawler_type_choices.OTHER_WAPOR:
        task = task_utils_create_wapor_entry.s(wapor_group_name=group_name, from_date=from_date)

    else:
        raise AfriCultuReSError(f'Unknown crawler_type: {crawler_type} for eo_source_group_pk: {eo_source_group_pk}')

    if eager:
        job = task.apply()
        return job.task_id
    else:
        job = task.apply_async()

        return f'{job.task_id}'


@shared_task
def task_utils_generate_eoproducts_for_eo_product_group(eo_product_id: int) -> str:
    eo_product_group = EOProductGroup.objects.get(pk=eo_product_id)
    qs_eo_product = EOProduct.objects.filter(group=eo_product_group,
                                             state=EOProductStateChoices.AVAILABLE)
    if qs_eo_product.exists():
        logger.info(f'Found {qs_eo_product.count()} EOProducts that are ready for baking')
        pipeline = eo_product_group.pipelines_from_output.get()
        task_name = pipeline.task_name
        task_kwargs = pipeline.task_kwargs
        task = get_task_ref_from_name(task_name)
        tasks = [task.s(eo_product_pk=eo_product.pk, **task_kwargs) for eo_product in qs_eo_product]
        qs_eo_product.update(state=EOProductStateChoices.SCHEDULED)
        job = group(tasks)
        result: GroupResult = job.apply_async()

        return result.id


@shared_task(bind=True, autoretry_for=(AfriCultuReSRetriableError,), max_retries=100)
def task_download_file(self, eo_source_pk: int):
    """
    Download a remote asset. Identified by it's ID number.
    if it's already available locally, set force_dl=True to download again.
    """
    from urllib.parse import urlparse
    from eo_engine.common.download import (download_http_eosource,
                                           download_ftp_eosource,
                                           download_sftp_eosource,
                                           download_wapor_eosource,
                                           download_sentinel_resource)

    eo_source = EOSource.objects.get(pk=eo_source_pk)
    eo_source.state = EOSourceStateChoices.DOWNLOADING
    eo_source.save()
    url_parse = urlparse(eo_source.url)
    scheme = url_parse.scheme

    logger.info(f'LOG:INFO:Downloading file {eo_source.filename} using scheme: {scheme}')

    try:
        if scheme.startswith('ftp'):
            return download_ftp_eosource(eo_source_pk)
        elif scheme.startswith('http'):
            return download_http_eosource(eo_source_pk)
        elif scheme.startswith('sftp'):
            return download_sftp_eosource(eo_source_pk)
        elif scheme.startswith('wapor'):
            return download_wapor_eosource(eo_source_pk)
        elif scheme.startswith('sentinel'):
            return download_sentinel_resource(eo_source_pk)
        else:
            raise Exception(f'There was no defined method for scheme: {scheme}')
    except AfriCultuReSRetriableError as exc:
        eo_source.refresh_from_db()
        eo_source.state = EOSourceStateChoices.DEFERRED
        eo_source.save()
        try:
            raise self.retry(countdown=10)
        except MaxRetriesExceededError as e:
            logger.info(f'LOG:INFO:DOWNLOADING_FILE. Maximum attempts exceeded. Failing.')
            eo_source.refresh_from_db()
            eo_source.state = EOSourceStateChoices.DOWNLOAD_FAILED
            eo_source.save()
    except BaseException as e:
        eo_source.refresh_from_db()
        eo_source.state = EOSourceStateChoices.DOWNLOAD_FAILED
        eo_source.save()
        raise Exception('Could not download.') from e


__all__ = [
    'task_upload_eo_product',
    'task_init_spider',
    'task_utils_create_wapor_entry',
    'task_scan_sentinel_hub',
    'task_sftp_parse_remote_dir',
    'task_utils_download_eo_sources_for_eo_source_group',
    'task_utils_discover_eo_sources_for_pipeline',
    'task_utils_download_eo_sources_for_pipeline',
    'task_utils_discover_inputs_for_eo_source_group',
    'task_utils_generate_eoproducts_for_eo_product_group'
]
