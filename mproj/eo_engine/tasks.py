import datetime
import os
import re
import subprocess
from datetime import date as dt_date
from logging import Logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional, Literal, NamedTuple

import numpy as np
import pandas
from celery import group
from celery.app import shared_task
from celery.exceptions import MaxRetriesExceededError
from celery.result import GroupResult
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.files import File
from django.core.files.temp import NamedTemporaryFile
from django.db import connections
from django.utils import timezone
from more_itertools import collapse
from osgeo import gdal
from scrapy import Spider

from eo_engine.common.tasks import get_task_ref_from_name
from eo_engine.common.verify import check_file_exists
from eo_engine.errors import AfriCultuReSRetriableError, AfriCultuReSError
from eo_engine.models import *
from eo_engine.task_managers import BaseTaskWithRetry

logger: Logger = get_task_logger(__name__)

now = timezone.now()


# Notes:
#  task should use the kwargs eo_source_pk and eo_product_pk.
#  these kw are picked up to tie the task with the element


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
    from eo_engine.common.db_ops import add_to_db
    from sentinelsat import SentinelAPI, geojson_to_wkt, read_geojson
    from eo_engine.common import RemoteFile

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


# SFTP
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


# Each pipeline has only one output product,
# so not needed to make 'for pipeline'
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


#############################
# TASKS

# RULES:
# TASKS THAT THAT MAKE PRODUCTS must be pass this filter: task_s??p??*
# TASKS THAT THAT MAKE PRODUCTS must have the argument eo_product_pk: int in their signature

###########
# s02p02
###########
@shared_task
def task_s02p02_ndvi300m_v2(eo_product_pk: int):
    produced_file = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = produced_file.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=produced_file.reference_date)
    # only has one input
    eo_source_input: EOSource = input_files_qs.get()

    filename_in = eo_source_input.filename

    def clip(in_file, out_file, geom):
        lat_str = "lat," + str(geom[0]) + "," + str(geom[1])
        lon_str = "lon," + str(geom[2]) + "," + str(geom[3])
        cp = subprocess.run(['ncks',
                             '-v', 'NDVI',
                             '-d', lat_str,
                             '-d', lon_str,
                             #                              '-d', "lat,13439,40320",
                             #                             '-d', "lon,50390,80640",
                             in_file,
                             out_file], check=True)
        return_path = Path(out_file)  # Path(str(clipped.getFileLocation()))
        print(return_path)
        return return_path

    with TemporaryDirectory(prefix='task_s0p02_clip_ndvi300m_v2_afr_') as temp_dir:
        geom = [13439, 40320, 50390, 80640]

        out_file = Path(temp_dir) / produced_file.filename
        clipped: Path = clip(in_file=eo_source_input.file.path, out_file=out_file.as_posix(), geom=geom)
        try:
            cp: subprocess.CompletedProcess = subprocess.run(
                ['ncatted',
                 '-a', f'short_name,NDVI,o,c,Normalized_difference_vegetation_index',
                 '-a', f'long_name,NDVI,o,c,Normalized Difference Vegetation Index Resampled 1 Km',
                 '-a', f'grid_mapping,NDVI,o,c,crs',
                 '-a', f'flag_meanings,NDVI,o,c,Missing cloud snow sea background',
                 '-a', f'flag_values,NDVI,o,c,[251 252 253 254 255]',
                 clipped.as_posix()], check=True)
        except subprocess.CalledProcessError as e:

            logger.info(f'EXIT CODE: {e.returncode}')
            logger.info(f'EXIT CODE: {e.stderr}')
            logger.info(f'EXIT CODE: {e.stdout}')
            raise e

        content = File(clipped.open('rb'))
        produced_file.file.save(name=produced_file.filename, content=content, save=False)
        produced_file.state = EOProductStateChoices.READY
        produced_file.datetime_creation = now
        produced_file.save()
    return


@shared_task
def task_s02p02_nvdi1km_v3(eo_product_pk):
    """" Resamples to 1km and cuts to AOI bbox """

    eo_product = EOProduct.objects.get(id=eo_product_pk)
    # this pipeline needs eo_products, 'S02P02_NDVI_300M_V3_AFR' which was made in another pipeline
    input_eo_product_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eoproductgroup
    input_files_qs = EOProduct.objects.filter(group=input_eo_product_group, reference_date=eo_product.reference_date)
    input_file = input_files_qs.get()

    target_resolution = 0.0089285714286
    xmin, ymin, xmax, ymax = -30.0044643, -40.0044643, 60.0066643, 40.0044643

    # Mark it as 'in process'
    eo_product.state = EOProductStateChoices.GENERATING
    eo_product.save()
    # input file//eo_product
    input_obj: EOProduct = input_file

    with TemporaryDirectory() as tmp_dir:
        output_temp_file = f"{tmp_dir}/tmp_file.nc"
        cp = subprocess.run([
            'gdalwarp',
            '-r', 'average',
            '-tr', f'{target_resolution}', f'{target_resolution}',
            '-te', f'{xmin}', f'{ymin}', f'{xmax}', f'{ymax}',
            f'{input_obj.file.path}', output_temp_file
        ], check=True)

        # metadata fine tuning using NCO tools
        # for usuage details see:
        # http://nco.sourceforge.net/nco.html#ncatted-netCDF-Attribute-Editor

        # Rename default Band1 to NDVI
        print('Rename Variable')
        cp = subprocess.run(['ncrename',
                             '-v', 'Band1,NDVI',
                             output_temp_file], check=True)

        # ncatted is for nc attribute editor
        print('Editing metadata')
        cp = subprocess.run(['ncatted',
                             '-a', 'short_name,NDVI,o,c,normalized_difference_vegetation_index',
                             '-a', "long_name,NDVI,o,c,Normalized Difference Vegetation Index Resampled 1 Km",

                             output_temp_file], check=True)

        with open(output_temp_file, 'rb') as fh:
            content = File(fh)
            eo_product.file.save(name=eo_product.filename, content=content)
            eo_product.state = EOProductStateChoices.READY
            eo_product.datetime_creation = now
            eo_product.save()
        os.unlink(output_temp_file)

    return


@shared_task
def task_s02p02_vci1km_v2(eo_product_pk):
    # Processing of the resampled NDVI 1km v3 (clipped to Africa) in order to retrieve VCI v2 for Africa
    # version 1.0 - 16/04/2021
    # Contact: icherif@yahoo.com
    # -----------------------------

    # Required aux data.

    import os
    import snappy
    from snappy import ProductIO, GPF, HashMap

    HashMap = snappy.jpy.get_type('java.util.HashMap')
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

    def write_product(data, file_path, format=None):
        ProductIO.writeProduct(data, file_path, format if format else 'NetCDF4-CF')

    def merge(data, data1, data2):
        params = HashMap()
        merged = GPF.createProduct('BandMerge', params, (data, data1, data2))
        band_names = merged.getBandNames()
        print("Merged Bands:   %s" % (list(band_names)))
        return merged

    # noinspection PyUnresolvedReferences
    def get_VCI(data, file, dir):
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
        BandDescriptor = snappy.jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')
        targetBand = BandDescriptor()
        targetBand.name = 'VCI'
        targetBand.type = 'float32'
        targetBand.noDataValue = 255.0
        targetBand.expression = 'if ( (NDVI > 0.92) or (max > 0.92) or (min >0.92)) then 255.0 else ((NDVI - min)/ (max - min))'

        targetBands = snappy.jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', 1)
        targetBands[0] = targetBand
        params = HashMap()
        params.put('targetBands', targetBands)
        vci_float = GPF.createProduct('BandMaths', params, data)
        # write intermediate file (unscaled)
        # write_product(vci_float, os.path.join(dir, file))
        return vci_float

    def vci_to_int(data, file, dir):
        # band_names = data.getBandNames()
        # print("Bands:   %s" % (list(band_names)))

        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
        BandDescriptor = snappy.jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')
        targetBand = BandDescriptor()
        targetBand.name = 'VCI'
        targetBand.type = 'int32'
        targetBand.noDataValue = 255
        targetBand.expression = 'if (VCI == 255) then 255 else ( ' \
                                'if (VCI > 1.125) then 250 else ( ' \
                                'if (VCI<-0.125) then 0  else rint((VCI + 0.125)/0.005)))'  # 250 is the max value, 0 is the min value
        targetBands = snappy.jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', 1)
        targetBands[0] = targetBand
        params = HashMap()
        params.put('targetBands', targetBands)
        vci = GPF.createProduct('BandMaths', params, data)
        write_product(vci, os.path.join(dir, file))
        return vci

    def process(f_ndvi, lts_dir, dir_out):
        # Get the dekad number

        dekad = os.path.basename(f_ndvi).split('_')[0][4:]
        f_lts = lts_dir + '/cgls_NDVI-LTS_' + dekad + '_V3.nc'

        f_lts_min = f_lts[:-3] + '_min.nc'
        f_lts_max = f_lts[:-3] + '_min.nc'
        print('\n== Processing ==')
        print('NDVI file: ', f_ndvi)
        print('with LTS min: ', f_lts_min)
        print('with LTS max: ', f_lts_max)

        print(Path(f_lts_max).is_file())

        data = ProductIO.readProduct(f_ndvi)
        data1 = ProductIO.readProduct(f_lts[:-3] + '_min.nc')
        data2 = ProductIO.readProduct(f_lts[:-3] + '_max.nc')
        try:
            merged = merge(data, data1, data2)
        except Exception as e:

            print('Problem merging the bands')
            raise e
        try:
            vci_float = get_VCI(merged, f_ndvi[:-3] + '_VCI_temp.nc', dir_out)
        except Exception:
            print('Problem computing VCI')
        try:
            date = os.path.basename(f_ndvi).split('_')[0]
            filename = 'g2_BIOPAR_VCI_' + date + '_AFRI_OLCI_V2.0.nc'
            vci = vci_to_int(vci_float, filename, dir_out)
        except Exception as e:
            print('Problem transforming VCI')
            raise e
        return os.path.join(dir_out, filename)

    output_obj = EOProduct.objects.get(id=eo_product_pk)

    input_eo_product_group = output_obj.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eoproductgroup
    input_files_qs = EOProduct.objects.filter(group=input_eo_product_group, reference_date=output_obj.reference_date)
    ndvi_1k_obj = input_files_qs.get()

    lts_dir = Path('/aux_files/NDVI_LTS')
    ndvi_path = ndvi_1k_obj.file.path

    with TemporaryDirectory() as tempdir:
        outfile = process(ndvi_path, lts_dir.as_posix(), tempdir)
        # set outfile (VCI) metadata
        # ncatted is for nc attribute editor
        print('Adding VCI Metadata')
        cp = subprocess.run(['ncatted',
                             '-a', 'short_name,VCI,o,c,vegetation_condition_index',
                             '-a', "long_name,VCI,o,c,Vegetation Condition Index 1 Km",
                             '-a', "units,VCI,o,c,-",
                             '-a', "scale_factor,VCI,o,d,0.005",
                             '-a', "add_offset,VCI,o,d,-0.125",
                             '-a', "missing_value,VCI,o,d,255",
                             outfile], check=True)

        content = File(open(outfile, 'rb'))

        output_obj.file.save(name=output_obj.filename, content=content, save=False)
        output_obj.state = EOProductStateChoices.READY
        output_obj.datetime_creation = now
        output_obj.save()
    return


@shared_task
def task_s02p02_lai300m_v1(eo_product_pk: int):
    produced_file = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = produced_file.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=produced_file.reference_date)
    # only has one input
    eo_source_input: EOSource = input_files_qs.get()

    filename_in = eo_source_input.filename
    rt = filename_in.split('-')[1][:3]
    ver = filename_in.split('_V')[1][:5]

    def clip(in_file, out_file, geom):
        lat_str = "lat," + str(geom[0]) + "," + str(geom[1])
        lon_str = "lon," + str(geom[2]) + "," + str(geom[3])
        subprocess.run(['ncks',
                        '-v', 'LAI',
                        '-d', lat_str,
                        '-d', lon_str,
                        in_file,
                        out_file], check=True)
        return_path = Path(out_file)  # Path(str(clipped.getFileLocation()))
        logger.info(return_path)
        return return_path

    # noinspection DuplicatedCode
    with TemporaryDirectory(prefix='task_s0p02_clip_lai300m_v1_afr_') as temp_dir:
        geom = [14200, 38800, 53900, 78700]
        out_file = Path(temp_dir) / produced_file.filename
        clipped: Path = clip(in_file=eo_source_input.file.path, out_file=out_file.as_posix(), geom=geom)
        try:
            subprocess.CompletedProcess = subprocess.run(
                ['ncatted',
                 '-a', f'Consolidation_period,LAI,o,c,{rt}',
                 '-a', f'LAI_version,LAI,o,c,{ver}',
                 clipped.as_posix()], check=True)
        except subprocess.CalledProcessError as e:

            logger.info(f'EXIT CODE: {e.returncode}')
            logger.info(f'EXIT CODE: {e.stderr}')
            logger.info(f'EXIT CODE: {e.stdout}')
            raise e

        content = File(clipped.open('rb'))
        produced_file.file.save(name=produced_file.filename, content=content, save=False)
        produced_file.state = EOProductStateChoices.READY
        produced_file.datetime_creation = now
        produced_file.save()
    return


# noinspection DuplicatedCode
@shared_task
def task_s02p02_ndvianom250m(eo_product_pk: int, iso: str):
    produced_file = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = produced_file.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=produced_file.reference_date)

    import rasterio
    from rasterio.merge import merge as rio_merge

    # noinspection SpellCheckingInspection
    def mosaic_f(in_files: List[Path], outfile: Path) -> Path:
        # prepend vsigzip if filename ends in .gz

        datasets = ['/vsigzip/' + x.as_posix() if x.suffix.endswith('gz') else x.as_posix() for x in in_files]
        logger.info(f'task_s02p02_process_ndvia:mosaic_f:datasets:{datasets}')
        mosaic, out_trans = rio_merge(
            datasets=datasets,
            nodata=255,
            method='max')

        # Update the metadata
        # get the meta from the first input
        src = rasterio.open(datasets[0])
        out_meta = src.meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2], "transform": out_trans,
            "crs": "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
            "tiled": True,
            'compress': 'LZW',
            "dtype": 'uint8',
        })
        with rasterio.open(outfile, "w", **out_meta) as dest:
            dest.write(mosaic)

        return Path(outfile)

    def clip(file_in: Path, file_out_path: Path, shp_file_path: Path) -> Path:
        # clip using the shapefile ot a netcdf format
        print("\nClipping file: %s" % file_in)

        warp_options = gdal.WarpOptions(
            cutlineDSName=shp_file_path.as_posix(), cropToCutline=True,
            dstSRS="EPSG:4326",
            format='netCDF',
            dstNodata=255,
            outputType=gdal.GDT_UInt16, options=['COMPRESS=LZW'])
        gdal.Warp(
            srcDSOrSrcDSTab=file_in.as_posix(),
            destNameOrDestDS=file_out_path.as_posix(),
            options=warp_options)
        return Path(file_out_path)

    def add_metadata(file_in: Path, file_out: Path) -> Path:
        import xarray as xr
        # Load the dataset
        ds = xr.open_dataset(file_in)

        # select parameters according to the product.
        da = ds.Band1

        # Output write
        try:
            da.name = 'NDVIA'
            da.attrs['short_name'] = 'NDVI anomaly'
            da.attrs['long_name'] = 'Normalized Difference Vegetation Index (NDVI) anomaly'
            da.attrs['_FillValue'] = 0
            da.attrs['scale_factor'] = 0.008
            da.attrs['add_offset'] = -1
            da.attrs['flag_masks'] = 253, 254, 255
            da.attrs['flag_meanings'] = "invalid water no_data"
            da.attrs['valid_range'] = 0, 250  # meaning -1 to 1
            parameters = dict({'NDVIA': {'dtype': 'f4', 'zlib': 'True', 'complevel': 4}})
            da.to_netcdf(file_out, encoding=parameters)
        except Exception as ex:
            template = "An exception of type {0} occurred while resampling. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logger.error(message)
            raise AfriCultuReSError(message)
        return Path(file_out)

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    if iso == 'ZAF':
        # South Africa
        f_shp = '/aux_files/Border_shp/Country_SAfr.shp'
    elif iso == 'MOZ':
        f_shp = '/aux_files/Border_shp/MOZ_adm0.shp'
    elif iso == 'TUN':
        f_shp = '/aux_files/Border_shp/TN_envelope.shp'
    elif iso == 'KEN':
        f_shp = '/aux_files/Border_shp/KEN_adm0.shp'
    elif iso == 'GHA':
        f_shp = '/aux_files/Border_shp/GHA_adm0.shp'
    elif iso == 'RWA':
        f_shp = '/aux_files/Border_shp/RWA_adm0.shp'
    elif iso == 'ETH':
        f_shp = '/aux_files/Border_shp/ETH_adm0.shp'
    elif iso == 'NER':
        f_shp = '/aux_files/Border_shp/NER_adm0.shp'
    else:
        raise AfriCultuReSError(f'no valid iso: {iso}')

    f_shp_path = Path(f_shp)
    if not f_shp_path.exists():
        raise AfriCultuReSError(f'Shapefile {f_shp_path.as_posix()} does not exist')

    with TemporaryDirectory() as temp_dir:
        input_files_path: List[Path] = [Path(x.file.path) for x in input_files_qs]
        temp_dir_path = Path(temp_dir)

        mosaic_f_path = mosaic_f(input_files_path, temp_dir_path / 'mosaic.tif')
        clipped_f_path = clip(mosaic_f_path, file_out_path=temp_dir_path / 'clipped.nc', shp_file_path=f_shp_path)
        final_raster_path = add_metadata(file_in=clipped_f_path, file_out=temp_dir_path / 'final_file.nc')

        content = File(final_raster_path.open('rb'))
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.READY
        eo_product.datetime_creation = now
        eo_product.save()
    return eo_product.file.path


###########
# s04p01
##########
# noinspection SpellCheckingInspection
@shared_task
def task_s04p01_lulc500m(eo_product_pk):
    from pymodis.convertmodis_gdal import createMosaicGDAL
    from osgeo import gdal

    eo_product = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=eo_product.reference_date)

    files: List[Path] = [Path(f.file.path) for f in input_files_qs]
    logger.info(f'INFO: input files count:  {files.__len__()}')
    # Create a subset to mosaic only LC_Type2
    subset = [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    with TemporaryDirectory('task_s04p01_lulc500m') as temp_dir:
        temp_dir_path = Path(temp_dir)
        output_file_1 = temp_dir_path / 'output_mosaic.hdf'
        output_file_2 = temp_dir_path / 'output_mosaic.nc'

        logger.info('Creating Mosaic Object')
        mosaic = createMosaicGDAL(
            hdfnames=[f.as_posix() for f in files],
            subset=subset,
            outformat='HDF4Image')

        logger.info('Create mosaicing image')
        mosaic.run(output_file_1.as_posix())

        # Define GDAL warp options to transform mosaiced image to netCDF format and reprojected to EPSG:4326
        # gdal_options = gdal.WarpOptions(
        #     format='netCDF', dstSRS='EPSG:4326'
        # )
        kwargs = {'format': 'netCDF', 'dstSRS': 'EPSG:4326'}

        # Define input raster for warp
        input_raster = gdal.Open(output_file_1.as_posix())

        logger.info(f'Running gdal.warp to generate nc output. kwargs are {[f"{k}:{v}" for k, v in kwargs.items()]}')
        gdal.Warp(output_file_2.as_posix(), input_raster, **kwargs)

        with output_file_2.open('rb') as file_handler:
            content = File(file_handler)
            eo_product.file.save(name=eo_product.filename, content=content, save=False)
            eo_product.state = EOProductStateChoices.READY
            eo_product.datetime_creation = now
            eo_product.save()

        return '+++Finished+++'


###########
# s04p03
##########
@shared_task
def task_s04p03_convert_to_tiff(eo_product_pk: int, tile: int):
    from osgeo import gdal, gdalconst

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    eo_source: EOSource = eo_product.eo_sources_inputs.first()

    eo_product.state = EOProductStateChoices.GENERATING
    eo_product.save()

    with NamedTemporaryFile('wb') as file_handle:
        ds = gdal.Open(eo_source.file.path)

        optionsNC2 = gdal.TranslateOptions(
            format='netCDF',
            # using gdalconst.GDT_Unknown was the only way to avoid
            # conversion to float and keep file size reasonable
            outputType=gdalconst.GDT_Unknown,
            noData=int(1), options=['COMPRESS=LZW'],
            outputSRS="EPSG:4326")  # 1 is the nodata value
        gdal.Translate(srcDS=ds, destName=file_handle.name, options=optionsNC2)

        subprocess.run(['ncrename',
                        '-v', 'Band1,Flood',
                        file_handle.name], check=True)
        subprocess.run(['ncatted',
                        '-a', 'short_name,Flood,o,c,Flood_MR',
                        '-a', "long_name,Flood,o,c,Flood map at medium resolution",
                        '-a', "tile_number,Flood,o,c," + str(tile),
                        '-a', "_FillValue,Flood,o,i,1",
                        file_handle.name], check=True)

        content = File(file_handle)
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.READY
        eo_product.datetime_creation = now
        eo_product.save()


@shared_task
def task_s04p03_floods375m(eo_product_pk: int):
    from rasterio.merge import merge
    eo_product = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=eo_product.reference_date)

    with TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        dst_path = temp_dir_path / 'mosaic.tif'
        out_nc = Path(temp_dir) / 'out.nc'

        datasets: List[Path] = list(map(Path, [x.file.path for x in input_files_qs]))

        merge(datasets=datasets, dst_path=dst_path, nodata=0)

        print('translating')
        subprocess.run([
            'gdal_translate',
            '-of', 'netCDF',
            dst_path.as_posix(),
            out_nc.as_posix()
        ], check=True)
        subprocess.run(['ncrename',
                        '-v', 'Band1,Flood',
                        out_nc.as_posix()], check=True)
        subprocess.run(['ncatted',
                        '-a', 'short_name,Flood,o,c,Flood_MR',
                        '-a', "long_name,Flood,o,c,Flood_map_at_medium_resolution",
                        '-a', "_FillValue,Flood,o,i,1",
                        out_nc.as_posix()], check=True)

        content = File(out_nc.open('rb'))
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.READY
        eo_product.datetime_creation = now
        eo_product.save()


@shared_task
def task_s04p03_floods10m(eo_product_pk: int):
    NotImplementedError()


###########
# s06p01
##########
@shared_task
def task_s06p01_wb300m_v2(eo_product_pk: int):
    eo_product = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=eo_product.reference_date)
    input_file = input_files_qs.get()

    def clip(file_in, file_out, geom) -> Path:
        lat_str = "lat," + str(geom[0]) + "," + str(geom[1])
        lon_str = "lon," + str(geom[2]) + "," + str(geom[3])
        subprocess.run(['ncks',
                        '-d', lat_str,
                        '-d', lon_str,
                        file_in,
                        file_out], check=True)
        return Path(file_out)

    with TemporaryDirectory(prefix='task_s06p01_clip_to_africa_') as temp_dir:
        date = input_file.filename.split('_')[3][:8]
        f_out = date + '_SE2_AFR_0300m_0030_WBMA.nc'
        geom = [16904, 46189, 64166, 93689]
        clipped = clip(input_file.file.path, Path(temp_dir).joinpath(f_out), geom)

        content = File(clipped.open('rb'))
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.READY
        eo_product.datetime_creation = now
        eo_product.save()

    return 0


@shared_task
def task_s06p01_wb10m_kzn(eo_product_pk: int):
    from eo_engine.common.misc import write_line_to_file
    eo_product = EOProduct.objects.get(id=eo_product_pk)
    logger.info(f'EOProduct: {eo_product}')
    reference_date = eo_product.reference_date
    logger.info(f'Reference_Date: {reference_date}')
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.all()
    input_files_qs = EOSource.objects.filter(
        group__in=input_eo_source_group,
        reference_date=reference_date)
    logger.info(f'Number of input Files: {input_files_qs.count()}')

    # All the variables that I will need
    Params = NamedTuple('Params', [
        ('south_africa_shapefile', Path),
        ('hand_file_tif_path', Path),
        ('dem_file_tif_path', Optional[Path]),
        ('orbits_threshold_csv_file_path', Path),
        ('archive_folder', Path),
        ('sentinelhub_username', str),
        ('sentinelhub_password', str),
        ('hand_threshold', int),
        ('dem_threshold', int),
        ('sentinel_zip_files_path_list', List[Path]),
        ('reference_date', datetime.date)
    ])

    OrbitLine = NamedTuple('OrbitLine', [
        ('date', str), ('threshold', float), ('relative_orbit', int)
    ])

    params = Params(
        south_africa_shapefile=settings.AUX_FILES_ROOT / 'Border_shp/Country_SAfr.shp',
        hand_file_tif_path=settings.AUX_FILES_ROOT / 'HAND/HAND_kzn_fs_2.tif',
        orbits_threshold_csv_file_path=settings.AUX_FILES_ROOT / "WB_kzn/Listparams_kzn.csv",
        archive_folder=settings.AUX_FILES_ROOT / "WB_kzn/archive",
        dem_file_tif_path=None,
        sentinel_zip_files_path_list=[],
        reference_date=reference_date,
        sentinelhub_username="nesch",
        sentinelhub_password="w@m0sPr0ject",
        hand_threshold=10,
        dem_threshold=0)

    params.archive_folder.mkdir(exist_ok=True)

    check_file_exists(file_path=params.south_africa_shapefile)
    check_file_exists(file_path=params.hand_file_tif_path)
    check_file_exists(file_path=params.orbits_threshold_csv_file_path)
    # add the sentinel zip files
    obj: EOProduct
    for obj in input_files_qs:
        _obj_path = Path(obj.file.path)
        params.sentinel_zip_files_path_list.append(_obj_path)
        del _obj_path

    def _process(output_folder: Path) -> Path:
        import csv

        date_str = params.reference_date.strftime('%Y%m%d')

        def get_relative_orbit_from_scihub(
                file_in: str) -> int:
            """Get the relative orbit from scihub for this file"""

            from sentinelsat import SentinelAPI
            api = SentinelAPI(
                params.sentinelhub_username,
                params.sentinelhub_password)
            file = file_in.replace("\\", "/").split('/')[-1]
            identifier = str(file[:len(file) - 13])
            products = api.query(
                platformname='Sentinel-1',
                identifier=identifier)
            products_df: pandas.DataFrame = api.to_dataframe(products)
            return products_df.relativeorbitnumber.values[0]

        def filename_to_date(
                file_name: str) -> datetime.date:
            """ Function to retrieve the date from the filename"""

            file = file_name.replace("\\", "/").split('/')[-1]
            part = str(file[:len(file) - 13]).split('_')[4]
            date: datetime.datetime = datetime.datetime.strptime(part[0:8], '%Y%m%d')
            return date.date()

        def write_to_orbit_thesholds_file(
                orbit_csv_file_path: Path,
                orbit_line_data: OrbitLine):

            with orbit_csv_file_path.open('a') as csv_file:
                headers = ['date', 'threshold', 'relative_orbit']
                writer = csv.DictWriter(csv_file, delimiter=',', fieldnames=headers)
                if not orbit_csv_file_path.exists():
                    # file does not exist, write headers
                    writer.writeheader()
                writer.writerow(orbit_line_data._asdict())
            return

        def read_orbit_threholds_csv(
                params_txt_path: Path,
                relative_orbit: int,
                reference_date: dt_date,
                max_threshold: float) -> (int, str):

            """ Function to read the last threshold found for the same relative orbit"""

            dtype = {
                'date': str,
                'threshold': float,
                'relative_orbit': int
            }
            dataframe = pandas.read_csv(params_txt_path, dtype=dtype, parse_dates=['date'])

            records: pandas.DataFrame = dataframe.query('relative_orbit == @relative_orbit')
            # Go through all lines with specific orbit, last record will be the newest one
            if records.empty:
                return max_threshold, ""
            else:
                sorted_records = records.sort_values('date', ascending=False)
                # return latest
                threshold = sorted_records['threshold'].values[0]
                latest_day = sorted_records['date'].values[0]
                txt = f'Threshold found for relative orbit={relative_orbit}, date={str(latest_day)}, threshold={threshold}'
                return threshold, txt

        def thresh_process(
                sigma_db: Path,
                water_tif: Path,
                log_file: Path = None
        ):

            # Function to detect and Apply the water threshold

            # Set constant values
            _constant_max_thresh = -10.0
            _constant_def_thresh = -20.0

            # Get the raster corresponding to the filename
            ds = gdal.Open(sigma_db.as_posix())
            if ds is None:
                raise AfriCultuReSError(f'Error opening file: {sigma_db.as_posix()}')

            # Read the first band
            band = ds.GetRasterBand(1)
            arr = band.ReadAsArray()

            # Get input file details (two possible ways)
            relative_orbit = get_relative_orbit_from_scihub(sigma_db.name)  # from scihub
            date = filename_to_date(sigma_db.name)
            write_line_to_file(file_path=log_file,
                               token=f'Relative orbit is: {relative_orbit}, Relative Date is {date}',
                               echo=True)

            arr_1d = arr[arr != 0.0]
            arr_min = arr_1d.min()
            arr_max = arr_1d.max()
            arr_n = (arr_1d - arr_min) / (arr_max - arr_min)

            bins = np.true_divide(range(0, 256), 255)
            counts = np.histogram(arr_n, bins)

            # Get Otsu Valley Emphasis threshold
            total = arr_n.shape[0]
            current_max, thresh_n = 0, 0
            sumVal, MU_2, MU_2, MU_K = 0, 0, 0, 0
            for t in range(0, 255):
                sumVal += t * counts[0][t] / total
                # print (sumVal)

            OMEGA1, OMEGA2 = 0, 0
            # varBetween, meanB, meanF = 0, 0, 0
            for t in range(0, 255):
                OMEGA1 += counts[0][t] / total
                OMEGA2 = 1 - OMEGA1
                if OMEGA2 == 0:
                    break
                MU_K += t * counts[0][t] / total
                MU_1 = MU_K / OMEGA1
                MU_2 = (sumVal - MU_K) / OMEGA2
                weight = (1 - (counts[0][t] / total))  # weight=1 #For Otsu set weight to 1
                varBetween = weight * (OMEGA1 * MU_1 * MU_1 + OMEGA2 * MU_2 * MU_2)  # only difference with Otsu
                if varBetween > current_max:
                    current_max = varBetween
                    thresh_n = t

            threshold = (arr_max - arr_min) * (thresh_n / 255) + arr_min
            write_line_to_file(log_file, f"Calculated Threshold: {threshold}", echo=True)
            if threshold > float(_constant_max_thresh):
                write_line_to_file(file_path=log_file, token='water threshold NOT found!', echo=True)
                # txtout.write('\nWater threshold NOT found!')
                # Get threshold from saved text file
                threshold, report = read_orbit_threholds_csv(
                    params.orbits_threshold_csv_file_path,
                    relative_orbit=relative_orbit,
                    reference_date=date,
                    max_threshold=_constant_max_thresh)
                if threshold >= float(_constant_max_thresh):  # default threshold used
                    write_line_to_file(file_path=log_file, token='Default water threshold used!', echo=True)
                    # Set threshold to the default value
                    threshold = _constant_def_thresh
                else:
                    write_line_to_file(file_path=log_file, token=report, echo=True)

            else:

                write_line_to_file(file_path=log_file,
                                   token='Water threshold found!. Appending it to file',
                                   echo=True)
                orbit_line = OrbitLine(date=str(date), threshold=threshold, relative_orbit=relative_orbit)
                write_to_orbit_thesholds_file(params.orbits_threshold_csv_file_path, orbit_line)
            write_line_to_file(file_path=log_file, token=f"Threshold is:{threshold}", echo=True)

            # Apply threshold value to raster
            ind = (arr == 0)  # keep indices of NaN values
            arr_out = 1.0 * (arr < threshold)  # to have integer values
            arr_out[ind] = 11  # set new NaN value

            geotiff_drv = gdal.GetDriverByName("GTiff")

            ds_out = geotiff_drv.Create(
                water_tif.as_posix(),
                ds.RasterXSize,
                ds.RasterYSize,
                1,
                gdal.GDT_Byte)  # GDT_UInt16

            ds_out.SetProjection(ds.GetProjection())
            ds_out.SetGeoTransform(ds.GetGeoTransform())
            ds_out.GetRasterBand(1).SetNoDataValue(11)
            ds_out.GetRasterBand(1).WriteArray(arr_out)

            try:
                del ds, arr, arr_out
            except:
                print('Error deleting datasets')
                # raise AfriCultuReSError()

        # -----------------------
        # Cleaning the water map
        # -----------------------

        def preprocess_filter(
                water_tif: Path,
                hand_file_tif_path: Path,
                temp_hand_file: Path):
            # import rasterio as rio
            print("start preprocess filtering")
            # Function to clip and resample the HAND raster to fit input file pixel size and dimension

            try:
                ds_in = gdal.Open(water_tif.as_posix())
            except:
                msg = f'Error opening file {water_tif.name}'
                raise AfriCultuReSError(msg)
            try:
                ds_in_hand = gdal.Open(hand_file_tif_path.as_posix())
            except:
                msg = f'Error opening file{hand_file_tif_path.name}'
                raise AfriCultuReSError(msg)

            geoTransform = ds_in.GetGeoTransform()
            minX = geoTransform[0]
            maxY = geoTransform[3]
            maxX = minX + geoTransform[1] * ds_in.RasterXSize
            minY = maxY + geoTransform[5] * ds_in.RasterYSize

            warpOptions = gdal.WarpOptions(
                format="Gtiff",
                # dstSRS="EPSG:4326",
                outputBounds=(minX, minY, maxX, maxY),
                width=ds_in.RasterXSize, height=ds_in.RasterYSize)
            try:
                gdal.Warp(
                    destNameOrDestDS=temp_hand_file.as_posix(),
                    srcDSOrSrcDSTab=ds_in_hand,
                    options=warpOptions)
            except:
                raise AfriCultuReSError('Error warping HAND file')
            return

        def apply_filter(
                water_tif_input: Path,
                # file_in_dem: Path,
                temp_hand_file_path: Path,
                water2_tif_output: Path,
                dem_thresh: float,
                hand_thresh: float):
            """
            Function to exclude water pixel where HAND threshold and DEM thresholds are exceeded. Unless they are set to -1, in that case they are ignored.
            """

            print("start apply filter")
            # Read files with GDAL
            try:
                water_band = gdal.Open(water_tif_input.as_posix())
            except:
                msg = f'Error opening file {water_tif_input.name}'
                raise AfriCultuReSError(msg)

            if float(hand_thresh) > 0:
                try:
                    hand_band = gdal.Open(temp_hand_file_path.as_posix())
                except:
                    msg = f'Error opening file {temp_hand_file_path.name}'
                    raise AfriCultuReSError(msg)

                # Get input values
                water = water_band.GetRasterBand(1).ReadAsArray()
                hand = hand_band.GetRasterBand(1).ReadAsArray()

                # Clean the water map
                water_filtered = water

                hand[np.isnan(hand)] = float(
                    hand_thresh) - 1  # get rid of the nan values for the logic less below computation
                water_filtered[
                    (hand > float(hand_thresh)) & (
                            water == 1)] = 0  # keep only the pixels with HAND below threshold

                try:
                    # Write output file
                    driver = gdal.GetDriverByName("GTiff")
                    ds_out = driver.Create(
                        water2_tif_output.as_posix(),
                        water.shape[1],
                        water.shape[0], 1,
                        gdal.GDT_Byte,
                        ['COMPRESS=LZW'])  # GDT_UInt16
                    ds_out.SetProjection(water_band.GetProjection())
                    ds_out.SetGeoTransform(water_band.GetGeoTransform())
                    ds_out.GetRasterBand(1).SetNoDataValue(11)
                    ds_out.GetRasterBand(1).WriteArray(water_filtered)
                except:
                    msg = 'Error storing the water map'
                    print(msg)
                    raise AfriCultuReSError(msg)
            else:
                raise AfriCultuReSError('hand_theshold < 0')

            return

        def clean_process(
                water_tif_input: Path,
                water2_tif_output: Path
        ):
            # Get the temp files of resampled DEM and HAND
            with TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                hand_temp = temp_dir_path / 'hand_temp.tif'

                print("HAND thresh is:", params.hand_threshold)

                # Pre-processing HAND data
                if float(params.hand_threshold) > 0:
                    try:
                        print("Params passed: ")
                        print("-input file: ", water_tif_input)
                        print("-input_HAND_file: ", params.hand_file_tif_path)
                        print("-output_HAND_file: ", hand_temp)

                        preprocess_filter(water_tif_input, params.hand_file_tif_path, hand_temp)

                    except:
                        raise AfriCultuReSError('Error pre-processsing HAND data')

                # Clean water data
                try:
                    apply_filter(
                        water_tif_input=water_tif_input,
                        temp_hand_file_path=hand_temp,
                        water2_tif_output=water2_tif_output,
                        dem_thresh=params.dem_threshold,
                        hand_thresh=params.hand_threshold
                    )
                except:
                    raise AfriCultuReSError('Error cleaning water data......')

        def mosaic(in_files: List[Path], output_file: Path) -> Path:
            from rasterio.merge import merge
            merge(datasets=in_files, dst_path=output_file, nodata=11, method='min')

            return output_file

        print("\nProcessing date: ", date_str)

        with TemporaryDirectory(suffix='_mosaic_bag') as mosaic_bag:
            mosaic_bag_path = Path(mosaic_bag)

            sentinel_zip_file: Path
            for sentinel_zip_file in params.sentinel_zip_files_path_list:
                log_file = params.archive_folder / (sentinel_zip_file.stem + '_output.txt')
                with TemporaryDirectory(suffix='_' + sentinel_zip_file.name) as temp_dir:
                    temp_dir_path = Path(temp_dir)

                    write_line_to_file(file_path=log_file, token="Processing file: %s" % sentinel_zip_file, echo=True)

                    sigma_db = temp_dir_path.joinpath(sentinel_zip_file.stem + '_sigma_dB.tif')  # temp
                    water_tif = temp_dir_path.joinpath(sentinel_zip_file.stem + '_water.tif')  # temp

                    water2_tif = mosaic_bag_path.joinpath(sentinel_zip_file.stem + '_water2.tif')  # will be mosaic'ed

                    print("\nPre-processing Sentinel-1...")

                    from eo_engine.common import s06p01
                    ops_files = dict()
                    ops_files['orb'] = {"input": sentinel_zip_file.as_posix(),
                                        "output": temp_dir_path / (sentinel_zip_file.stem + '_orb')}

                    ops_files['brd'] = {"input": ops_files['orb']['output'].with_suffix('.dim'),
                                        "output": temp_dir_path / (sentinel_zip_file.stem + '_orb_brd')}
                    #
                    ops_files['the'] = {"input": ops_files['brd']['output'].with_suffix('.dim'),
                                        "output": temp_dir_path / (sentinel_zip_file.stem + '_orb_brd_the')}
                    #
                    ops_files['cal'] = {"input": ops_files['the']['output'].with_suffix('.dim'),
                                        "output": temp_dir_path / (sentinel_zip_file.stem + '_orb_brd_the_cal')}
                    ops_files['tc'] = {"input": ops_files['cal']['output'].with_suffix('.dim'),
                                       "output": temp_dir_path / (sentinel_zip_file.stem + '_orb_brd_the_cal_tc')}
                    ops_files['spk'] = {"input": ops_files['tc']['output'].with_suffix('.dim'),
                                        "output": temp_dir_path / (sentinel_zip_file.stem + '_orb_brd_the_cal_tc_spk')}
                    ops_files['db'] = {"input": ops_files['spk']['output'].with_suffix('.dim'),
                                       "output": sigma_db}
                    # all this fun for the sigma_db...
                    for opt in ['orb',
                                'brd',
                                'the',
                                'cal',
                                'tc',
                                'spk',
                                'db']:
                        check_file_exists(Path(ops_files[opt]['input']))

                        subprocess.run([
                            'python',
                            Path(s06p01.__file__).as_posix(),
                            ops_files[opt]['input'],
                            ops_files[opt]['output'],
                            opt
                        ], check=True)

                    # check that sigma_db was made
                    check_file_exists(sigma_db)

                    print("Finding the threshold...")
                    thresh_process(sigma_db=sigma_db, water_tif=water_tif, log_file=log_file)

                    print("Cleaning the raster using HAND data...")
                    clean_process(water_tif_input=water_tif, water2_tif_output=water2_tif,
                                  )

            # Mosaic the produced files
            water_files: List[Path] = list(mosaic_bag_path.glob('*water2.tif'))

            print("\nMosaicing files: %s" % water_files)
            temp_file = Path(mosaic_bag_path / 'mosaic_water2.tif')
            mosaic(in_files=water_files,
                   output_file=temp_file
                   )

            print('cutting/warping')
            final_output = output_folder / f'{date_str}_SE1_KZN_0010m_0006_WBMA.tif'
            subprocess.run(
                ['gdalwarp',
                 '-cutline', params.south_africa_shapefile.as_posix(),
                 '-co', 'COMPRESS=LZW',
                 temp_file.as_posix(),
                 final_output.as_posix()
                 ], check=True)

            return final_output

    with TemporaryDirectory('_output') as temp_output_folder:
        output_folder = Path(temp_output_folder)

        result_path = _process(output_folder=output_folder)

        logger.info(f'Returned: result_path: {result_path}')
        content = File(result_path.open('rb'))
        for conn in connections.all():
            conn.close_if_unusable_or_obsolete()
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.READY
        eo_product.datetime_creation = now
        eo_product.save()


@shared_task
def task_s06p01_wb10m_bag(eo_product_pk: int):
    raise AfriCultuReSError() from NotImplementedError('Not done yet!')


@shared_task
def task_s06p01_wb100m(eo_product_pk: int, aoi_wkt: str):
    import os
    import snappy
    from snappy import ProductIO, GPF, HashMap, WKTReader

    eo_product = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=eo_product.reference_date)
    input_file = input_files_qs.get()
    # any of the above eo_product, should point to the same eo_source

    HashMap = snappy.jpy.get_type('java.util.HashMap')
    # Get snappy Operators
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

    def clip(data, file_in: Path, dir_out, geom):
        params = HashMap()
        # params.put('sourceBands', 'WB') #'QUAL' # no band selected here. thus all bands are kept
        params.put('region', '0, 0, 0, 0')
        params.put('geoRegion', geom)
        params.put('subSamplingX', 1)
        params.put('subSamplingY', 1)
        params.put('fullSwath', False)
        params.put('copyMetadata', True)
        clipped = GPF.createProduct('Subset', params, data)

        filename = 'out.nc'
        ProductIO.writeProduct(clipped, os.path.join(dir_out, filename), 'NetCDF4-CF')  # 'GeoTIFF'
        return Path(str(clipped.getFileLocation()))

    # order is TUN, RWA, ETH, MOZ, ZAF, GHA, NER, KEN

    # product_list defined in the outer scope
    with TemporaryDirectory(prefix='task_s0601_wb_100m_') as temp_dir:
        data = ProductIO.readProduct(input_file.file.path)
        geom = WKTReader().read(aoi_wkt)
        file_in = Path(input_file.file.path)
        clipped: Path = clip(data=data, file_in=file_in, dir_out=temp_dir, geom=geom)

        content = File(clipped.open('rb'))
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.READY
        eo_product.datetime_creation = now
        eo_product.save()

    return 0


######
# S06P04
@shared_task
def task_s06p04_et3km(eo_product_pk: int):
    from eo_engine.common.contrib.h5georef import H5Georef
    import bz2

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    produced_file = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = produced_file.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=produced_file.reference_date)
    # only has one input
    eo_source_input: EOSource = input_files_qs.get()

    hdf5File = eo_source_input.file.path
    with NamedTemporaryFile() as dest, \
            NamedTemporaryFile() as final_file:
        logger.info('Processing file: %s' % hdf5File)

        dest.write(bz2.decompress(eo_source_input.file.read()))
        dest.flush()

        h5g = H5Georef(Path(dest.name))
        logger.info("\nCreating sample points ...")
        samples = h5g.get_sample_coords()
        logger.info("%s" % [s for s in samples])

        logger.info("\nGeoreferencing...")
        georef_file = h5g.georef_gtif(samples)

        if georef_file != -1:
            logger.info("\nProjecting...")
            warped_file = h5g.warp('EPSG:4326')

            if warped_file != -1:
                logger.info("\nConverting to netcdf...")
                # file_nc = "" + outDirName + "/" + file[5:-8] + ".nc"
                netcdf_file = h5g.to_netcdf(Path(final_file.name))
        try:
            cp = subprocess.run(['ncatted',
                                 '-a', 'short_name,Band1,o,c,Daily_ET',
                                 '-a', "long_name,Band1,o,c,Daily_Evapotranspiration_3km",
                                 '-a', "units,Band1,o,c,[mm/day]",
                                 #  '-a', "scale_factor,Band1,o,d,1000", # if enabled the scale_factor is applied twice reading the netcdf file
                                 #  '-a', "add_offset,Band1,o,d,0",
                                 '-a', "missing_value,Band1,o,d,-0.001",
                                 '-a', "_FillValue,Band1,o,d,-0.001",
                                 final_file.name], check=True)
        except BaseException as e:
            raise e

        content = File(final_file)
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.READY
        eo_product.datetime_creation = now
        eo_product.save()


@shared_task
def task_s06p04_etanom5km(eo_product_pk: int):
    """
    # Description of task:
    The task consists in clipping the ET anomaly file(SSEBop v5) to Africa and changing the metadata.
    Clipping is done with GDAL warp as proposed by Nikos.
    Metadata is edited using NCO as proposed by Nikos.

    # Data download
    The dataset is available here for v5:
    https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/global/monthly/etav5/anomaly/downloads

    # Script execution
    1. Main_SSEBopv5_1.py

    # Contact Information:
    **Contact**: icherif@yahoo.com
    **Date creation:** 10-08-2021

    """
    from zipfile import ZipFile

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    produced_file = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = produced_file.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=produced_file.reference_date)
    # only has one input
    eo_source_input: EOSource = input_files_qs.get()

    def clip(file_in: Path, file_out: Path):
        logger.info('Clip the file using GDAL')
        target_resolution = 0.009651999920606611424
        xmin, ymin, xmax, ymax = -20.0084493160248371, -38.0030921809375428, 55.0068940669297461, 40.0043711774050763
        subprocess.run([
            'gdalwarp',
            '-r', 'average', '-overwrite',
            '-tr', f'{target_resolution}', f'{target_resolution}',
            '-te', f'{xmin}', f'{ymin}', f'{xmax}', f'{ymax}',
            file_in.as_posix(), file_out.as_posix()],
            check=True)

    with TemporaryDirectory() as temp_dir, \
            ZipFile(eo_source_input.file.path) as zip_file:
        filename = eo_source_input.filename
        temp_dir_path = Path(temp_dir)
        logger.info(f'Extracting {filename} to TempDir...')
        zip_file.extractall(temp_dir)

        f_new_tif = next(temp_dir_path.glob('*.tif'))
        f_new_nc = temp_dir_path / 'temp_out.nc'

        clip(f_new_tif, f_new_nc)
        subprocess.run(['ncrename',
                        '-v', 'Band1,ETanom',
                        f_new_nc], check=True)
        subprocess.run(['ncatted',
                        '-a', "long_name,ETanom,o,c,Monthly_Evapotranspiration_Anomaly",
                        '-a', "Unit,ETanom,o,c,[%]",
                        f_new_nc], check=True)

        with open(f_new_nc, 'rb') as file_handler:
            content = File(file_handler)
            eo_product.file.save(name=eo_product.filename, content=content, save=False)
            eo_product.state = EOProductStateChoices.READY
            eo_product.datetime_creation = now
            eo_product.save()

        return '+++Finished+++'


@shared_task
def task_s06p04_et250m(eo_product_pk: int, iso: str):
    import rasterio
    import numpy as np

    africa_borders_buff10km = Path("/aux_files/Border_shp_with_buff/africa_borders_buff10km.shp")
    check_file_exists(africa_borders_buff10km)
    eo_product = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.all()
    input_files_qs = EOSource.objects.filter(
        group__in=input_eo_source_group,
        reference_date=eo_product.reference_date)

    et_file = input_files_qs.filter(
        group=EOSourceGroup.objects.get(
            name=EOSourceGroupChoices.S06P04_WAPOR_L1_AETI_D_AFRICA)).get()
    qual_ndvi_file = input_files_qs.filter(
        group=EOSourceGroup.objects.get(
            name=EOSourceGroupChoices.S06P04_WAPOR_L1_QUAL_NDVI_D_AFRICA)).get()
    qual_lst_file = input_files_qs.filter(
        group=EOSourceGroup.objects.get(
            name=EOSourceGroupChoices.S06P04_WAPOR_L1_QUAL_LST_D_AFRICA)).get()
    logger.info(
        f'Genereting {eo_product.filename}. Input files: [{",".join([et_file.filename, qual_ndvi_file.filename, qual_lst_file.filename])}]')

    def get_aeti_qual(file_in_path_et: Path,
                      file_in_path_lst: Path,
                      file_in_path_ndvi: Path,
                      file_out_path: Path):

        et_band = rasterio.open(file_in_path_et)
        ndvi_band = rasterio.open(file_in_path_ndvi)
        lst_band = rasterio.open(file_in_path_lst)
        logger.info('Reading Rasters.')
        logger.info(f'Reading {file_in_path_et.name}.')
        et = et_band.read(1)
        logger.info(f'Reading {file_in_path_ndvi.name}.')
        ndvi = ndvi_band.read(1)
        logger.info(f'Reading {file_in_path_lst.name}.')
        lst = lst_band.read(1)

        check1 = np.logical_or(ndvi <= 10, ndvi == 250)
        check2 = np.logical_and(lst == 0, check1)

        et_ql = np.where(check2, et, -9999)  # Exclude pixels

        logger.info('Releasing memory')
        et = None
        ndvi = None
        lst = None
        logger.info('Stats for et_ql')
        logger.info(f' np.max(et_ql) == -9999: {np.max(et_ql) == -9999}')
        logger.info(f' np.min(et_ql)-> {np.min(et_ql)}')
        logger.info(f' np.max(et_ql)-> {np.max(et_ql)}')
        if np.max(et_ql) == -9999:
            raise AfriCultuReSError(f"No file was produced because  np.max(et_ql) == -9999 -> {np.max(et_ql) == -9999}")
        else:
            et_meta = et_band.meta.copy()
            et_meta.update(
                {'crs': rasterio.crs.CRS({'init': 'epsg:4326'})})

            logger.info('Writing output file....')
            with rasterio.open(
                    file_out_path, 'w',
                    compress='lzw',
                    **et_meta) as file:
                file.write(et_ql, 1)

    file_in_path_et = Path(et_file.file.path)
    check_file_exists(file_in_path_et)
    file_in_path_lst = Path(qual_lst_file.file.path)
    check_file_exists(file_in_path_lst)
    file_in_path_ndvi = Path(qual_ndvi_file.file.path)
    check_file_exists(file_in_path_ndvi)
    with TemporaryDirectory() as temp_dir:
        # this process needs temp files.
        # These will be GC'ed at the end.

        temp_dir_path = Path(temp_dir)

        file1_tif = temp_dir_path / 'out1.tif'
        file2_tif = temp_dir_path / 'out2.tif'
        file3_nc = temp_dir_path / 'out3.nc'
        file3_final_nc = temp_dir_path / 'final.nc'

        get_aeti_qual(file_in_path_et=file_in_path_et, file_in_path_lst=file_in_path_lst,
                      file_in_path_ndvi=file_in_path_ndvi, file_out_path=file1_tif)

        logger.info('cutting/warping')
        subprocess.run(['gdalwarp',
                        '-cutline', africa_borders_buff10km.as_posix(),
                        '-co', 'COMPRESS=LZW',
                        file1_tif.as_posix(),
                        file2_tif.as_posix()], check=True)

        logger.info('translating')
        subprocess.run([
            'gdal_translate',
            '-of', 'netCDF',
            file2_tif.as_posix(),
            file3_nc.as_posix()
        ], check=True)

        logger.info('Running NCCopy')
        subprocess.run([
            'nccopy',
            '-k', 'nc4',
            '-d8',
            file3_nc.as_posix(),
            file3_final_nc.as_posix()
        ], check=True)

        logger.info(f'Running ncatted for Metadata editing:{file3_final_nc.name},')
        logger.info(f'File: {file3_final_nc.name}, exists: {file3_final_nc.exists()}')
        subprocess.run([
            'ncatted',
            '-a', "scale_factor,Band1,o,d,0.1",
            file3_final_nc.as_posix()
        ], check=True)

        with open((Path(temp_dir) / 'final.nc').as_posix(), 'rb') as file_handler:
            content = File(file_handler)
            eo_product.file.save(name=eo_product.filename, content=content, save=False)
            eo_product.state = EOProductStateChoices.READY
            eo_product.datetime_creation = now
            eo_product.save()

    return '+++Finished+++'


# noinspection SpellCheckingInspection
@shared_task
def task_s06p04_et100m(eo_product_pk: int, iso: str):
    import rasterio
    import numpy as np

    if iso == 'ETH':
        border_shp = Path("/aux_files/Border_shp_with_buff/ETH_adm0_buff10km.shp")
    elif iso == "GHA":
        border_shp = Path("/aux_files/Border_shp_with_buff/GH_adm0_buff10km.shp")
    elif iso == "RWA":
        border_shp = Path("/aux_files/Border_shp_with_buff/RW_adm0_buff10km.shp")
    elif iso == "MOZ":
        border_shp = Path("/aux_files/Border_shp_with_buff/MOZ_adm0_buff10km.shp")
    elif iso == "KEN":
        border_shp = Path("/aux_files/Border_shp_with_buff/KE_adm0_buff10km.shp")
    elif iso == 'TUN':
        border_shp = Path("/aux_files/Border_shp_with_buff/TUN_adm0_buff10km.shp")
    elif iso == 'NER':
        border_shp = Path("/aux_files/Border_shp_with_buff/NER_adm0_buff10km.shp")
    elif iso == 'ZAF':
        border_shp = Path("/aux_files/Border_shp_with_buff/ZAF_adm0_buff10km.shp")
    else:
        raise AfriCultuReSError(f'Unknown iso: {iso}')

    assert border_shp.exists()

    eo_product = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.all()
    input_files_qs = EOSource.objects.filter(
        group__in=input_eo_source_group,
        reference_date=eo_product.reference_date)

    et_file = input_files_qs.filter(
        group=EOSourceGroup.objects.get(
            name=f'S06P04_WAPOR_L2_AETI_D_{iso}')).get()
    qual_ndvi_file = input_files_qs.filter(
        group=EOSourceGroup.objects.get(
            name=f'S06P04_WAPOR_L2_QUAL_NDVI_D_{iso}')).get()
    qual_lst_file = input_files_qs.filter(
        group=EOSourceGroup.objects.get(
            name=f'S06P04_WAPOR_L2_QUAL_LST_D_{iso}')).get()

    def get_aeti_qual(file_in_path_et: Path,
                      file_in_path_lst: Path,
                      file_in_path_ndvi: Path,
                      file_out_path: Path):

        et_band = rasterio.open(file_in_path_et)
        ndvi_band = rasterio.open(file_in_path_ndvi)
        lst_band = rasterio.open(file_in_path_lst)

        et = et_band.read(1).astype(rasterio.int16)
        ndvi = ndvi_band.read(1).astype(rasterio.int16)
        lst = lst_band.read(1).astype(rasterio.int16)

        check1 = np.logical_or(ndvi <= 10, ndvi == 250)
        check2 = np.logical_and(lst == 0, check1)

        et_ql = np.where(check2, et, -9999)  # Exclude pixels

        if np.max(et_ql) == -9999:
            raise AfriCultuReSError("No file was produced")
        else:
            et_meta = et_band.meta.copy()
            # noinspection PyUnresolvedReferences
            et_meta.update({'crs': rasterio.crs.CRS({'init': 'epsg:4326'})})

            print('Writing output file....')
            with rasterio.open(file_out_path, 'w', compress='lzw', **et_meta) as file:
                file.write(et_ql, 1)

    file_in_path_et = Path(et_file.file.path)
    file_in_path_lst = Path(qual_lst_file.file.path)
    file_in_path_ndvi = Path(qual_ndvi_file.file.path)
    with TemporaryDirectory() as temp_dir:
        get_aeti_qual(file_in_path_et=file_in_path_et, file_in_path_lst=file_in_path_lst,
                      file_in_path_ndvi=file_in_path_ndvi, file_out_path=Path(temp_dir) / 'out1.tif')

        logger.info('Cutting/Warping')
        subprocess.run(['gdalwarp',
                        '-cutline', border_shp.as_posix(),
                        '-co', 'COMPRESS=LZW',
                        (Path(temp_dir) / 'out1.tif').as_posix(),
                        (Path(temp_dir) / 'out2.tif').as_posix()], check=True)

        logger.info('Translating')
        subprocess.run([
            'gdal_translate',
            '-of', 'netCDF',
            (Path(temp_dir) / 'out2.tif').as_posix(),
            (Path(temp_dir) / 'final.nc').as_posix()], check=True)

        # set outfile metadata
        logger.info('Adding Metadata')
        cp = subprocess.run(['ncatted',
                             '-a', "scale_factor,Band1,o,d,0.1",
                             (Path(temp_dir) / 'final.nc').as_posix()], check=True)
        with open((Path(temp_dir) / 'final.nc').as_posix(), 'rb') as file_handler:
            content = File(file_handler)
            eo_product.file.save(name=eo_product.filename, content=content, save=False)
            eo_product.state = EOProductStateChoices.READY
            eo_product.datetime_creation = now
            eo_product.save()


######
# DEBUG TASKS
# Add the debug in the name

@shared_task
def task_debug_add(x: int, y: int) -> int:
    return x + y


@shared_task
def task_debug_append_char(token: str) -> str:
    import string
    from random import choice
    new_char = choice(string.ascii_letters)
    print(f'Appending {new_char} to {token}.')
    return token + new_char


@shared_task
def task_debug_failing(wait_time: Optional[int] = None):
    if wait_time is None:
        wait_time = 2
    import time
    time.sleep(wait_time)

    logger.info('About to throw exception!')
    raise Exception('Expected-Error')
