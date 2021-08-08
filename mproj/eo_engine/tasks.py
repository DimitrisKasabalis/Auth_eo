import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Union

import more_itertools
from celery import group
from celery.app import shared_task
from celery.utils.log import get_task_logger
from django.core.files import File
from django.utils import timezone
from pytz import utc

from eo_engine.common import get_task_ref_from_name
from eo_engine.models import EOProduct, EOProductStatusChoices, FunctionalRules
from eo_engine.models import (EOSource, EOSourceStatusChoices)

logger = get_task_logger(__name__)

now = timezone.now()


def random_name_gen(length=10) -> str:
    import random
    import string
    return ''.join(random.choice(string.ascii_uppercase) for i in range(10))


@shared_task
def task_init_spider(spider_name):
    from scrapy.spiderloader import SpiderLoader
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    # requires SCRAPY_SETTINGS_MODULE env variable
    # currently it's set in DJ's manage.py
    scrapy_settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(scrapy_settings)
    spider = spider_loader.load(spider_name)
    print(scrapy_settings)
    process = CrawlerProcess(scrapy_settings)
    process.crawl(spider)

    # # block until the crawling is finished
    process.start()

    return "Spider {} finished crawling".format(spider_name)


@shared_task(bind=True)
def task_schedule_download_eosource(self):
    # Entry point for downloading products.
    #
    # check the rules entry on FunctionalRules Table

    qs = EOSource.objects.none()

    # rules: eg, start_date for this prod group
    download_rules = FunctionalRules.objects.get(domain='download_rules').rules

    for rule in download_rules['rules']:
        for prod, start_date in more_itertools.chunked(rule.values(), 2):  # two elements in every rule entry
            qs |= EOSource.objects.filter(status=EOSourceStatusChoices.availableRemotely,
                                          product=prod,
                                          datetime_reference__gte=datetime.strptime(start_date, '%d/%m/%Y').replace(
                                              tzinfo=utc))

    if qs.exists():
        job = group(task_download_file.s(filename=eo_source.filename) for eo_source in qs)
        qs.update(status=EOSourceStatusChoices.scheduledForDownload)
        return job.apply_async()


# noinspection SpellCheckingInspection
@shared_task
def task_schedule_create_eoproduct():
    qs = EOProduct.objects.none()

    qs |= EOProduct.objects.filter(status=EOProductStatusChoices.Available)
    if qs.exists():
        logger.info(f'Found {qs.count()} EOProducts that are ready')

        job = group(
            get_task_ref_from_name(eo_product.task_name).s(eo_product_pk=eo_product.pk, **eo_product.task_kwargs) for
            eo_product in qs)
        qs.update(status=EOProductStatusChoices.Scheduled)
        return job.apply_async()

    return


@shared_task(bind=True)
def task_download_file(self, filename: str, force_dl=False):
    """
    Download a remote asset identified by it's ID number.
    if it's already available locally, set force_dl=True to download again.

    """
    logger.info(f'downloading file {filename}')
    eo_source = EOSource.objects.get(filename=filename)

    if force_dl and eo_source.local_file_exists:
        print(f"File {eo_source.filename} file found at directory {eo_source.local_path}. Skipping")
        return eo_source.filename
    try:
        eo_source.download()

        if eo_source.file.size == 0:
            logger.warn(f"warning: file {eo_source.filename} has really filesize of 0.")
    except Exception as e:
        eo_source.status = EOSourceStatusChoices.availableRemotely
        raise e

    return eo_source.filename  # return the name of the file, ckts as PK


#############################
# TASKS

# rules
# tasks that make products must start with 'task_s??p??*


@shared_task
def task_s02p02_c_gls_ndvi_300_clip(eo_product_pk: Union[int, EOProduct], aoi: List[int]):
    # Preamble
    if isinstance(eo_product_pk, EOProduct):
        eo_product = eo_product_pk
    else:
        eo_product: EOProduct = EOProduct.objects.get(pk=eo_product_pk)
    input: EOSource = eo_product.eo_sources_inputs.first()

    import xarray as xr
    import numpy as np

    def find_nearest(array, value):
        array = np.asarray(array)
        idx = (np.abs(array - value)).argmin()
        return array[idx]

    def bnd_box_adj(my_ext: [int, int, int, int]):

        lat_1k = np.round(np.arange(80., -60., -1. / 112), 8)
        lon_1k = np.round(np.arange(-180., 180., 1. / 112), 8)

        lat_300 = ds.lat.values
        lon_300 = ds.lon.values
        ext_1k = np.zeros(4)

        # UPL Long 1K
        ext_1k[0] = find_nearest(lon_1k, my_ext[0]) - 1. / 336
        # UPL Lat 1K
        ext_1k[1] = find_nearest(lat_1k, my_ext[1]) + 1. / 336

        # LOWR Long 1K
        ext_1k[2] = find_nearest(lon_1k, my_ext[2]) + 1. / 336
        # LOWR Lat 1K
        ext_1k[3] = find_nearest(lat_1k, my_ext[3]) - 1. / 336

        # UPL
        my_ext[0] = find_nearest(lon_300, ext_1k[0])
        my_ext[1] = find_nearest(lat_300, ext_1k[1])

        # LOWR
        my_ext[2] = find_nearest(lon_300, ext_1k[2])
        my_ext[3] = find_nearest(lat_300, ext_1k[3])
        return my_ext

    def _date_extr(name: str):

        pos = [pos for pos, char in enumerate(name) if char == '_'][2]
        date_str = name[pos + 1: pos + 9]
        return date_str

    param = {'product': 'NDVI',
             'short_name': 'Normalized_difference_vegetation_index',
             'long_name': 'Normalized Difference Vegetation Index Resampled 1 Km',
             'grid_mapping': 'crs',
             'flag_meanings': 'Missing cloud snow sea background',
             'flag_values': '[251 252 253 254 255]',
             'units': '',
             'PHYSICAL_MIN': -0.08,
             'PHYSICAL_MAX': 0.92,
             'DIGITAL_MAX': 250,
             'SCALING': 1. / 250,
             'OFFSET': -0.08}

    ds = xr.open_dataset(input.file.path, mask_and_scale=False)
    da = ds.NDVI
    adj_ext = bnd_box_adj(aoi)
    da = da.sel(lon=slice(adj_ext[0], adj_ext[2]), lat=slice(adj_ext[1], adj_ext[3]))
    try:
        prmts = {param['product']: {'dtype': 'i4', 'zlib': 'True', 'complevel': 4}}
        name = param['product']  # 'NDVI'
        date_str = _date_extr(input.filename)
        # file_name = f'CGLS_{name}_{date}_300m_Africa.nc'
        with tempfile.TemporaryDirectory() as tmp_dir:
            file = Path(tmp_dir) / f'tmp.nc'
            da.to_netcdf(file, encoding=prmts)  # hmm... maybe it will take too much time, and our conn will die?
            content = File(file.open('rb'))
            eo_product.file.save(name=eo_product.filename, content=content, save=False)
            eo_product.filesize = eo_product.file.size
            eo_product.status = EOProductStatusChoices.Ready
            logger.debug(f'removing temp file {file.name}')
            file.unlink(missing_ok=True)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        raise ex

    eo_product.status = EOProductStatusChoices.Ready
    eo_product.datetime_creation = now
    eo_product.save()
    return eo_product.file.path


@shared_task
def task_s02p02_agro_nvdi_300_resample_to_1km(eo_product_pk):
    """" Resamples to 1km and cuts to AOI bbox """

    import subprocess
    import os
    eo_product = EOProduct.objects.get(id=eo_product_pk)

    target_resolution = 0.0089285714286
    xmin, ymin, xmax, ymax = -30.0044643, -40.0044643, 60.0066643, 40.0044643

    # Mark it as 'in process'
    eo_product.status = EOProductStatusChoices.Generating
    eo_product.save()
    # input file//eo_product
    input_obj: EOProduct = eo_product.eo_products_inputs.first()

    with tempfile.TemporaryDirectory() as tmp_dir:
        output_temp_file = f"{tmp_dir}/tmp_file.nc"
        cp = subprocess.run([
            'gdalwarp',
            '-r', 'average',
            '-tr', f'{target_resolution}', f'{target_resolution}',
            '-te', f'{xmin}', f'{ymin}', f'{xmax}', f'{ymax}',
            f'{input_obj.file.path}', output_temp_file
        ])

        if cp.returncode != 0:
            raise Exception(f'EXIT CODE: {cp.returncode}, ERROR: {cp.stderr} ')
        # metadata fine tuning using NCO tools
        # for usuage details see:
        # http://nco.sourceforge.net/nco.html#ncatted-netCDF-Attribute-Editor

        # Rename default Band1 to NDVI
        print('Rename Variable')
        cp = subprocess.run(['ncrename',
                             '-v', 'Band1,NDVI',
                             output_temp_file])
        if cp.returncode != 0:
            raise Exception(f'EXIT CODE: {cp.returncode}, ERROR: {cp.stderr} ')

        # ncatted is for nc attribute editor
        print('Editing metadata')
        cp = subprocess.run(['ncatted',
                             '-a', 'short_name,NDVI,o,c,normalized_difference_vegetation_index',
                             '-a', "long_name,NDVI,o,c,Normalized Difference Vegetation Index Resampled 1 Km",

                             output_temp_file])
        if cp.returncode != 0:
            raise Exception(f'EXIT CODE: {cp.returncode}, ERROR: {cp.stderr} ')

        with open(output_temp_file, 'rb') as fh:
            content = File(fh)
            eo_product.file.save(name=eo_product.filename, content=content)
            eo_product.status = EOProductStatusChoices.Ready
            eo_product.save()
        os.unlink(output_temp_file)

    return


######
# DEBUG TASKS

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
