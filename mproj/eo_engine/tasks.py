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
from osgeo import gdal
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
    from mproj import celery_app as capp
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
    input: EOSource = eo_product.inputs.first()

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
def task_s02p02_agro_nvdi_300_resample_to_1km(self, product_pk, aoi):
    """" Resamples to 1km and cuts to AOI bbox """

    import xarray as xr
    import os
    import numpy as np
    import pandas as pd

    if aoi is None:
        aoi = [-30, 40, 60, -40]

    def _param(ds):
        if 'LAI' in ds.data_vars:
            param = {'product': 'LAI',
                     'short_name': 'leaf_area_index',
                     'long_name': 'Leaf Area Index Resampled 1 Km',
                     'grid_mapping': 'crs',
                     'flag_meanings': 'Missing',
                     'flag_values': '255',
                     'units': '',
                     'PHYSICAL_MIN': 0,
                     'PHYSICAL_MAX': 7,
                     'DIGITAL_MAX': 210,
                     'SCALING': 1. / 30,
                     'OFFSET': 0}
            da = ds.LAI

        elif 'FCOVER' in ds.data_vars:
            param = {'product': 'FCOVER',
                     'short_name': 'vegetation_area_fraction',
                     'long_name': 'Fraction of green Vegetation Cover Resampled 1 Km',
                     'grid_mapping': 'crs',
                     'flag_meanings': 'Missing',
                     'flag_values': '255',
                     'units': '',
                     'valid_range': '',
                     'PHYSICAL_MIN': 0,
                     'PHYSICAL_MAX': 1.,
                     'DIGITAL_MAX': 250,
                     'SCALING': 1. / 250,
                     'OFFSET': 0}
            da = ds.FCOVER

        elif 'FAPAR' in ds.data_vars:
            param = {'product': 'FAPAR',
                     'short_name': 'Fraction_of_Absorbed_Photosynthetically_Active_Radiation',
                     'long_name': 'Fraction of Absorbed Photosynthetically Active Radiation Resampled 1 KM',
                     'grid_mapping': 'crs',
                     'flag_meanings': 'Missing',
                     'flag_values': '255',
                     'units': '',
                     'valid_range': '',
                     'PHYSICAL_MIN': 0,
                     'PHYSICAL_MAX': 0.94,
                     'DIGITAL_MAX': 235,
                     'SCALING': 1. / 250,
                     'OFFSET': 0}
            da = ds.FAPAR

        elif 'NDVI' in ds.data_vars:
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
            da = ds.NDVI

        elif 'DMP' in ds.data_vars:
            param = {'product': 'DMP',
                     'short_name': 'dry_matter_productivity',
                     'long_name': 'Dry matter productivity Resampled 1KM',
                     'grid_mapping': 'crs',
                     'flag_meanings': 'sea',
                     'flag_values': '-2',
                     'units': 'kg / ha / day',
                     'PHYSICAL_MIN': 0,
                     'PHYSICAL_MAX': 327.67,
                     'DIGITAL_MAX': 32767,
                     'SCALING': 1. / 100,
                     'OFFSET': 0}
            da = ds.DMP

        elif 'GDMP' in ds.data_vars:
            param = {'product': 'GDMP',
                     'short_name': 'Gross_dry_matter_productivity',
                     'long_name': 'Gross dry matter productivity Resampled 1KM',
                     'grid_mapping': 'crs',
                     'flag_meanings': 'sea',
                     'flag_values': '-2',
                     'units': 'kg / hectare / day',
                     'PHYSICAL_MIN': 0,
                     'PHYSICAL_MAX': 655.34,
                     'DIGITAL_MAX': 32767,
                     'SCALING': 1. / 50,
                     'OFFSET': 0}
            da = ds.GDMP

        else:
            raise Exception('GLC product not found please chek')

        return da, param

    def _date_extr(path):
        _, tail = os.path.split(path)
        pos = [pos for pos, char in enumerate(tail) if char == '_'][2]
        date = tail[pos + 1: pos + 9]
        date_h = pd.to_datetime(date, format='%Y%m%d')
        return date, date_h

    def _aoi(da, ds, AOI):
        def find_nearest(array, value):
            array = np.asarray(array)
            idx = (np.abs(array - value)).argmin()
            return array[idx]

        def bnd_box_adj(my_ext):
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

        if len(AOI):
            assert AOI[0] <= AOI[2], 'min Longitude is bigger than correspond Max, ' \
                                     'pls change position or check values.'
            assert AOI[1] >= AOI[3], 'min Latitude is bigger than correspond Max, ' \
                                     'pls change position or check values.'
            assert ds.lon[0] <= AOI[0] <= ds.lon[-1], 'min Longitudinal value out of original dataset Max ext.'
            assert ds.lat[-1] <= AOI[1] <= ds.lat[0], 'Max Latitudinal value out of original dataset Max ext.'

            assert ds.lon[0] <= AOI[2] <= ds.lon[-1], 'Max Longitudinal value out of original dataset Max ext.'
            assert ds.lat[-1] <= AOI[3] <= ds.lat[0], 'min Latitudinal value out of original dataset Max ext.'

            adj_ext = bnd_box_adj(AOI)
            try:
                da = da.sel(lon=slice(adj_ext[0], adj_ext[2]), lat=slice(adj_ext[1], adj_ext[3]))
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                raise Exception(message)from ex

        else:
            da = da.shift(lat=1, lon=1)
        return da

    eo_product = EOProduct.objects.get(id=product_pk)

    # Mark it as 'in process'
    eo_product.status = EOProductStatusChoices.InProcess
    eo_product.save()

    # input file.
    eo_source = eo_product.inputs.first()

    # Load the dataset
    ds = xr.open_dataset(eo_source.local_path, mask_and_scale=False)

    # select parameters according to the product.
    da, param = _param(ds)
    date, date_h = _date_extr(
        eo_source.filename)  # there's a time dimension inside the data. It's is not equal with the time on the name

    # AOI
    da = _aoi(da, ds, aoi)

    # Algorithm core
    try:
        # create the mask according to the fixed values
        da_msk = da.where(da <= param['DIGITAL_MAX'])

        # create the coarsen dataset
        coarsen = da_msk.coarsen(lat=3, lon=3, boundary='trim', keep_attrs=False).mean()

        # force results to integer
        coarsen_int = np.rint(coarsen)

        # mask the dataset according to the minumum required values
        vo = xr.where(da <= param['DIGITAL_MAX'], 1, 0)
        vo_cnt = vo.coarsen(lat=3, lon=3, boundary='trim', keep_attrs=False).sum()
        da_r = coarsen_int.where(vo_cnt >= 5)

        # force nan to int
        da_r = xr.where(np.isnan(da_r), 255, coarsen_int)

        # Add time dimension
        print('Time is', date_h)

    #  da_r = da_r.assign_coords({'time': date_h})
    #  da_r = da_r.expand_dims(dim='time', axis=0)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        raise Exception(message) from ex

    # Output write
    try:
        da_r.name = param['product']
        da_r.attrs['short_name'] = param['short_name']
        da_r.attrs['long_name'] = param['long_name']
        da_r.attrs['_FillValue'] = int(255)
        da_r.attrs['scale_factor'] = np.float32(param['SCALING'])
        da_r.attrs['add_offset'] = np.float32(param['OFFSET'])

        prmts = dict({param['product']: {
            'dtype': 'i4',
            'zlib': 'True',
            'complevel': 4
        }
        })
        name = param['product']
        # if len(aoi) != 0:
        #     file_name = f'CGLS_{name}_{date}_1KM_Resampled_Africa.nc'
        # else:
        #     file_name = f'CGLS_{name}_{date}_1KM_Resampled_.nc'

        datetime_now = timezone.now()
        process = {"operation": self.name, "date": datetime_now}

        eo_product.process = process
        eo_product.datetime_creation = datetime_now
        with tempfile.TemporaryDirectory() as tmp_dir:
            file = Path(tmp_dir) / f'tmp_{eo_product.filename}'
            da_r.to_netcdf(file, encoding=prmts)
            with file.open('rb') as fh:
                content = File(fh)
                eo_product.file.save(name=eo_product.filename, content=content)
                eo_product.status = EOProductStatusChoices.READY
                eo_product.save()

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        raise Exception(message) from ex

    # print(f'{file_name} resampled')

    return {"rtype": 'algo', 'product_pk': product_pk}


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
