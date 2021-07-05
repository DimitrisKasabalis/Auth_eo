import tempfile
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryFile
from typing import Optional, Tuple, List

from celery import group, Task
from celery.app import shared_task
from celery.utils.log import get_task_logger
from django.core.files import File
from django.utils import timezone
from osgeo import gdal
from pytz import utc

from eo_engine.models import EOProduct, EOProductStatusChoices
from eo_engine.models import (EOSource, EOSourceStatusChoices)

logger = get_task_logger(__name__)


def random_name_gen(length=10) -> str:
    import random
    import string
    return ''.join(random.choice(string.ascii_uppercase) for i in range(10))


@shared_task(bind=True)
def task_schedule_download(self, params: Tuple[Tuple[str, str]]):
    qs = EOSource.objects.none()
    for prod, start_date in params:
        start_date = datetime.strptime(start_date, '%d/%m/%Y').replace(tzinfo=utc)
        qs |= EOSource.objects.filter(status=EOSourceStatusChoices.availableRemotely,
                                      product=prod,
                                      datetime_reference__gte=start_date)

    if qs.exists():
        job = group(task_download_file.s(filename=eo_source.filename) for eo_source in qs)
        qs.update(status=EOSourceStatusChoices.scheduledForDownload)
        return job.apply_async()


@shared_task(bind=True)
def task_download_file(self, filename: str, force_dl=False):
    """
    Download a remote asset identified by it's ID number.
    if it's already available locally, set force_dl=True to download again.

    """
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


@shared_task(bind=True)
def task_fetch_n_files(self: Task, n: int = 1, product: str = None) -> Optional[str]:
    """
    Download n number of known files.

    :param self: Bound tasks
    :param n: number of files
    :param product: Type of product
    :return:
    """

    product = product.lower()

    eo_source_qs = EOSource.objects.filter(status=EOSourceStatusChoices.availableRemotely)
    if product:
        if product not in EOSourceProductChoices:
            raise ValueError(f"unknown product: {product}")

        eo_source_qs = eo_source_qs.filter(product=product)

    # Queryset can only be sliced at the end
    eo_source_qs = eo_source_qs[:n]

    if not eo_source_qs.exists():  # empty qs
        logger.info(f"No files to fetch: n: {n}, product: {product}")
        return

    filenames = eo_source_qs.values_list('filename', flat=True)

    task = group(task_download_file.s(filename=filename) for filename in filenames)
    result = task.apply_async()

    return result.id


@shared_task(bind=True)
def task_s02p02_ndvi_vci_clip_ndvi(self, output_pk: int, aoi: List[int], **kwargs):
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
    output: EOProduct = EOProduct.objects.get(pk=output_pk)
    input: EOSource = output.inputs.first()
    ds = xr.open_dataset(input.file.path, mask_and_scale=False)
    da = ds.NDVI
    adj_ext = bnd_box_adj(aoi)
    da = da.sel(lon=slice(adj_ext[0], adj_ext[2]), lat=slice(adj_ext[1], adj_ext[3]))
    try:
        prmts = {param['product']: {'dtype': 'int32', 'zlib': 'True', 'complevel': 4}}
        name = param['product']  # 'NDVI'
        date_str = _date_extr(input.filename)
        # file_name = f'CGLS_{name}_{date}_300m_Africa.nc'
        with tempfile.TemporaryDirectory() as tmp_dir:
            file = Path(tmp_dir) / f'tmp.nc'

            da.to_netcdf(file, encoding=prmts)  # hmm... maybe it will take too much time, and our conn will die?
            content = File(file.open('rb'))
            output.file.save(name=output.filename, content=content, save=False)
            output.filesize = output.file.size
            output.status = EOProductStatusChoices.Ready
        print(f'removing temp file {file.name}')
        file.unlink(missing_ok=True)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        raise ex

    output.status = EOProductStatusChoices.Ready
    output.save()
    return output.filename


# @shared_task()
# def task_translate_h5_to_geotiff(src: Union[str, Path]) -> str:
#     src_path = Path(src)
#     if src_path.suffix == '.zip':  # if it's zip'ed, use the zipdriver to access the data directly
#         zipfile = ZipFile(src_path)
#         zip_path = list(filter(lambda x: str(x).endswith('.h5'), zipfile.namelist()))[
#             0]  # path of h5 inside the zip file
#         uri = f"/vsizip/{src_path.as_posix()}/{zip_path}"  # construct a uri path
#     elif src_path.suffix == '.h5':
#         uri = f"{src_path.as_posix()}"
#     else:
#         raise ValueError('Unrecognised Error')
#
#     optionsTiffParams = {
#         "format": "GTiff",
#         "outputBounds": [-30, 40, 60, -40],
#         "outputSRS": "EPSG: 4326",
#         "metadataOptions": "all"
#     }
#
#     optionsTiff: GDALTranslateOptions = gdal.TranslateOptions(**optionsTiffParams)
#
#     dst_path = src_path.with_suffix('.tif')
#
#     srcDS = gdal.Open(uri)
#     if srcDS is None:
#         raise Exception
#
#     result: gdal.Dataset = gdal.Translate(srcDS=srcDS, destName=dst_path.as_posix(), options=optionsTiff)
#     filepath = result.GetFileList()[0]
#
#     return Path(filepath).as_posix()


# @shared_task(bind=True)
# def task_warp_geotiff_to_nc(self, src: str) -> str:
#     DEFAULT_WARP_PARAMS = {
#         "format": "netCDF",
#         "copyMetadata": "all"
#     }


@shared_task(bind=True)
def task_c_gls_nvdi_resample_from_300m_to_1km(self, product_pk, aoi):
    """" Resamples to 1km and cuts to AOI bbox"""

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


@shared_task(bind=True)
def task_nc_to_geotiff(self, src: str):
    src_path = Path(src)
    src_name = src_path.name
    dst_filename = src_name.split(".")[0] + ".tif"
    dst_path = Path(src) / "processed" / f"{self.name}" / dst_filename
    optionsTiff = gdal.TranslateOptions(format="GTiff", outputBounds=[-30, 40, 60, -40], outputSRS="EPSG: 4326",
                                        metadataOptions="all")
    ds = gdal.Open(src_path.as_posix())
    if ds is None:
        raise ValueError(f'Could not open file: {src_path.as_posix()}')

    res_ds = gdal.Translate(destName=dst_path.as_posix(), srcDS=ds, options=optionsTiff)
    res_filepath = res_ds.GetFileList()[0]
    return Path(res_filepath).as_posix()


@shared_task(bind=True)
def task_start_scrape(self, spider_name):
    from scrapy.spiderloader import SpiderLoader
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    scrapy_settings = get_project_settings()  # requires SCRAPY_SETTINGS_MODULE env variable

    spider_loader = SpiderLoader.from_settings(scrapy_settings)
    spider = spider_loader.load(spider_name)
    print(spider)

    process = CrawlerProcess(scrapy_settings)
    process.crawl(spider)
    #
    # # block until the crawling is finished
    process.start()

    return


@shared_task
def task_generate_daily_report():
    # generate report
    # email report
    return 'Done!'


@shared_task
def task_add(x: int, y: int) -> int:
    return x + y


@shared_task
def task_append_char(token: str) -> str:
    import string
    from random import choice
    new_char = choice(string.ascii_letters)
    print(f'Appending {new_char} to {token}.')
    return token + new_char


__all__ = [

]
