import os
import subprocess
import tempfile
from celery import group
from celery.app import shared_task
from celery.utils.log import get_task_logger
from django.core.files import File
from django.core.files.temp import NamedTemporaryFile
from django.utils import timezone
from more_itertools import collapse
from osgeo import gdal
from pathlib import Path
from scrapy import Spider
from subprocess import run, CompletedProcess
from tempfile import TemporaryDirectory
from typing import List, Union, Optional, Literal

from eo_engine.common.tasks import get_task_ref_from_name
from eo_engine.errors import AfriCultuReSRetriableError, AfriCultuReSError
from eo_engine.models import Credentials
from eo_engine.models import EOProduct, EOProductStateChoices
from eo_engine.models import EOSource, EOSourceStateChoices

logger = get_task_logger(__name__)

now = timezone.now()


def random_name_gen(length=10) -> str:
    import random
    import string
    return ''.join(random.choice(string.ascii_uppercase) for i in range(10))


# Notes:
#  task should use the kwargs eo_source_pk and eo_product_pk.
#  these kw are picked up to tie the task with the element


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
        logger.err(msg)
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


# utils
@shared_task(bind=True)
def create_wapor_entry(self, level_id: Literal['L1', 'L2'], product_id: Literal['AETI', 'QUAL_LST', 'QUAL_NDVI'],
                       dimension_id: Literal['D'], dimension_member: str, area_id: str = None):
    # don't try to be smart. Generate the filename per instructions
    # and let the factory do the job
    # L1_AETI_D_1904.tif
    # L1_QUAL_LST_D_1904.tif
    # L1_QUAL_NDVI_D_1904.tif

    # L2_AETI_D_1904_TUN.tif
    # L2_QUAL_LST_D_1904_TUN.tif
    # L2_QUAL_NDVI_D_1904_TUN.tif
    from .models.factories import create_wapor_object

    if area_id is None and level_id == "L2":
        raise AttributeError('L2 can not be choosen without and area')
    if area_id:
        wapor_filename = f'{level_id.upper()}_{product_id.upper()}_{dimension_id.upper()}_{dimension_member.upper()}_{area_id.upper()}.tif'
    else:
        wapor_filename = f'{level_id.upper()}_{product_id.upper()}_{dimension_id.upper()}_{dimension_member.upper()}.tif'

    logger.info(f'fun: {self.name}, generated filename: {wapor_filename}')
    create_wapor_object(wapor_filename)
    return


# SFTP
@shared_task
def task_sftp_parse_remote_dir(remote_dir: Union[str, List[str]]):
    from eo_engine.common.products import filename_to_product
    # remote dir is in the form of
    #  sftp://adf.adf.com/asdf/aa'
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
        product_group = filename_to_product(entry.filename)
        add_to_db(entry, product_group=product_group[0])


# @shared_task(bind=True)
def task_schedule_download_eosource(self):
    qs = EOSource.objects.filter(status=EOSourceStateChoices.AvailableRemotely)
    if qs.exists():
        qs.update(status=EOSourceStateChoices.ScheduledForDownload)
        job = group(task_download_file.s(eo_source_pk=eo_source.pk) for eo_source in qs.only('pk'))
        return job.apply_async()


# noinspection SpellCheckingInspection
@shared_task
def task_schedule_create_eoproduct():
    qs = EOProduct.objects.none()

    qs |= EOProduct.objects.filter(status=EOProductStateChoices.Available)
    if qs.exists():
        logger.info(f'Found {qs.count()} EOProducts that are ready')

        job = group(
            get_task_ref_from_name(eo_product.task_name).s(eo_product_pk=eo_product.pk, **eo_product.task_kwargs) for
            eo_product in qs)
        qs.update(status=EOProductStateChoices.Scheduled)
        return job.apply_async()

    return


@shared_task(bind=True, autoretry_for=(AfriCultuReSRetriableError,), max_retries=100)
def task_download_file(self, eo_source_pk: int):
    """
    Download a remote asset identified by it's ID number.
    if it's already available locally, set force_dl=True to download again.

    """
    from urllib.parse import urlparse
    from eo_engine.common.download import (download_http_eosource,
                                           download_ftp_eosource,
                                           download_sftp_eosource,
                                           download_wapor_eosource)
    eo_source = EOSource.objects.get(pk=eo_source_pk)
    eo_source.state = EOSourceStateChoices.BeingDownloaded
    eo_source.save()
    url_parse = urlparse(eo_source.url)
    scheme = url_parse.scheme

    logger.info(f'LOG:INFO: Downloading file {eo_source.filename} using scheme: {scheme}')

    try:
        if scheme.startswith('ftp'):
            return download_ftp_eosource(eo_source_pk)
        elif scheme.startswith('http'):
            return download_http_eosource(eo_source_pk)
        elif scheme.startswith('sftp'):
            return download_sftp_eosource(eo_source_pk)
        elif scheme.startswith('wapor'):
            return download_wapor_eosource(eo_source_pk)
        else:
            raise Exception(f'There was no defined method for scheme: {scheme}')
    except AfriCultuReSRetriableError as exc:
        eo_source.refresh_from_db()
        eo_source.state = EOSourceStateChoices.Defered
        eo_source.save()
        raise self.retry(countdown=10)
    except BaseException as e:
        eo_source.refresh_from_db()
        eo_source.state = EOSourceStateChoices.FailedToDownload
        eo_source.save()
        raise Exception('Could not download.') from e


#############################
# TASKS

# rules
# tasks that make products must start with 'task_s??p??*

###########
# s02p02
###########
@shared_task
def task_s02p02_c_gls_ndvi_300_clip(
        eo_product_pk: Union[int, EOProduct],
        aoi: List[int]
):
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
        with TemporaryDirectory() as tmp_dir:
            file = Path(tmp_dir) / f'tmp.nc'
            da.to_netcdf(file, encoding=prmts)  # hmm... maybe it will take too much time, and our conn will die?
            content = File(file.open('rb'))
            eo_product.file.save(name=eo_product.filename, content=content, save=False)
            eo_product.filesize = eo_product.file.size
            eo_product.state = EOProductStateChoices.Ready
            logger.debug(f'removing temp file {file.name}')
            file.unlink(missing_ok=True)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        raise ex

    eo_product.state = EOProductStateChoices.Ready
    eo_product.datetime_creation = now
    eo_product.save()
    return eo_product.file.path


@shared_task
def task_s02p02_agro_nvdi_300_resample_to_1km(eo_product_pk):
    """" Resamples to 1km and cuts to AOI bbox """

    eo_product = EOProduct.objects.get(id=eo_product_pk)

    target_resolution = 0.0089285714286
    xmin, ymin, xmax, ymax = -30.0044643, -40.0044643, 60.0066643, 40.0044643

    # Mark it as 'in process'
    eo_product.state = EOProductStateChoices.Generating
    eo_product.save()
    # input file//eo_product
    input_obj: EOProduct = eo_product.eo_products_inputs.first()

    with TemporaryDirectory() as tmp_dir:
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
            eo_product.state = EOProductStateChoices.Ready
            eo_product.datetime_creation = now
            eo_product.save()
        os.unlink(output_temp_file)

    return


@shared_task
def task_s02p02_cgls_compute_vci_1km_v2(eo_product_pk):
    # Processing of the resampled NDVI 1km v3 (clipped to Africa) in order to retrieve VCI v2 for Africa
    # version 1.0 - 16/04/2021
    # Contact: icherif@yahoo.com
    # -----------------------------

    # Required aux data.

    import os
    import snappy
    from snappy import ProductIO, GPF, HashMap
    import subprocess

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

    def VCI_to_int(data, file, dir):
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
        except:
            print('Problem computing VCI')
        try:
            date = os.path.basename(f_ndvi).split('_')[0]
            filename = 'g2_BIOPAR_VCI_' + date + '_AFRI_OLCI_V2.0.nc'
            vci = VCI_to_int(vci_float, filename, dir_out)
        except Exception as e:
            print('Problem transforming VCI')
            raise e
        return os.path.join(dir_out, filename)

    output_obj = EOProduct.objects.get(pk=eo_product_pk)
    ndvi_1k_obj = output_obj.eo_products_inputs.first()

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
                             '-a', "SCALING,VCI,o,d,0.005",
                             '-a', "OFFSET,VCI,o,d,-0.125",
                             outfile])
        if cp.returncode != 0:
            raise Exception(f'EXIT CODE: {cp.returncode}, ERROR: {cp.stderr} ')

        content = File(open(outfile, 'rb'))

        output_obj.file.save(name=output_obj.filename, content=content, save=False)
        output_obj.state = EOProductStateChoices.Ready
        output_obj.datetime_creation = now
        output_obj.save()
    return


@shared_task
def task_s0p02_clip_lai300m_v1_afr(eo_product_pk: int):
    import snappy
    from snappy import ProductIO, GPF, HashMap, WKTReader

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    eo_source: EOSource = eo_product.eo_sources_inputs.first()

    filename_in = eo_source.filename

    HashMap = snappy.jpy.get_type('java.util.HashMap')
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
    wkt = "POLYGON((-19.5848214278419448 37.738, 54.2276785724937156 37.738, 54.2276785724937156 -35.4776785714023148, -19.5848214278419448 -35.4776785714023148, -19.5848214278419448 37.738, -19.5848214278419448 37.738))"  # Africa by DK
    rt = filename_in.split('-')[1][:3]
    ver = filename_in.split('_V')[1][:5]

    def clip(data, out_file, geom):
        # Read the file
        params = HashMap()
        # params.put('sourceBands', 'LAI') #'QUAL' all layers are kept!
        params.put('region', '0, 0, 0, 0')
        params.put('geoRegion', geom)
        params.put('subSamplingX', 1)
        params.put('subSamplingY', 1)
        params.put('fullSwath', False)
        params.put('copyMetadata', True)
        clipped = GPF.createProduct('Subset', params, data)

        ProductIO.writeProduct(clipped, out_file, 'NetCDF4-CF')  # 'GeoTIFF'
        return_path = Path(str(clipped.getFileLocation()))
        print(return_path)
        return return_path

    with TemporaryDirectory(prefix='task_s0p02_clip_lai300m_v1_afr_') as temp_dir:
        data = ProductIO.readProduct(eo_source.file.path)
        geom = WKTReader().read(wkt)
        out_file = Path(temp_dir) / eo_product.filename
        clipped: Path = clip(data=data, out_file=out_file.as_posix(), geom=geom)

        try:
            cp: CompletedProcess = run(
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
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.Ready
        eo_product.datetime_creation = now
        eo_product.save()
    return


@shared_task
def task_s02p02_process_ndvia(eo_product_pk: int, iso: str):
    # raise NotImplementedError()
    import rasterio
    from rasterio.merge import merge as rio_merge

    def mosaic_f(in_files: List[Path], outfile: Path) -> Path:
        # prepend vsigzip if filename ends in .gz

        datasets = ['/vsigzip/' + x.as_posix() if x.suffix.endswith('gz') else x.as_posix() for x in in_files]
        logger.info(f'task_s02p02_process_ndvia:mosaic_f:datasets:{datasets}')
        mosaic, out_trans = rio_merge(
            datasets=datasets,
            nodata=0,
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

        warpOptions = gdal.WarpOptions(
            cutlineDSName=shp_file_path.as_posix(), cropToCutline=True,
            dstSRS="EPSG:4326",
            format='netCDF',
            outputType=gdal.GDT_UInt16, options=['COMPRESS=LZW'])
        gdal.Warp(
            srcDSOrSrcDSTab=file_in.as_posix(),
            destNameOrDestDS=file_out_path.as_posix(),
            options=warpOptions)
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
            prmts = dict({'NDVIA': {'dtype': 'f4', 'zlib': 'True', 'complevel': 4}})
            da.to_netcdf(file_out, encoding=prmts)
        except Exception as ex:
            template = "An exception of type {0} occurred while resampling. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logger.err(message)
            raise AfriCultuReSError(message)
        return Path(file_out)

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    if iso == 'ZAF':
        f_shp = '/aux_files/Border_shp/Country_SAfr_main.shp'
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

    with tempfile.TemporaryDirectory() as temp_dir:
        input_files_qs = eo_product.eo_sources_inputs.all()
        input_files_path: List[Path] = [Path(x.file.path) for x in input_files_qs]
        temp_dir_path = Path(temp_dir)

        mosaic_f_path = mosaic_f(input_files_path, temp_dir_path / 'mosaic.tif')
        clipped_f_path = clip(mosaic_f_path, file_out_path=temp_dir_path / 'clipped.nc', shp_file_path=f_shp_path)
        final_raster_path = add_metadata(file_in=clipped_f_path, file_out=temp_dir_path / 'final_file.nc')

        content = File(final_raster_path.open('rb'))
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.Ready
        eo_product.datetime_creation = now
        eo_product.save()
    return eo_product.file.path


###########
# s04p03
##########
@shared_task
def task_s04p03_convert_to_tiff(eo_product_pk: int, tile: int):
    from osgeo import gdal, gdalconst

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    eo_source: EOSource = eo_product.eo_sources_inputs.first()

    eo_product.state = EOProductStateChoices.Generating
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

        cp = subprocess.run(['ncrename',
                             '-v', 'Band1,Flood',
                             file_handle.name])
        cp = subprocess.run(['ncatted',
                             '-a', 'short_name,Flood,o,c,Flood_MR',
                             '-a', "long_name,Flood,o,c,Flood map at medium resolution",
                             '-a', "tile_number,Flood,o,c," + str(tile),
                             '-a', "_FillValue,Flood,o,i,1",
                             file_handle.name])

        content = File(file_handle)
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.Ready
        eo_product.datetime_creation = now
        eo_product.save()


###########
# s06p01
##########
@shared_task
def task_s0601_wb_100m(eo_product_pk: int, wkt: str, iso: str):
    import os
    import snappy
    from snappy import ProductIO, GPF, HashMap, WKTReader

    # ORder is TUN, RWA, ETH, MOZ, ZAF, GHA, NER, KEN

    eo_product = EOProduct.objects.get(pk=eo_product_pk)

    eo_source: EOSource = eo_product.eo_sources_inputs.first()

    # any of the above eo_product, should point to the same eo_source

    HashMap = snappy.jpy.get_type('java.util.HashMap')
    # Get snappy Operators
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

    def clip(data, file_in: Path, dir_out, geom, cnt):
        params = HashMap()
        # params.put('sourceBands', 'WB') #'QUAL' # no band selected here. thus all bands are kept
        params.put('region', '0, 0, 0, 0')
        params.put('geoRegion', geom)
        params.put('subSamplingX', 1)
        params.put('subSamplingY', 1)
        params.put('fullSwath', False)
        params.put('copyMetadata', True)
        clipped = GPF.createProduct('Subset', params, data)
        date = file_in.name.split('_')[3][:6]
        # filename='Subset_c_gls_WB100_' + date + '_GLOBE_S2_V1.0.1_snappy_' + cnt + '.nc'
        filename = date + '_SE2_' + cnt + '_0100m_0030_WBMA.nc'
        ProductIO.writeProduct(clipped, os.path.join(dir_out, filename), 'NetCDF4-CF')  # 'GeoTIFF'
        return Path(str(clipped.getFileLocation()))

    # order is TUN, RWA, ETH, MOZ, ZAF, GHA, NER, KEN

    # product_list defined in the outer scope
    with TemporaryDirectory(prefix='task_s0601_wb_100m_') as temp_dir:
        data = ProductIO.readProduct(eo_source.file.path)
        geom = WKTReader().read(wkt)
        cnt = iso.upper()
        file_in = Path(eo_source.file.path)
        clipped: Path = clip(data=data, file_in=file_in, dir_out=temp_dir, geom=geom, cnt=cnt)

        content = File(clipped.open('rb'))
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.Ready
        eo_product.datetime_creation = now
        eo_product.save()

    return 0


#####
# S06P01
@shared_task
def task_s06p01_clip_to_africa(eo_product_pk: int):
    import snappy
    from snappy import ProductIO, GPF, HashMap, WKTReader

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    eo_source = eo_product.eo_sources_inputs.first()

    wkt = "POLYGON((-30 40, 60 40, 60 -40, -30 -40, -30 40, -30 40))"  # Africa
    HashMap = snappy.jpy.get_type('java.util.HashMap')
    # Get snappy Operators
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

    def clip(file_in, file_out, geom) -> Path:
        # Read the file
        data = ProductIO.readProduct(file_in)

        params = HashMap()
        # params.put('sourceBands', 'WB') #'QUAL'
        params.put('region', '0, 0, 0, 0')
        params.put('geoRegion', geom)
        params.put('subSamplingX', 1)
        params.put('subSamplingY', 1)
        params.put('fullSwath', False)
        params.put('copyMetadata', True)
        clipped = GPF.createProduct('Subset', params, data)

        ProductIO.writeProduct(clipped, file_out.as_posix(), 'NetCDF4-CF')
        return Path(str(clipped.getFileLocation()))

    with TemporaryDirectory(prefix='task_s06p01_clip_to_africa_') as temp_dir:
        date = eo_source.filename.split('_')[3][:8]
        f_out = date + '_SE2_AFR_0300m_0030_WBMA.nc'
        clipped = clip(eo_source.file.path, Path(temp_dir).joinpath(f_out), WKTReader().read(wkt))

        content = File(clipped.open('rb'))
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.Ready
        eo_product.datetime_creation = now
        eo_product.save()

    return 0


####
# S06P03


######
# S06P04

@shared_task
def task_s06p04_et_3km(eo_product_pk: int):
    from eo_engine.common.contrib.h5georef import H5Georef
    import bz2

    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    # only has one input
    eo_source_input: EOSource = eo_product.eo_sources_inputs.first()
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
                                 '-a', "ET_UNITS,Band1,o,c,[mm/day]",
                                 '-a', "ET_SCALING_FACTOR,Band1,o,d,1000",
                                 '-a', "ET_OFFSET,Band1,o,d,0",
                                 '-a', "ET_MISSING_VALUE,Band1,o,d,-1",
                                 '-a', "_FillValue,Band1,o,d,-0.001",
                                 final_file.name], check=True)
        except Exception as e:
            raise e

        content = File(final_file)
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.Ready
        eo_product.datetime_creation = now
        eo_product.save()


@shared_task
def task_s06p04_main_sse_bop_v5(eo_product_pk: int):
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

    def clip(file_in, file_out):
        # Clip the file using GDAL
        target_resolution = 0.009651999920606611424
        xmin, ymin, xmax, ymax = -20.0084493160248371, -38.0030921809375428, 55.0068940669297461, 40.0043711774050763
        cp = subprocess.run([
            'gdalwarp',
            '-r', 'average', '-overwrite',
            '-tr', f'{target_resolution}', f'{target_resolution}',
            '-te', f'{xmin}', f'{ymin}', f'{xmax}', f'{ymax}',
            file_in, file_out], check=True)

        eo_product = EOProduct.objects.get(eo_product_pk)
        eo_source = eo_product.eo_sources_inputs.first()

        with TemporaryDirectory() as temp_dir, ZipFile(eo_source.file.path) as z:
            f = eo_source.filename
            z.extractall(temp_dir)
            date = f[1:5]
            month = f[5:7]
            f_new_tif = temp_dir + "/" + "m" + date + month + "_modisSSEBopETv5_anomaly_pct.tif"
            f_new_nc = temp_dir + "/" + "m" + date + month + "01_MOD_AFR_5600m_0030_ETanom_v5.nc"

            clip(f_new_tif, f_new_nc)
            cp = subprocess.run(['ncrename',
                                 '-v', 'Band1,ETanom',
                                 f_new_nc], check=True)
            cp = subprocess.run(['ncatted',
                                 '-a', "long_name,ETanom,o,c,Monthly_Evapotranspiration_Anomaly",
                                 '-a', "Unit,ETanom,o,c,[%]",
                                 f_new_nc], check=True)

            with open(f_new_nc, 'rb') as file_handler:
                content = File(file_handler)
                eo_product.file.save(name=eo_product.filename, content=content, save=False)
                eo_product.state = EOProductStateChoices.Ready
                eo_product.datetime_creation = now
                eo_product.save()

        return '+++Finished+++'


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
