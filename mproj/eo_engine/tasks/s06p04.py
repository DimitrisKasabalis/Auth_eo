import bz2
import subprocess
from logging import Logger
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import numpy as np
import rasterio
from celery import shared_task
from celery.utils.log import get_task_logger
from django.core.files import File
from django.core.files.temp import NamedTemporaryFile
from django.utils import timezone

from eo_engine.common.contrib.h5georef import H5Georef
from eo_engine.common.verify import check_file_exists
from eo_engine.errors import AfriCultuReSError
from eo_engine.models import EOProduct, EOSource, EOProductStateChoices, EOSourceGroup, EOSourceGroupChoices

logger: Logger = get_task_logger(__name__)

now = timezone.now()


@shared_task
def task_s06p04_et3km(eo_product_pk: int):
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
                crs=rasterio.crs.CRS({'init': 'epsg:4326'}))

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

        get_aeti_qual(
            file_in_path_et=file_in_path_et, file_in_path_lst=file_in_path_lst,
            file_in_path_ndvi=file_in_path_ndvi, file_out_path=file1_tif)

        logger.info('cutting/warping')
        subprocess.run(
            ['gdalwarp',
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


@shared_task
def task_s06p04_et100m(eo_product_pk: int, iso: str):
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
            et_meta.update(
                crs=rasterio.crs.CRS({'init': 'epsg:4326'})
            )

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


__all__ = [
    'task_s06p04_et3km',
    'task_s06p04_etanom5km',
    'task_s06p04_et250m',
    'task_s06p04_et100m'
]
