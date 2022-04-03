import subprocess
from logging import Logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import numpy as np
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.files import File
from django.core.files.temp import NamedTemporaryFile
from django.utils import timezone
from osgeo import gdal, gdalconst
from rasterio.merge import merge

from eo_engine.models import EOProduct, EOSource, EOProductStateChoices

logger: Logger = get_task_logger(__name__)

now = timezone.now()


@shared_task
def task_s04p03_convert_to_tiff(eo_product_pk: int, tile: int):
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
    dir_preevent = settings.AUX_FILES_ROOT / 'WB_bag/Archive'
    aoi = [-1.788, 11.74, 0.699, 8.285]  # AOI= [minX,maxY,maxX,minY]

    def coreg_file(file_in, file_out, aoi):
        # Function to clip and resample the event raster to fit input file pixel size and dimension
        ds_in = gdal.Open(file_in)
        # Clipping the file
        warpOptions = gdal.WarpOptions(format="Gtiff",
                                       dstSRS="EPSG:4326",
                                       outputBounds=(aoi[0], aoi[3], aoi[2], aoi[1]),
                                       options=['COMPRESS=LZW'])
        try:
            gdal.Warp(destNameOrDestDS=file_out, srcDSOrSrcDSTab=ds_in, options=warpOptions)
        except Exception as e:
            print('Error warping event file')
            raise e
        return

    def get_flood(file_in, file_in_ev, file_out):
        # Read files
        try:
            pre_band = gdal.Open(file_in)
        except Exception as e:
            print('Error opening file ', file_in)
            raise e
        try:
            ev_band = gdal.Open(file_in_ev)
        except Exception as e:
            print('Error opening file ', file_in_ev)
            raise e
        # Get input values
        pre = pre_band.GetRasterBand(1).ReadAsArray()
        ev = ev_band.GetRasterBand(1).ReadAsArray()
        # Get the flood map
        flood = ev
        flood[(pre == 1) & (ev == 1)] = 2  # pixels with ev=1 and pre=1 are permanent water
        flood[np.isnan(ev)] = 11  # get rid of the nan values in the event flood map
        flood[np.isnan(pre)] = 11  # get rid of the nan values in the pre-flood map
        try:
            # Write output file
            driver = gdal.GetDriverByName("GTiff")
            ds_out = driver.Create(file_out, pre.shape[1], pre.shape[0], 1, gdal.GDT_UInt16, ['COMPRESS=LZW'])
            ds_out.SetProjection(pre_band.GetProjection())
            ds_out.SetGeoTransform(pre_band.GetGeoTransform())
            ds_out.GetRasterBand(1).SetNoDataValue(11)
            ds_out.GetRasterBand(1).WriteArray(flood)
        except Exception as e:
            print('Error storing the flood map')
            raise e
        return 0

    raise NotImplementedError()


__all__ = ['task_s04p03_convert_to_tiff',
           'task_s04p03_floods375m',
           'task_s04p03_floods10m'
           ]
