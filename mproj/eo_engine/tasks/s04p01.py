from logging import Logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

from celery import shared_task
from celery.utils.log import get_task_logger
from django.core.files import File
from django.utils import timezone
from osgeo import gdal
from pymodis.convertmodis_gdal import createMosaicGDAL

from eo_engine.models import EOProduct, EOSource, EOProductStateChoices

logger: Logger = get_task_logger(__name__)

now = timezone.now()


@shared_task
def task_s04p01_lulc500m(eo_product_pk):

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


__all__ = [
    'task_s04p01_lulc500m'
]
