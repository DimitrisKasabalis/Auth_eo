import subprocess
import tempfile
from logging import Logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Literal, Union, Set, Iterable, Any, NamedTuple, TypeVar

import numpy as np
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.files import File
from django.core.files.temp import NamedTemporaryFile
from django.utils import timezone
from django.utils.datetime_safe import datetime
from osgeo import gdal, gdalconst
from rasterio.merge import merge

from eo_engine.errors import AfriCultuReSError
from eo_engine.models import EOProduct, EOSource, EOProductStateChoices

logger: Logger = get_task_logger(__name__)

now = timezone.now()

AUGUST = 8
FLOOD_PRE_EVENT_MIN_DATE_INCLUSIVE = 14
FLOOD_PRE_EVENT_MAX_DATE_INCLUSIVE = 16
DateFilename = NamedTuple('DateFilename', [('date', datetime.date), ('filepath', Path)])


def get_prevent_dir() -> Path:
    return_path: Path = settings.AUX_FILES_ROOT / 'WB_bag/Archive'
    if not return_path.exists():
        logger.info(f'{return_path.name} does not exist. Making it')
        return_path.mkdir(parents=True, exist_ok=True)

    return return_path


def BAG_FILENAME_TO_DATE(filename: str) -> datetime.date:
    date_str = filename[:8]
    date = datetime.strptime(date_str, '%Y%m%d')

    return date.date()


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


def coreg_file(
        file_in: Union[str, Path],
        file_out: Union[str, Path],
        aoi_bbox: List[float],
        template_raster: Union[str, Path] = None
):
    """ Function to clip and resample the event raster to fit input file pixel size and dimension
    """
    try:
        file_in = file_in.as_posix()
    except AttributeError:
        pass

    try:
        file_out = file_out.as_posix()
    except AttributeError:
        pass

    try:
        template_raster = template_raster.as_posix()
    except AttributeError:
        pass

    ds_in = gdal.Open(file_in)
    # Clipping the file
    if ds_in is None:
        raise AfriCultuReSError('Failed to open %s' % file_in)
    warp_options = {}
    warp_options.update(
        format="Gtiff",
        dstSRS="EPSG:4326",
        outputBounds=(aoi_bbox[0], aoi_bbox[3], aoi_bbox[2], aoi_bbox[1]),
        options=['COMPRESS=LZW'],
    )
    if template_raster:
        template_raster_ds: gdal.Dataset = gdal.Open(template_raster)

        warp_options.update(
            xRes=template_raster_ds.RasterXSize(),
            yRes=template_raster_ds.RasterYSize()
        )
    warp_options = gdal.WarpOptions(**warp_options)
    try:
        gdal.Warp(destNameOrDestDS=file_out, srcDSOrSrcDSTab=ds_in, options=warp_options)
    except Exception as e:
        print('Error warping event file')
        raise e
    return


def filter_pre_event_files(x: Set[DateFilename],
                           from_date_inclusive: datetime.date,
                           to_date_inclusive: datetime.date) -> List[DateFilename]:
    filtered = [e for e in x if from_date_inclusive <= e.date <= to_date_inclusive]
    filtered = sorted(filtered, key=lambda e: e.date)
    return filtered


def get_pre_flood_event_file(year: int, aoi: str) -> Path:
    files: Set[DateFilename] = set()
    from_date_inclusive = datetime(
        year,
        AUGUST,
        FLOOD_PRE_EVENT_MIN_DATE_INCLUSIVE
    ).date()

    to_date_inclusive = datetime(
        year,
        AUGUST,
        FLOOD_PRE_EVENT_MAX_DATE_INCLUSIVE
    ).date()

    prevent_dir = get_prevent_dir()
    for file_path in prevent_dir.glob(f'{year}*.tif'):
        files.add(
            DateFilename(
                date=BAG_FILENAME_TO_DATE(file_path.name),
                filepath=file_path)
        )

    files_filtered = filter_pre_event_files(
        files,
        from_date_inclusive=from_date_inclusive,
        to_date_inclusive=to_date_inclusive
    )

    if files_filtered:
        record = files_filtered[0]
        date = record.date
        file_path = record.filepath
        msg = f'found pre-event file: {file_path.name} ({date})'
        logger.info(msg)
        return file_path

    # we don't have a pre-event file ready
    else:
        logger.info('no prevent file found, attempting to make one')
        from eo_engine.models.eo_group import EOProductGroupChoices, EOProductGroup
        from eo_engine.models.eo_product import EOProduct, EOProductStateChoices

        if aoi == 'BAG':
            group = EOProductGroup.objects.get(name=EOProductGroupChoices.S06P01_WB_10M_BAG)
            aoi_bbox = [-1.788, 11.74, 0.699, 8.285]
        else:
            raise AfriCultuReSError(f'no configuration  for this AOI: {aoi}')

        bag_year_event_qs = EOProduct.objects.filter(
            reference_date__gte=from_date_inclusive,
            reference_date__lte=to_date_inclusive,
            group=group,
            state=EOProductStateChoices.READY
        ).order_by('reference_date')

        if bag_year_event_qs.exists():

            bag_year_event = bag_year_event_qs.first()
            msg = f' BAG Product chosen as pre-event: {bag_year_event}'
            logger.info(msg)
            prevent_dir = get_prevent_dir()
            preevent_file_in: Path = Path(bag_year_event.file.path)
            preevent_file_out: Path = prevent_dir / (preevent_file_in.stem + '_pre.tif')
            coreg_file(
                file_in=preevent_file_in.as_posix(),
                file_out=preevent_file_out.as_posix(),
                aoi_bbox=aoi_bbox
            )

            return get_pre_flood_event_file(year=year, aoi=aoi)
        else:
            'No valid BAG products exist in the system.'
            raise Exception('Non suitable file found')


@shared_task
def task_s04p03_floods10m(eo_product_pk: int, aoi: Literal['BAG', 'KZN']):
    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOProduct.objects.filter(group=input_eo_source_group, reference_date=eo_product.reference_date)

    def get_flood(pre_flood_event_file: Path,
                  file_in_event,
                  file_out: Path):
        # Read files
        try:
            pre_band = gdal.Open(pre_flood_event_file.as_posix())
        except Exception as e:
            print('Error opening file %s:\n %s' %
                  (pre_flood_event_file.name, pre_flood_event_file.as_posix()))
            raise e
        try:
            ev_band = gdal.Open(file_in_event.as_posix())
        except Exception as e:
            print('Error opening file %s:\n %s', (file_in_event.name, file_in_event.as_posix()))
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
            ds_out = driver.Create(
                file_out.as_posix(),
                pre.shape[1],
                pre.shape[0], 1,
                gdal.GDT_UInt16,
                ['COMPRESS=LZW'])
            ds_out.SetProjection(pre_band.GetProjection())
            ds_out.SetGeoTransform(pre_band.GetGeoTransform())
            ds_out.GetRasterBand(1).SetNoDataValue(11)
            ds_out.GetRasterBand(1).WriteArray(flood)
        except Exception as e:
            print('Error storing the flood map')
            raise e
        return 0

    input_file = input_files_qs.first()

    year = input_file.reference_date.year

    if aoi == 'BAG':
        aoi_bbox = [-1.788, 11.74, 0.699, 8.285]
    else:
        raise AfriCultuReSError('Not provisioned for another AOI :/.')

    pre_flood_event_file: Path = get_pre_flood_event_file(year, aoi)
    flood_in_event: Path = Path(input_file.file.path)
    eo_product.state = EOProductStateChoices.GENERATING
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        file_out = temp_path / 'out.tif'
        flood_in_event_tmp = temp_path / 'temp_flood_event.tif'
        coreg_file(
            file_in=flood_in_event,
            file_out=flood_in_event_tmp,
            aoi_bbox=aoi_bbox)
        get_flood(
            pre_flood_event_file=pre_flood_event_file,
            file_in_event=flood_in_event_tmp,
            file_out=file_out
        )

        with file_out.open('rb') as file_handler:
            content = File(file_handler)
            eo_product.file.save(name=eo_product.filename, content=content, save=False)
            eo_product.state = EOProductStateChoices.READY
            eo_product.datetime_creation = now
            eo_product.save()

    print('done')


__all__ = ['task_s04p03_convert_to_tiff',
           'task_s04p03_floods375m',
           'task_s04p03_floods10m'
           ]
