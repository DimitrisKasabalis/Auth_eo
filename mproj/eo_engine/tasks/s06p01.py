import datetime
import os
import subprocess
from datetime import date as dt_date
from logging import Logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import NamedTuple, Optional, List, Literal

import numpy as np
import pandas
import snappy
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.files import File
from django.db import connections
from django.utils import timezone
from osgeo import gdal
from snappy import ProductIO, GPF, WKTReader

from eo_engine.common.misc import write_line_to_file
from eo_engine.common.verify import check_file_exists
from eo_engine.errors import AfriCultuReSError
from eo_engine.models import EOProduct, EOSource, EOProductStateChoices
from eo_engine.tasks.s02p02 import now

logger: Logger = get_task_logger(__name__)

now = timezone.now()

# All the variables that I will need
WB10mParams = NamedTuple('Params', [
    ('boundary_shapefile', Path),
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


def WB10mParamsFactory(reference_date: datetime.date, version: Literal['kzn', 'bag']) -> WB10mParams:
    if version.lower() not in ['kzn', 'bag']:
        raise AfriCultuReSError(f'wrong parameters for version {version}. Should be either "kzn" or "bag"')

    if version == 'kzn':
        boundary_shapefile = settings.AUX_FILES_ROOT / 'Border_shp/Country_SAfr.shp'
        hand_file_tif_path = 'HAND/HAND_kzn_fs_2.tif'
        orbits_threshold_csv_file_path = settings.AUX_FILES_ROOT / "WB_kzn/Listparams_kzn.csv"
        archive_folder = settings.AUX_FILES_ROOT / "WB_kzn/archive"
        dem_file_tif_path = None
        hand_threshold = 10
        dem_threshold = 0
    elif version == 'bag':
        boundary_shapefile = settings.AUX_FILES_ROOT / 'WB_bag/Floods_env_bag.shp'
        hand_file_tif_path = settings.AUX_FILES_ROOT / 'HAND/hand_GH.tif'
        orbits_threshold_csv_file_path = settings.AUX_FILES_ROOT / "WB_bag/Listparams_bag.csv"
        archive_folder = settings.AUX_FILES_ROOT / "WB_bag/archive"
        dem_file_tif_path = None
        hand_threshold = 10
        dem_threshold = 0
    else:
        raise

    return WB10mParams(
        boundary_shapefile=boundary_shapefile,
        hand_file_tif_path=hand_file_tif_path,
        orbits_threshold_csv_file_path=orbits_threshold_csv_file_path,
        archive_folder=archive_folder,
        dem_file_tif_path=dem_file_tif_path,
        sentinel_zip_files_path_list=[],
        reference_date=reference_date,
        sentinelhub_username="nesch",
        sentinelhub_password="w@m0sPr0ject",
        hand_threshold=hand_threshold,
        dem_threshold=dem_threshold
    )


def s06p01_wb10m(eo_product_pk: int, version: Literal['kzn', 'bag']):
    eo_product = EOProduct.objects.get(id=eo_product_pk)
    logger.info(f'EOProduct: {eo_product}')
    reference_date = eo_product.reference_date
    logger.info(f'Reference_Date: {reference_date}')
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.all()
    input_files_qs = EOSource.objects.filter(
        group__in=input_eo_source_group,
        reference_date=reference_date)
    logger.info(f'Number of input Files: {input_files_qs.count()}')

    params = WB10mParamsFactory(reference_date=reference_date, version=version)

    params.archive_folder.mkdir(parents=True, exist_ok=True)

    check_file_exists(file_path=params.boundary_shapefile)
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
                dstSRS="EPSG:4326",
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

                    except Exception:
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
                except Exception:
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
                 '-cutline', params.boundary_shapefile.as_posix(),
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
def task_s06p01_wb100m(eo_product_pk: int, aoi_wkt: str):
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


@shared_task
def task_s06p01_wb10m_kzn(eo_product_pk: int):
    return task_s06p01_wb100m(eo_product_pk=eo_product_pk, version='kzn')


@shared_task
def task_s06p01_wb10m_bag(eo_product_pk: int):
    return s06p01_wb10m(eo_product_pk, version='bag')


@shared_task
def task_s06p01_wb300m_v2(eo_product_pk: int):
    eo_product = EOProduct.objects.get(id=eo_product_pk)
    input_eo_source_group = eo_product.group.eoproductgroup.pipelines_from_output.get().input_groups.get().eosourcegroup
    input_files_qs = EOSource.objects.filter(group=input_eo_source_group, reference_date=eo_product.reference_date)
    input_file = input_files_qs.get()

    # fixed parameters
    geom = [16904, 46189, 64166, 93689]
    date = input_file.filename.split('_')[3][:8]
    f_out = date + '_SE2_AFR_0300m_0030_WBMA.nc'

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
        clipped = clip(input_file.file.path, Path(temp_dir).joinpath(f_out), geom)

        content = File(clipped.open('rb'))
        eo_product.file.save(name=eo_product.filename, content=content, save=False)
        eo_product.state = EOProductStateChoices.READY
        eo_product.datetime_creation = now
        eo_product.save()

    return 0


__all__ = [
    'task_s06p01_wb10m_bag',
    'task_s06p01_wb10m_kzn',
    'task_s06p01_wb100m',
    'task_s06p01_wb300m_v2'
]
