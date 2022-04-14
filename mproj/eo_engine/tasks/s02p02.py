import os
import subprocess
from logging import Logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import snappy
from celery import shared_task
from celery.utils.log import get_task_logger
from django.core.files import File
from django.utils import timezone
from osgeo import gdal
from snappy import ProductIO, GPF

from eo_engine.errors import AfriCultuReSError
from eo_engine.models import EOProduct, EOSource, EOProductStateChoices

logger: Logger = get_task_logger(__name__)

now = timezone.now()


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
                             '-a', "add_offset,NDVI,o,d,-0.08",
                             '-a', "scale_factor,NDVI,o,d,0.004",
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
        f_lts_max = f_lts[:-3] + '_max.nc'
        print('\n== Processing ==')
        print('NDVI file: ', f_ndvi)
        print('with LTS min: ', f_lts_min)
        print('with LTS max: ', f_lts_max)

        print(Path(f_lts_max).is_file())

        data = ProductIO.readProduct(f_ndvi)
        data1 = ProductIO.readProduct(f_lts_min)
        data2 = ProductIO.readProduct(f_lts_max)
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


__all__ = [
    'task_s02p02_ndvi300m_v2',
    'task_s02p02_nvdi1km_v3',
    'task_s02p02_vci1km_v2',
    'task_s02p02_lai300m_v1',
    'task_s02p02_ndvianom250m',
    'task_s06p01_wb300m_v2',
]
