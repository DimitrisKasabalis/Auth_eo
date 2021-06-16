# coding: utf-8
import datetime as dt
import os
import re
import sys

# import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import xarray as xr
# from tqdm import tqdm


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
        sys.exit('GLC product not found please check')

    return da, param


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
            print(message)
            raise sys.exit(1)
    else:
        da = da.shift(lat=1, lon=1)
    return da


def _date_extr(path):
    _, tail = os.path.split(path)
    pos = [pos for pos, char in enumerate(tail) if char == '_'][2]
    date = tail[pos + 1: pos + 9]
    date_h = pd.to_datetime(date, format='%Y%m%d')
    return date, date_h


def _resampler(path, my_ext, plot, out_folder):
    # Load the dataset
    ds = xr.open_dataset(path, mask_and_scale=False)

    # select parameters according to the product.
    da, param = _param(ds)
    date, date_h = _date_extr(path)

    # AOI
    da = _aoi(da, ds, my_ext)

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
        print(message)
        raise sys.exit(1)

    # Output write
    try:
        da_r.name = param['product']
        da_r.attrs['short_name'] = param['short_name']
        da_r.attrs['long_name'] = param['long_name']
        da_r.attrs['_FillValue'] = int(255)
        da_r.attrs['scale_factor'] = np.float32(param['SCALING'])
        da_r.attrs['add_offset'] = np.float32(param['OFFSET'])

        prmts = dict({param['product']: {'dtype': 'i4', 'zlib': 'True', 'complevel': 4}})

        name = param['product']
        if len(my_ext) != 0:
            file_name = f'CGLS_{name}_{date}_1KM_Resampled_Africa.nc'
        else:
            file_name = f'CGLS_{name}_{date}_1KM_Resampled_.nc'

        out_file = os.path.join(out_folder, file_name)

        da_r.to_netcdf(out_file, encoding=prmts)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        raise sys.exit(1)

    print(f'{file_name} resampled')

    # # Plot
    # if plot:
    #     da_r.plot(robust=True, cmap='YlGn', figsize=(15, 10))
    #     plt.title(f'Copernicus Global Land\n Resampled {name} to 1K over Europe\n date: {date_h.date()}')
    #     plt.ylabel('latitude')
    #     plt.xlabel('longitude')
    #     plt.draw()
    #     plt.show()


if __name__ == '__main__':

    """ Code to resample NDVI 300m to 1km based on the resampling code from CGLS.
      The input NDVI 300m v3 include 3 subdatasets:
      - NDVI: NDVI values (8-bit unsigned integer)
      - NDVI_unc: NDVI standard_error (16-bit integer)
      - NOBS: NDVI number of observations
      The output file includes only one layer with NDVI values
      Documentation here: https://github.com/xavi-rp/ResampleTool_notebook/blob/master/Resample_Report_v2.5.pdf
    
      Ines Cherif - icherif@yahoo.com 
      v1 - 22/4/2021
    """

    # define the input folder
    in_folder = r'D:\Data\from_CGLS\with_NDVI_v2\NDVI_300_v2'
    # define the output folder
    out_folder = r'D:\Data\from_CGLS\with_NDVI_v2\NDVI_1km_v3'

    # Define the credential for the Copernicus Global Land repository
    user = ''
    psw = ''

    # Define the AOI [Upper left long, lat, Lower right long, lat]
    AOI = [-30, 40, 60, -40]  # Boundaries of Africa

    # Define if plot results or not
    plot = False

    if os.path.isfile(in_folder):
        # Single file process
        _resampler(in_folder, AOI, plot, out_folder)
        print('File resampled!')
    elif os.path.isdir(in_folder):
        # Process all files in folder
        if not os.listdir(in_folder):
            print("Directory is empty")
        else:
            for filename in os.listdir(in_folder):
                if filename.endswith(".nc"):
                    print('processing file', filename)
                    path_ = os.path.join(in_folder, filename)
                    _resampler(path_, AOI, plot, out_folder)
            print('All files in folder resampled!')
    else:
        print('Something is wrong with the file or folder')
