"""
A script to georeference Daily Evapotranspiration HDF5 files produced by EUMETSAT.
The specific product under consideration is LSA-SAF ET product (DMET 312).
This script uses the equations given in the Annex C of the Product User Manual(PUM) of the Evapotranspiration and surface fluxes products.
The PUM can be obtained online at: https://landsaf.ipma.pt/GetDocument.do?id=754

The code uses 10 random GCPs for georeferencing each HDF5 input file, projects it to GCS WGS84 and clips it to the extent of Africa. All the tasks will be executed when the produced GDAL commands are run.

version 2.0 - 26/1/2021
Contact: The Geovision team http://geovision.web.auth.gr/, icherif@yahoo.com
"""
from pathlib import Path

import tables
from math import pow, sin, cos, atan, sqrt, radians, degrees
from pyproj import Proj, transform
from osgeo import gdal

import re
import os
import random
from subprocess import Popen, PIPE

from django.conf import settings

GDAL_TRANSLATE = settings.GDAL_TRANSLATE
GDAL_WARP = settings.GDAL_WRAP


class H5Georef(object):
    latLongProj = '+init=epsg:4326'

    def __init__(self, h5FilePath: Path):
        # the LSA-SAF parameters have this shift because they use Fortran
        # (an array's first index starts at 1 and not 0)
        self.CLCorrection = -1
        self.p1 = 42164  # Distance between satellite and center of the Earth, measured in km
        self.p2 = 1.006803
        self.p3 = 1737121856

        # Open an HDF5 file and extract its relevant parameters.
        self.h5FilePath = h5FilePath
        self.DirName = os.path.dirname(h5FilePath)

        h5File = tables.open_file(h5FilePath)

        self.arrays = dict()

        # Access HDF5 attributes
        for arr in h5File.root._v_attrs._v_node:
            npArray = arr.read()
            scalingFactor = arr.attrs.SCALING_FACTOR
            self.arrays[arr.name] = {
                "nCols": arr.attrs.N_COLS,
                "nLines": arr.attrs.N_LINES,
                "scalingFactor": scalingFactor,
                "missingValue": arr.attrs.MISSING_VALUE / scalingFactor,
                "min": npArray.min() / scalingFactor,
                "max": npArray.max() / scalingFactor,
                "oldMin": npArray.min(),
                "oldMax": npArray.max()}

        # Set the projection string
        subLonRE = re.search(r"[A-Za-z]{4}[<(][-+]*[0-9]{3}\.?[0-9]*[>)]",
                             h5File.root._v_attrs["PROJECTION_NAME"].decode(
                                 "utf-8"))  # projection in bytes ("b'Geos<000.0>) is converted to string here
        if subLonRE:
            self.subLon = float(subLonRE.group()[5:-1])

        else:
            raise ValueError
        self.satHeight = 35785831
        self.GEOSProjString = "+proj=geos +lon_0=%s +h=%s +x_0=0.0 +y_0=0.0" % (self.subLon, self.satHeight)

        # Set the correction parameters
        self.coff = h5File.root._v_attrs["COFF"]  # should this be corrected? #+ self.CLCorrection
        self.loff = h5File.root._v_attrs["LOFF"]  # should this be corrected? #+ self.CLCorrection
        self.cfac = h5File.root._v_attrs["CFAC"]
        self.lfac = h5File.root._v_attrs["LFAC"]

        h5File.close()

    def _run_command(self, command):
        # Run an external command and return its return code, stdout and stderr.
        newProcess = Popen(command, stdout=PIPE, stderr=PIPE)
        stdout, stderr = newProcess.communicate()
        return newProcess.returncode, stdout, stderr

    def get_sample_coords(self, numSamples=10):
        # Return a list of tuples holding line, col, northing, easting.
        # These tuples will be used as GCPs for the georeferencing of the file

        samplePoints = []
        nCols = self.arrays.get('ET').get('nCols')
        nLines = self.arrays.get('ET').get('nLines')

        while len(samplePoints) < numSamples:
            line = random.randint(1, nLines)
            col = random.randint(1, nCols)
            lon, lat = self._get_lat_lon(line, col)

            if lon:
                easting, northing = self._get_east_north(lon, lat)
                samplePoints.append((line, col, northing, easting))
        return samplePoints

    def _get_east_north(self, lon, lat):
        # Convert between latlon and geos coordinates.
        inProj = Proj(self.latLongProj)
        outProj = Proj(self.GEOSProjString)

        easting, northing = transform(inProj, outProj, lon, lat)

        return float(easting), float(northing)

    def _get_lat_lon(self, nLin, nCol):
        # Get the lat lon coordinates of a pixel based on LSASAF PUM.
        try:
            x = radians((nCol - self.coff) / (pow(2, -16) * self.cfac))  # x in Degrees
            y = radians((nLin - self.loff) / (pow(2, -16) * self.lfac))  # y in Degrees
            sd = sqrt(pow(self.p1 * cos(x) * cos(y), 2) - \
                      self.p3 * (pow(cos(y), 2) + self.p2 * pow(sin(y), 2)))
            sn = ((self.p1 * cos(x) * cos(y)) - sd) / (pow(cos(y), 2) + \
                                                       self.p2 * pow(sin(y), 2))
            s1 = self.p1 - sn * cos(x) * cos(y)
            s2 = sn * sin(x) * cos(y)
            s3 = -sn * sin(y)
            sxy = sqrt(pow(s1, 2) + pow(s2, 2))
            lon = degrees(atan(s2 / s1)) + self.subLon
            lat = degrees(atan(self.p2 * s3 / sxy))
        except ValueError:
            lon = lat = None
        return lon, lat

    def georef_gtif(self, samplePoints, selectedArrays=None):
        """ Create a georeferenced GeoTiff file for each of the ET array using
        the GCPs created by the function get_sample_coords(). """

        if selectedArrays is None:
            selectedArrays = self.arrays.get('ET')

        outFileName = self.h5FilePath.as_posix() + "_ET.tif"

        # Prepare the gdal_translate command
        translateCommand = [GDAL_TRANSLATE, '-r', 'bilinear', '-ot',
                            'Float32', '-a_nodata', str(selectedArrays.get('missingValue')), \
                            '-a_srs', str(self.GEOSProjString), '-scale', str(selectedArrays.get('oldMin')), \
                            str(selectedArrays.get('oldMax')), str(selectedArrays.get('min')),
                            str(selectedArrays.get('max'))]

        for (line, col, northing, easting) in samplePoints:
            translateCommand.append('-gcp')
            translateCommand.append(str(col))
            translateCommand.append(str(line))
            translateCommand.append(str(easting))
            translateCommand.append(str(northing))
        translateCommand.append("HDF5:" + self.h5FilePath.as_posix() + "://ET")
        translateCommand.append(outFileName)

        # print("running gdal.translate...............")
        self._run_command(translateCommand)

        return outFileName

    def warp(self, projectionString=None):
        #  Warp the georeferenced files to the desired projection.
        if projectionString is None:
            projectionString = self.latLongProj

        missingValue = self.arrays['ET'].get("missingValue")

        inFileName = self.h5FilePath.as_posix() + "_ET.tif"
        outFileName = self.h5FilePath.as_posix() + "_ET_GCS.tif"

        warpCommand = [GDAL_WARP, '-overwrite', '-r', 'bilinear', '-tr',
                       '0.0275', '0.0275', '-dstnodata', str(missingValue), '-te', '-30', '-40', '60', '40', '-s_srs',
                       str(self.GEOSProjString), \
                       '-t_srs', str(projectionString), inFileName, outFileName]
        self._run_command(warpCommand)

        return outFileName

    def to_netcdf(self, myfile: Path) -> Path:
        # convert to netcdf
        tiffile = self.h5FilePath.as_posix() + "_ET_GCS.tif"
        # print("\nConverting file: %s" % tiffile)
        optionsNC2 = gdal.TranslateOptions(format='netCDF')
        # Use Gdal to open the file
        ds = gdal.Open(tiffile)
        gdal.Translate(srcDS=ds, destName=myfile.as_posix(), options=optionsNC2)

        return myfile
