import sys
from pathlib import Path

import snappy
from snappy import ProductIO, GPF


class Sentinel1PreprocessingError(Exception):
    pass

def sentinel_1_pre_processing_with_snappy(file_in: Path, file_out: Path, opt):
    def write_product(data, file_path, format=None):
        # Output Geotiff file with snappy
        print(file_path)
        ProductIO.writeProduct(data, file_path, format if format else 'GeoTIFF')

    def apply_orbit_file(file_in: str, file_out: str):
        print("Start reading input file....")
        data = ProductIO.readProduct(file_in)

        # Apply orbit file with snappy
        params = HashMap()
        params.put('selectedPolarisations', 'VV')
        params.put('polyDegree', 3)
        params.put('orbitType', 'Sentinel Restituted (Auto Download)')  # Precise (Auto Download)')
        params.put('continueOnFail', True)  # False)
        orbit = GPF.createProduct('Apply-Orbit-File', params, data)
        write_product(orbit, file_out, format='BEAM-DIMAP')
        return 0

    def border_noise_removal(file_in: str, file_out: str):
        print("Start reading _orb file....")
        data = ProductIO.readProduct(file_in)

        # Remove border noise with snappy
        params = HashMap()
        params.put('selectedPolarisations', 'VV')
        params.put('borderLimit', 1000)  # put 1000 to make sure every bad pixels are included
        params.put('trimThreshold', 0.5)
        noise_removed = GPF.createProduct('Remove-GRD-Border-Noise', params, data)
        write_product(noise_removed, file_out, format='BEAM-DIMAP')
        return 0

    def thermal_noise_removal(file_in, file_out):
        print("Start reading _orb_brd file....")
        data = ProductIO.readProduct(file_in)
        # Remove thermal noise with snappy
        params = HashMap()
        params.put('selectedPolarisations', 'VV')
        params.put('removeThermalNoise', True)
        params.put('reIntroduceThermalNoise', False)
        thermal_noise_removed = GPF.createProduct('ThermalNoiseRemoval', params, data)
        write_product(thermal_noise_removed, file_out, format='BEAM-DIMAP')
        return 0

    def do_calibration(file_in, file_out):
        print("Start reading _orb_brd_the file....")
        # data = ProductIO.readProduct(f + '_orb_brd_the.dim')
        data = ProductIO.readProduct(file_in)
        # Perform S1 calibration with snappy
        params = HashMap()
        params.put('outputImageInComplex', False)
        params.put('outputImageScaleInDb', False)
        params.put('createGammaBand', False)
        params.put('createBetaBand', False)
        params.put('selectedPolarisations', 'VV')
        params.put('outputSigmaBand', True)
        params.put('outputGammaBand', False)
        params.put('outputBetaBand', False)
        calibration = GPF.createProduct('Calibration', params, data)
        # write_product(calibration, f + '_orb_brd_the_cal', format='BEAM-DIMAP')
        write_product(calibration, file_out, format='BEAM-DIMAP')
        return 0

    def range_doppler_terrain_correction(file_in, file_out):
        print("Start reading _orb_brd_the_cal file....")
        # data = ProductIO.readProduct(f + '_orb_brd_the_cal.dim')
        data = ProductIO.readProduct(file_in)

        # Apply terrain correction with snappy
        params = HashMap()
        params.put('demName', 'SRTM 3Sec')
        params.put('externalDEMNoDataValue', 0.0)
        params.put('externalDEMApplyEGM', True)
        params.put('demResamplingMethod', 'BILINEAR_INTERPOLATION')
        params.put('imgResamplingMethod', 'NEAREST_NEIGHBOUR')
        params.put('pixelSpacingInMeter', 10.0)
        params.put('pixelSpacingInDegree', 8.983152841195215E-5)

        terrain = GPF.createProduct('Terrain-Correction', params, data)
        data = None
        write_product(terrain, file_out, format='BEAM-DIMAP')
        return 0

    def speckle_filter(file_in, file_out):
        print("Start reading _orb_brd_the_cal_tc file....")
        # data = ProductIO.readProduct(f + '_orb_brd_the_cal_tc.dim')
        data = ProductIO.readProduct(file_in)
        # Apply speckle filtering with snappy
        params = HashMap()
        params.put('filter', 'Lee')
        params.put('filterSizeX', 3)
        params.put('filterSizeY', 3)
        params.put('dampingFactor', 2)
        params.put('estimateENL', True)
        params.put('enl', 1.0)
        params.put('numLooksStr', '1')
        params.put('windowSize', '7x7')
        params.put('targetWindowSizeStr', '3x3')
        params.put('sigmaStr', '0.9')
        params.put('anSize', 50)
        speckle_filtered = GPF.createProduct('Speckle-Filter', params, data)
        # write_product(speckle_filtered, f + '_orb_brd_the_cal_tc_spk', format='BEAM-DIMAP')
        write_product(speckle_filtered, file_out, format='BEAM-DIMAP')
        return 0

    def convert_dB(file_in, file_out):
        # Convert values to dB with snappy
        data = ProductIO.readProduct(file_in)
        params = HashMap()
        data_db = GPF.createProduct('LinearToFromdB', params, data)
        write_product(data_db, file_out)
        return 0

    try:
        HashMap = snappy.jpy.get_type('java.util.HashMap')
        # Get snappy Operators
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
        print("Snappy operators initialized")
    except:
        raise Sentinel1PreprocessingError('Problem initialyzing snappy')

    if opt == 'orb':
        try:
            apply_orbit_file(file_in, file_out)
        except BaseException as e:
            raise Sentinel1PreprocessingError('Problem applying the orbit file') from e
    elif opt == 'brd':
        try:
            border_noise_removal(file_in,  file_out)
        except:
            raise Sentinel1PreprocessingError('Problem with the noise removal')

    elif opt == 'the':
        try:
            thermal_noise_removal(file_in,  file_out)
        except:
            raise Sentinel1PreprocessingError('Problem with the thermal noise removal')

    elif opt == 'cal':
        try:
            do_calibration(file_in,  file_out)
        except:
            raise Sentinel1PreprocessingError('Problem with the calibration')

    elif opt == 'tc':
        try:
            range_doppler_terrain_correction(file_in,  file_out)
        except:
            raise Sentinel1PreprocessingError('Problem with the terrain correction')

    elif opt == 'spk':
        try:
            speckle_filter(file_in,  file_out)
        except:
            raise Sentinel1PreprocessingError('Problem with speckle filtering')

    elif opt == 'db':
        try:
            convert_dB(file_in,  file_out)
        except:
            raise Sentinel1PreprocessingError('Problem with decibel conversion')
    else:
        raise Sentinel1PreprocessingError('not a valid option')

    return 0


if __name__ == "__main__":
    # import os
    # assert 'PROJ_LIB' in os.environ.keys()
    # argv[0] is the filepath of this.
    args = sys.argv[1:]
    print(args)
    sentinel_1_pre_processing_with_snappy(*args)
