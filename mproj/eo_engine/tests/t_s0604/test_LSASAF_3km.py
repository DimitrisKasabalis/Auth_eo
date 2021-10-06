import bz2
import tempfile
from pathlib import Path
from eo_engine.models import EOSource

from .. import BaseTest

THIS_FOLDER = Path(__file__).parent
SAMPLE_BZ2 = THIS_FOLDER / 'sample_data' / 'HDF5_LSASAF_MSG_DMET_MSG-Disk_202109230000.bz2'


# noinspection PyPep8Naming
class Test_S06P04_3KM(BaseTest):

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_process_bz2(self):
        """ un BZ2, then geo ref, then warp, then to netcdf"""
        from eo_engine.common.contrib.h5georef import H5Georef
        with tempfile.NamedTemporaryFile('wb') as hdf5File, \
                tempfile.TemporaryDirectory() as temp_dir:
            hdf5File.write(bz2.decompress(SAMPLE_BZ2.read_bytes()))
            hdf5File.flush()

            h5g = H5Georef(Path(hdf5File.name))
            samples = h5g.get_sample_coords()
            georef_file = h5g.georef_gtif(samples)

            if georef_file != -1:
                # print("\nProjecting...")
                warped_file = h5g.warp('EPSG:4326')

                if warped_file != -1:
                    # print("\nConverting to netcdf...")
                    file_nc = Path(temp_dir) / Path(SAMPLE_BZ2.name[5:-8]).with_suffix(".nc")
                    netcdf_file = h5g.to_netcdf(file_nc)
        self.assertGreater(netcdf_file.stat().st_size, 10)  # file exists, and has some size

        print('--done--')

    def test_scan_remote_dir(self):
        from eo_engine.tasks import task_sftp_parse_remote_dir
        from eo_engine.tasks import task_download_file
        url = 'sftp://safmil.ipma.pt/home/safpt/OperationalChain/LSASAF_Products/DMET'
        t = task_sftp_parse_remote_dir(remote_dir=url)

        all = EOSource.objects.all()
        t2 = task_download_file(eo_source_pk=all.first().pk)

        print('--done--')
