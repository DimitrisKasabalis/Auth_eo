import shutil
import time
from datetime import datetime
from pathlib import Path

from celery.contrib.testing.worker import start_worker
from django.core.files import File
from django.test import override_settings, TransactionTestCase
from django.utils.timezone import now

from eo_engine.models import EOSource, EOSourceStateChoices, EOSourceGroupChoices, Credentials, EOProduct
from mproj.celery import app

from eo_engine.tasks import task_s0601_wb_100m

TEST_MEDIA_ROOT = Path(__file__).parent / 'test_media_root'
# ftp://ftp.globalland.cls.fr/home/glbland_ftp/Core/SIRS/dataset-sirs-wb-nrt-100m  # 800 mb
TEST_FILE = Path(__file__).parent / 'sample_data/c_gls_WB100_202010010000_GLOBE_S2_V1.0.1.nc'


@override_settings(
    MEDIA_ROOT=TEST_MEDIA_ROOT
)
class TaskS06P01Wb100mToCountryTest(TransactionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        TEST_MEDIA_ROOT.mkdir(exist_ok=True)
        cls.eo_source = EOSource.objects.create(
            state=EOSourceStateChoices.AVAILABLE_LOCALLY,
            group=EOSourceGroupChoices.MSG_3km_GLOB,
            filename=TEST_FILE.name,
            domain='test-domain',
            datetime_seen=now(),
            filesize_reported=TEST_FILE.stat().st_size,
            datetime_reference=datetime(year=2021, month=9, day=23),
            url=f'stfp://non-existant-sftp/path/{TEST_FILE.name}',
            credentials=Credentials.objects.first(),  # doesn't matter

        )
        cls.eo_source.file.save(name=TEST_FILE.name, content=File(TEST_FILE.open('rb')))
        cls.celery_worker = start_worker(app, shutdown_timeout=1800, perform_ping_check=False)
        cls.celery_worker.__enter__()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.celery_worker.__exit__(None, None, None)
        shutil.rmtree(TEST_MEDIA_ROOT)

    def setUp(self) -> None:
        super().setUp()
        # Run the task here
        eo_products = EOProduct.objects.all()
        for eo_product in eo_products:
            task = task_s0601_wb_100m.s(eo_product_pk=eo_product.pk,
                                        **eo_product.task_kwargs
                                        )
            time.sleep(1)
            job = task.apply()

        # self.last_result = job.get(timeout=180)

    def tearDown(self) -> None:
        pass

    def test_assert_product_exists(self):
        p_qs = EOProduct.objects.all()

        self.assertTrue(p_qs.exists())
        self.assertEqual(p_qs.count(), 8)

    def test_check_outouts(self):
        output_folder = TEST_MEDIA_ROOT
        files = list(output_folder.glob('*.nc'))

        self.assertEqual(len(files), 8)
