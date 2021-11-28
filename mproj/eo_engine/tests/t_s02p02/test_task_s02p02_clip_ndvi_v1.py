import shutil
import time
from datetime import datetime
from pathlib import Path

from celery.contrib.testing.worker import start_worker
from django.core.files import File
from django.test import override_settings, TestCase
from django.utils.timezone import now

from eo_engine.models import EOProduct, EOSource, EOSourceStateChoices, EOSourceGroupChoices, Credentials, \
    EOProductStateChoices
from eo_engine.tasks import task_s02p02_c_gls_ndvi_300_clip
from mproj.celery import app

THIS_FOLDER = Path(__file__).parent
TEST_MEDIA_ROOT = THIS_FOLDER / 'test_media_root'
TEST_FILE = THIS_FOLDER / 'sample_data' / 'c_gls_NDVI300_202012210000_GLOBE_OLCI_V2.0.1.nc'


@override_settings(MEDIA_ROOT=TEST_MEDIA_ROOT)
class TaskS06P01Wb100mToCountryTest(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.celery_worker = start_worker(app, shutdown_timeout=1800, perform_ping_check=False)
        cls.celery_worker.__enter__()
        super().setUpClass()
        assert TEST_FILE.exists()
        TEST_MEDIA_ROOT.mkdir(exist_ok=True)

    @classmethod
    def setUpTestData(cls):
        # <editor-fold desc="cls-eo-source">
        f = cls.eo_source = EOSource.objects.create(
            state=EOSourceStateChoices.AvailableLocally,
            group=EOSourceGroupChoices.C_GLS_NDVI_300M_V2_GLOB,
            filename=TEST_FILE.name,
            domain='test-domain',
            datetime_seen=now(),
            filesize_reported=TEST_FILE.stat().st_size,
            datetime_reference=datetime(year=2020, month=12, day=10),
            url=f'http://non-existant-sftp/path/{TEST_FILE.name}',  # this is not import
            credentials=Credentials.objects.first(),  # doesn't matter also
        )
        f.file.save(name=TEST_FILE.name, content=File(TEST_FILE.open('rb')), save=True)
        # </editor-fold>

        cls.eo_source.file.save(name=TEST_FILE.name, content=File(TEST_FILE.open('rb')))

        cls.eo_product: EOProduct = EOProduct.objects.first()
        assert cls.eo_product.state == EOProductStateChoices.Available
        task = task_s02p02_c_gls_ndvi_300_clip \
            .s(eo_product_pk=cls.eo_product.pk, **cls.eo_product.task_kwargs)
        time.sleep(1)
        job = task.apply()  # don't use apply_async
        cls.last_result = job.get(timeout=180)  # 2 mins to finish the job

    @classmethod
    def tearDownClass(cls):
        cls.celery_worker.__exit__(None, None, None)
        shutil.rmtree(TEST_MEDIA_ROOT)
        super().tearDownClass()

    def test_check_eo_products_table(self):
        self.assertEqual(EOProduct.objects.all().count(), 2)

    def test_check_eo_sources_table(self):
        self.assertEqual(EOSource.objects.filter(group=EOSourceGroupChoices.C_GLS_NDVI_300M_V2_GLOB).count(), 1)

    def test_state(self):
        self.eo_product.refresh_from_db()
        self.assertEqual(self.eo_product.state,EOProductStateChoices.Ready)
