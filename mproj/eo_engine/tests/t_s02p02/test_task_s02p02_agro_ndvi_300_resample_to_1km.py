import shutil
import time
from datetime import datetime
from pathlib import Path

from celery.contrib.testing.worker import start_worker
from django.core.files import File
from django.test import override_settings, TransactionTestCase
from django.utils.timezone import now

from eo_engine.models import EOProduct, EOSource, EOSourceGroupChoices, Credentials, \
    EOProductStateChoices, EOProductGroupChoices
from eo_engine.tasks import task_s02p02_agro_nvdi_300_resample_to_1km
from mproj.celery import app

THIS_FOLDER = Path(__file__).parent
TEST_MEDIA_ROOT = THIS_FOLDER / 'test_media_root'
TEST_FILE = THIS_FOLDER / 'sample_data' / '20201221_SE3_AFR_0300m_0010_NDVI.nc'


@override_settings(MEDIA_ROOT=TEST_MEDIA_ROOT)
class TestTaskS02P02AgroNvdi300ResampleTo1km(TransactionTestCase):

    @classmethod
    def setUpClass(cls):
        cls.celery_worker = start_worker(app, shutdown_timeout=1800, perform_ping_check=False)
        cls.celery_worker.__enter__()
        super().setUpClass()
        assert TEST_FILE.exists()
        TEST_MEDIA_ROOT.mkdir(exist_ok=True)

    # @classmethod
    # def setUpTestData(cls):
        # <editor-fold desc="cls-eo-source">
        product_source = cls.product_source = EOProduct.objects.create(
            filename=TEST_FILE.name,
            output_folder='S2_P02/NDVI_300',
            group=EOProductGroupChoices.S02P02_NDVI_300M_V3_AFR,
            datetime_creation=datetime(year=2020, month=12, day=10),
            task_name='doesn-matter',
            state=EOProductStateChoices.READY,
        )
        product_source.file.save(name=TEST_FILE.name, content=File(TEST_FILE.open('rb')), save=True)
        # </editor-fold>
        product_source.refresh_from_db()
        cls.eo_product: EOProduct = EOProduct.objects.filter(eo_products_inputs=product_source).first()
        assert cls.eo_product.state == EOProductStateChoices.AVAILABLE
        task = task_s02p02_agro_nvdi_300_resample_to_1km \
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
        self.assertEqual(self.eo_product.state, EOProductStateChoices.READY)
