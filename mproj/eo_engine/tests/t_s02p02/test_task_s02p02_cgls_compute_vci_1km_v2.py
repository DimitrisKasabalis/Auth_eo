import shutil
import time
from datetime import datetime
from pathlib import Path

from celery.contrib.testing.worker import start_worker
from django.core.files import File
from django.test import override_settings, TransactionTestCase

from eo_engine.common.tasks import get_task_ref_from_name
from eo_engine.models import EOProduct, EOSource, EOProductStateChoices, EOProductGroupChoices
from mproj.celery import app

THIS_FOLDER = Path(__file__).parent
TEST_MEDIA_ROOT = THIS_FOLDER / 'test_media_root'
TEST_FILE = THIS_FOLDER / 'sample_data' / '20201221_SE3_AFR_1000m_0010_NDVI.nc'


@override_settings(MEDIA_ROOT=TEST_MEDIA_ROOT)
class TestTaskS02P02CglsComputeVci1kmV2(TransactionTestCase):

    @classmethod
    def setUpClass(cls):
        cls.celery_worker = start_worker(app, shutdown_timeout=1800, perform_ping_check=False)
        cls.celery_worker.__enter__()
        super().setUpClass()
        assert TEST_FILE.exists()
        TEST_MEDIA_ROOT.mkdir(exist_ok=True)

        product_source = cls.product_source = EOProduct.objects.create(
            filename=TEST_FILE.name,
            output_folder='S2_P02/NDVI_1000',  # doesn't matter
            group=EOProductGroupChoices.AGRO_NDVI_1KM_V3_AFR,  # matters
            datetime_creation=datetime(year=2020, month=12, day=10),
            task_name='doesn-matter',  # doesn't matter
            state=EOProductStateChoices.Ready,
        )
        product_source.file.save(name=TEST_FILE.name, content=File(TEST_FILE.open('rb')), save=True)

        product_source.refresh_from_db()
        cls.eo_product: EOProduct = EOProduct.objects.filter(eo_products_inputs=product_source).first()
        # pre-flight checks
        assert cls.eo_product.state == EOProductStateChoices.Available
        task_ref = get_task_ref_from_name(cls.eo_product.task_name)

        task = task_ref.s(eo_product_pk=cls.eo_product.pk, **cls.eo_product.task_kwargs)
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
        self.assertEqual(EOProduct.objects.filter(group=EOProductGroupChoices.AGRO_VCI_1KM_V2_AFR).count(), 1)

    def test_state(self):
        self.eo_product.refresh_from_db()
        self.assertEqual(self.eo_product.state, EOProductStateChoices.Ready)
