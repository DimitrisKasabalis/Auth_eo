from datetime import datetime
from pathlib import Path
import shutil

from celery.contrib.testing.worker import start_worker
from django.core.files import File
from django.test import TransactionTestCase, override_settings
from django.utils.timezone import now

from mproj.celery import app

from eo_engine.tasks import task_s06p04_et_3km
from eo_engine.models import EOSource, EOSourceStateChoices, EOSourceGroupChoices, EOProduct
from eo_engine.models import Credentials

TEST_MEDIA_ROOT = Path(__file__).parent / 'test_media_root'
TEST_FILE = Path(__file__).parent / 'sample_data/HDF5_LSASAF_MSG_DMET_MSG-Disk_202109230000.bz2'


@override_settings(
    MEDIA_ROOT=TEST_MEDIA_ROOT
)
class TaskS06P04Test(TransactionTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        TEST_MEDIA_ROOT.mkdir(exist_ok=True)
        cls.eo_source = EOSource.objects.create(
            state=EOSourceStateChoices.AvailableLocally,
            group=EOSourceGroupChoices.MSG_3km_GLOB,
            filename=TEST_FILE.name,
            domain='test-domain',
            datetime_seen=now(),
            filesize_reported=TEST_FILE.stat().st_size,
            datetime_reference=datetime(year=2021, month=9, day=23),
            url=f'stfp://non-existant-sftp/path/{TEST_FILE.name}',
            credentials=Credentials.objects.first(),

        )
        cls.eo_source.file.save(name=TEST_FILE.name, content=File(TEST_FILE.open('rb')))
        cls.celery_worker = start_worker(app, shutdown_timeout=1800, perform_ping_check=False)
        cls.celery_worker.__enter__()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.celery_worker.__exit__(None, None, None)
        # shutil.rmtree(TEST_MEDIA_ROOT)

    def setUp(self) -> None:
        super().setUp()
        # Run the task here
        task = task_s06p04_et_3km.s(eo_product_pk=EOProduct.objects.first().pk)
        job = task.apply_async()

        self.result = job.get(timeout=180)

    def tearDown(self) -> None:
        pass

    def test_assert_product_exists(self):
        p_qs = EOProduct.objects.all()

        self.assertTrue(p_qs.exists())
        self.assertEqual(p_qs.count(), 1)
