import time
from datetime import datetime
from pathlib import Path

from celery.contrib.testing.worker import start_worker
from django.test import override_settings, TransactionTestCase
from django.utils.timezone import now

from eo_engine.common.contrib import waporv2
from eo_engine.common.download import download_wapor_eosource
from eo_engine.errors import AfriCultuReSRetriableError
from eo_engine.models import EOSource, EOSourceStateChoices, EOSourceGroupChoices, Credentials
from eo_engine.tasks import task_download_file
from mproj.celery import app

API_KEY = 'f90c385495d61b7afd7518864b57ea34097d5b12edef76e66d3abb9d859785ccb020b606edb4574d'


class WAPORClient(TransactionTestCase):

    def test_client(self):
        with waporv2.WAPORv2Client() as wp:
            res = wp.get('http://www.google.com')
            res.raise_for_status()

    def test_client_auth(self):
        with waporv2.WAPORv2Client() as wp:
            product = 'L2_QUAL_NDVI_D_1904_KEN.tif'
            rv = waporv2.WAPORRemoteVariable.from_filename(product)
            rv.set_api_key(API_KEY)
            rj = rv.submit()
            print(rj)


TEST_MEDIA_ROOT = Path(__file__).parent / 'test_media_root'
TEST_FILE = Path(__file__).parent / 'sample_data/L2_QUAL_NDVI_D_1904_KEN.tif'


@override_settings(
    MEDIA_ROOT=TEST_MEDIA_ROOT
)
class WAPORWorkFlow(TransactionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        TEST_MEDIA_ROOT.mkdir(exist_ok=True)

        cls.celery_worker = start_worker(app, shutdown_timeout=1800, perform_ping_check=False)
        cls.celery_worker.__enter__()

    @classmethod
    def tearDownClass(cls):
        super(WAPORWorkFlow, cls).tearDownClass()

    def setUp(self) -> None:
        super().setUp()

    def test_workflow(self):
        eo_source = create_wapor_object(TEST_FILE)
        eo_source.save()
        from eo_engine.common.contrib.waporv2 import WAPORRemoteJob
        with self.assertRaises(AfriCultuReSRetriableError):
            download_wapor_eosource(eo_source.pk)
        # self.eo_source.refresh_from_db()
        self.assertTrue(str(eo_source.url).startswith('wapor://'))
        remote_job = WAPORRemoteJob.from_eosource_url(eo_source.url)
        print(remote_job.job_url())
        time.sleep(15)  # the job if no queue takes about 10 secs to complete
        download_wapor_eosource(eo_source.pk)

    def test_workflow_task(self):
        eo_source = create_wapor_object(TEST_FILE)
        eo_source.save()
        from eo_engine.tasks import task_download_file
        task = task_download_file.s(eo_source.pk)
        job = task.apply_async()

        job.get(timeout=120000000)

        print('--done--')
