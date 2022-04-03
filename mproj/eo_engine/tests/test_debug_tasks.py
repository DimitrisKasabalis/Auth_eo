from django.test import TransactionTestCase, SimpleTestCase

import eo_engine.tasks.example
from eo_engine import tasks
from mproj.celery import app
from celery.contrib.testing.worker import start_worker

from eo_engine.models import GeopTask


class FooTaskTestCase(TransactionTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.celery_worker = start_worker(app, shutdown_timeout=1800, perform_ping_check=False)
        cls.celery_worker.__enter__()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.celery_worker.__exit__(None, None, None)

    def setUp(self):
        super().setUp()
        self.task = eo_engine._tasks.example.task_debug_failing.delay(1)
        self.results = self.task.get(propagate=False) # dont raise exception

    def test_state(self):
        assert GeopTask.objects.all().count() == 1
        assert GeopTask.objects.first().status == GeopTask.TaskTypeChoices.FAILURE
