import sys

from celery import chain

from . import BaseTestWorker


class TestTasks(BaseTestWorker):
    celery_worker_perform_ping_check = True

    def test_add_task(self):
        from eo_engine.tasks import task_append_char

        task = chain(
            task_append_char.s('a'),
            task_append_char.s(),
            task_append_char.s(),
        )
        job = task.apply_async()
        job.get()

        assert 1 + 1 == 2
