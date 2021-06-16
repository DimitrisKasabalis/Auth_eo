from unittest import mock

from celery import chain
from celery.result import AsyncResult

from . import BaseTestWorker


class TestTasks(BaseTestWorker):
    celery_worker_perform_ping_check = True

    def test_add_task(self, initial_char='a'):
        from eo_engine.tasks import task_append_char
        with mock.patch('random.choice', return_value='b') as mocked_choice:
            task = chain(
                task_append_char.s(initial_char),
                task_append_char.s(),
                task_append_char.s(),
            )
            job: AsyncResult = task.apply_async()
            result: str = job.get()
            assert mocked_choice.call_count == 3
            assert result.startswith(initial_char)
            assert len(result) == 4
            assert job.state == 'SUCCESS'
