from fnmatch import fnmatch

from celery import Task
from celery.utils.log import get_task_logger
from django.utils import timezone

from eo_engine.common.tasks import is_process_task
from eo_engine.models import GeopTask, EOProduct, EOProductStateChoices

logger = get_task_logger(__name__)


class BaseTaskWithRetry(Task):

    # doc: https://docs.celeryproject.org/en/latest/userguide/application.html#abstract-tasks

    def __call__(self, *args, **kwargs):
        task_id = self.request.id
        if task_id is None:
            # The task was called directly
            return self.run(*args, **kwargs)

        # eager mode
        if self.request.is_eager:
            return self.run(*args, **kwargs)

        # The task namespace is eo_engine or mproj
        if self.name.startswith('eo_engine') or self.name.startswith('mproj'):
            task: GeopTask = GeopTask.objects.get(task_id=task_id)
            now = timezone.now()
            task.datetime_started = now
            task.status = task.TaskTypeChoices.STARTED

            group_task = task.group_task
            if group_task and group_task.datetime_started is None:
                group_task.datetime_started = now
                group_task.save()
            task.save()

        if is_process_task(self.name):  # ie eo_engine.tasks.task_s02p02_c_gls_ndvi_300_clip
            # mark generating product as 'GENERATING'
            eo_product_pk = kwargs['eo_product_pk']
            eo_product = EOProduct.objects.get(pk=eo_product_pk)
            logger.info(f"Marking product {eo_product} as 'GENERATING'")
            eo_product.state = EOProductStateChoices.Generating
            eo_product.save()

        return self.run(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        return

    def on_success(self, retval, task_id, args, kwargs):
        try:
            task = GeopTask.objects.get(task_id=task_id)
        except GeopTask.DoesNotExist:
            return
        task.datetime_finished = timezone.now()
        try:
            task.time_to_complete = \
                (task.datetime_finished - task.datetime_started).__str__()
        except:
            task.time_to_complete = None
        task.status = task.TaskTypeChoices.SUCCESS

        # mark product available on success
        if is_process_task(self.name):
            # mark generating product as 'GENERATING'
            eo_product_pk = kwargs['eo_product_pk']
            eo_product = EOProduct.objects.get(pk=eo_product_pk)
            eo_product.state = EOProductStateChoices.Ready
            eo_product.save()

        task.save()

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """This is run by the worker when the task is to be retried."""
        try:
            task = GeopTask.objects.get(task_id=task_id)
        except GeopTask.DoesNotExist:
            logger.info('on_retry: task_id is not tracked by app')
            return
        task.status = task.TaskTypeChoices.RETRY
        task.retries += 1
        task.save()

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """ This is run by the worker when the task fails."""
        try:
            task = GeopTask.objects.get(task_id=task_id)
        except GeopTask.DoesNotExist:
            logger.info('on_failure: task_id is not tracked by app')
            return
        task.datetime_finished = timezone.now()
        task.status = task.TaskTypeChoices.FAILURE
        task.save()

        # mark generating product as failed
        if is_process_task(self.name):
            eo_product_pk = kwargs['eo_product_pk']
            eo_product = EOProduct.objects.get(pk=eo_product_pk)
            eo_product.state = EOProductStateChoices.Failed
            eo_product.save()
