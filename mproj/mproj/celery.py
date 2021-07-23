from __future__ import absolute_import

import inspect
import os

from celery import Celery
from celery.schedules import crontab
from celery.signals import before_task_publish
from django.conf import settings

from eo_engine.signals import logger

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mproj.settings')
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'eo_scraper.settings')

app = Celery('Celery', task_cls='eo_engine.task_managers:BaseTaskWithRetry')

app.config_from_object('django.conf:settings')

app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

app.conf.beat_schedule = {
    'init_spider': {
        'task': 'eo_engine.tasks.task_init_spider',
        'schedule': crontab(minute=0, hour=1, day_of_month='*/3'),  # 1:00 am every 3rd day
        'kwargs': {
            'spider_name': 'c_gsl_ndvi300-v2-glob-spider'
        },
    },
    'queue_download_available_eosource': {
        'task': 'eo_engine.tasks.task_schedule_download_eosource',
        'schedule': crontab(minute='0', hour='*/8')  # 0 minute every  8th hour
    },
    'queue_create_eoproduct': {
        'task': 'eo_engine.tasks.task_schedule_create_eoproduct',
        'schedule': crontab(minute='*/2')
    },
}


@before_task_publish.connect
def handles_task_publish(sender: str = None, headers=None, body=None, **kwargs):
    """ Essential procedures after a task was published on the broker.
    This function is connected to the  after_task_publish signal """
    # set the default Django settings module for the 'celery' program.
    # print('Before connect signal firing up')
    from eo_engine.models import GeopGroupTask, GeopTask

    print('handles_task_publish')

    if not (sender.startswith('eo_engine') or
            sender.startswith('app')):
        return
    info = headers

    task_id = info['id']
    group_task_id = info.get('group', None)
    root_id = None if task_id == info.get('root_id') else info.get('root_id', task_id)
    if group_task_id:
        group_task_obj, created = GeopGroupTask.objects.get_or_create(group_task_id=group_task_id, root_id=root_id)

    else:
        group_task_obj = None
    # in case of Retry, celery will try to
    # republish the task with the same task_id
    # _kwargs_repr = json.dumps(body[1])

    task_obj, created = GeopTask.objects.get_or_create(task_id=task_id)

    if created:
        _data = dict()
        _data.update(task_name=info['task'],
                     task_args=info['argsrepr'],
                     task_kwargs=body[1],
                     parent_id=info['parent_id'],
                     root_id=info['root_id'],
                     group_task=group_task_obj
                     )
        for k, v in _data.items():
            setattr(task_obj, k, v)
        task_obj.save()
        logger.info(f'{inspect.stack()[0][3]}: Created task: {info["id"]}')
    else:
        logger.info(f'{inspect.stack()[0][3]}: Task {info["id"]} found.')
    return
