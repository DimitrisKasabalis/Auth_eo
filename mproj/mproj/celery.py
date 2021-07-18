from __future__ import absolute_import

import os

from celery import Celery
from celery.schedules import crontab
from django.conf import settings

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
        'schedule': crontab(minute='*/15')
    },
}
