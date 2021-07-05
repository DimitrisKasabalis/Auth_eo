from __future__ import absolute_import
import os
from celery import Celery
from celery.schedules import crontab
from django.conf import settings

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mproj.settings')
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'eo_scraper.settings')
app = Celery('Celery')

app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

app.conf.beat_schedule = {
    # 'op_discover_ndvi-300-21_dataset': {
    #     'task': 'eo_engine.tasks.task_start_scrape',
    #     'schedule': crontab(minute=0, hour=1, day_of_month='*/3'),
    #     'kwargs': {
    #         'spider_name': 'ndvi-300m-v2-spider'
    #     },
    # },
    'op_schedule_download': {
        'task': 'eo_engine.tasks.task_schedule_download',
        'schedule': crontab(minute=5),
        'kwargs': {'params': (
            ('NDVI-300m-v2-GLOB', '1/6/2020'),
            ('WB-100m-v1-GLOB', '1/11/2020'),
        )
        }
    }
}
