from .celery import app as celery_app, handles_task_publish

__version__ = '0.2.0'
__all__ = ('celery_app',)
