from django.apps import AppConfig


class EoEngineConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'eo_engine'

    # noinspection PyUnresolvedReferences
    def ready(self):
        from mproj import celery_app
        from eo_engine.signals import before_task_publish
        # dir(celery_app)
