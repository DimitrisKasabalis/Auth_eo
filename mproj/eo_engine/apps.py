from django.apps import AppConfig


class EoEngineConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'eo_engine'

    # noinspection PyUnresolvedReferences
    def ready(self):
        from mproj import celery_app
