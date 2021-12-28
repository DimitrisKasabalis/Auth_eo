from django.urls import path, include
from django_celery_results import urls as celery_urls
from . import views

app_name = 'eo_engine'

urlpatterns = [
    path('', views.main_page, name='main-page'),
    path('crawler/configure/<str:group_name>', views.configure_crawler, name='crawler-configure'),
    path('crawler/triger/<str:group_name>', views.trigger_crawler, name='trigger-spider'),
    path('credentials/list', views.utilities_view_post_credentials, name='credentials-list'),
    path('pipeline/<int:pipeline_pk>/inputs', views.pipeline_inputs, name='pipeline-inputs-list'),
    path('pipeline/<int:pipeline_pk>/outputs', views.pipeline_outputs, name='pipeline-outputs-list'),
    path('tasks/revoke/<str:task_id>', views.view_revoke_task, name='revoke-task'),
    path('tasks/submit', views.submit_task, name='submit-task'),
    path('utilities/delete_file/<str:resource_type>/<int:pk>', views.delete_file, name='delete-file'),
    path('utilities/refresh-rows', views.utilities_save_rows, name='refresh-rows'),
    path('utilties/create-wapor/<str:product>', views.create_wapor_entry, name='create-wapor'),

    # json responses
    # 'c/task/done/<task_pattern:task_id>/'
    # 'c/task/status/<task_pattern:task_id>/'
    path('c/', include(celery_urls))
]
