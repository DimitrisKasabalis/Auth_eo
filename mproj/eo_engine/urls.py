from django.urls import path, include
from django_celery_results import urls as celery_urls
from . import views

app_name = 'eo_engine'

urlpatterns = [
    path('', views.hello, name='main-page'),
    path('spiders/', views.list_spiders, name='list-spiders'),
    path('eosources/', views.list_eosources, name='list-eosources'),
    path('eoproducts/', views.list_eoproducts, name='list-eoproducts'),
    path('trigger/spider_crawl/<str:spider_name>', views.trigger_spider, name='trigger-spider'),
    path('trigger/download_eosource/<int:eo_source_pk>', views.trigger_download_eosource, name='trigger-dl-eosource'),
    path('trigger/eoproduct_genenration/<str:filename>', views.trigger_generate_eoproduct,
         name='trigger-eoproduct_generation'),
    path('delete_file/<str:resource_type>/<int:pk>', views.delete_file, name='delete-file'),
    path('tasks/revoke/<str:task_id>', views.view_revoke_task, name='revoke-task'),

    # json responses
    # 'c/task/done/<task_pattern:task_id>/'
    # 'c/task/status/<task_pattern:task_id>/'
    path('c/', include(celery_urls))
]
