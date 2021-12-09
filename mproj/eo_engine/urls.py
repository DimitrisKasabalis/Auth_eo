from django.urls import path, include
from django_celery_results import urls as celery_urls
from . import views

app_name = 'eo_engine'

urlpatterns = [
    path('', views.homepage, name='main-page'),
    path('credentials', views.utilities_view_post_credentials, name='credentials'),
    path('configure/crawler/<str:group_name>', views.configure_crawler, name='configure-crawler'),
    path('delete_file/<str:resource_type>/<int:pk>', views.delete_file, name='delete-file'),
    path('eoproducts/<str:product_group>', views.list_eoproducts, name='list-eoproducts'),
    path('eosources/<str:product_group>', views.list_eosources, name='list-eosources'),
    path('spiders/', views.list_crawelers, name='list-spiders'),
    path('tasks/revoke/<str:task_id>', views.view_revoke_task, name='revoke-task'),
    path('tasks/submit', views.submit_task, name='submit-task'),
    path('trigger/download_eosource/<int:eo_source_pk>', views.trigger_download_eosource, name='trigger-dl-eosource'),
    path('trigger/eoproduct_generation/<str:filename>', views.trigger_generate_eoproduct,
         name='trigger-eoproduct_generation'),
    path('trigger/spider_crawl/<str:spider_name>', views.trigger_spider, name='trigger-spider'),
    path('utilities/refresh-rows', views.utilities_save_rows, name='refresh-rows'),
    path('utilties/create-wapor/<str:product>', views.create_wapor_entry, name='create-wapor'),

    # json responses
    # 'c/task/done/<task_pattern:task_id>/'
    # 'c/task/status/<task_pattern:task_id>/'
    path('c/', include(celery_urls))
]
