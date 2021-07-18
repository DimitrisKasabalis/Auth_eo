from django.urls import path

from . import views

app_name = 'eo_engine'

urlpatterns = [
    path('', views.hello, name='main-page'),
    path('spiders/', views.list_spiders, name='list-spiders'),
    path('eosources/', views.list_eosources, name='list-eosources'),
    path('eoproducts/', views.list_eoproducts, name='list-eoproducts'),
    path('trigger/spider_crawl/<str:spider_name>', views.trigger_spider, name='trigger-spider'),
    path('trigger/download_eosource/<str:filename>', views.trigger_download_eosource, name='trigger-dl-eosource'),
    path('trigger/eoproduct_genenration/<str:filename>', views.trigger_generate_eoproduct,
         name='trigger-eoproduct_generation'),
    path('delete_file/<str:file_type>/<str:filename>', views.delete_file, name='delete-file')
]
