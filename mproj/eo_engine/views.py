import os
from typing import Literal
import logging
from celery.result import AsyncResult
from django.http import HttpResponse
from django.shortcuts import render, redirect

# Create your views here.
from django.urls import reverse

logger = logging.getLogger('eo_engine.frontend_ops')


def hello(request):
    return render(request, "hello.html")


def list_eosources(request):
    from .models import EOSource
    qs = EOSource.objects.all()

    context = {'eo_sources': qs}

    return render(request, 'list_eosources.html', context=context)


def list_eoproducts(request):
    from .models import EOProduct
    qs = EOProduct.objects.all()

    context = {'eo_products': qs}

    return render(request, 'list_eoproducts.html', context=context)


def delete_file(request, file_type: Literal['eosource', 'eoproduct'], filename: str):
    if file_type == "eosource":
        raise NotImplementedError()
        from .models import EOSource
        obj = EOSource.objects.get(filename=filename)
    else:
        from .models import EOProduct
        from .models import EOProductStatusChoices
        obj = EOProduct.objects.get(filename=filename)
        if obj.status == EOProductStatusChoices.Ready:
            logger.info(f'Removing file{obj.filename}')
            obj.file.delete()
            obj.status = EOProductStatusChoices.Available
            obj.save()
        return redirect('eo_engine:list-eoproducts')


def trigger_generate_eoproduct(request, filename):
    from eo_engine.common import get_task_ref_from_name
    from .models import EOProduct, EOProductStatusChoices
    eo_product = EOProduct.objects.get(filename=filename)
    task = get_task_ref_from_name(eo_product.task_name).s(eo_product_pk=eo_product.pk, **eo_product.task_kwargs)
    job: AsyncResult = task.apply_async()
    eo_product.status = EOProductStatusChoices.Scheduled
    eo_product.save()
    context = {'card_info':
                   {'task_name': task.name,
                    'param': eo_product.task_kwargs,
                    'job_id': job.task_id
                    },
               'previous_page': {'url': reverse('eo_engine:list-eoproducts'), 'label': 'Products List'},
               'main_page': {'url': reverse('eo_engine:main-page'), 'label': "Main Page"}

               }

    return render(request, 'task_triggered.html', context)


def trigger_download_eosource(request, filename):
    from .tasks import task_download_file
    from .models import EOSource, EOSourceStatusChoices
    obj = EOSource.objects.get(filename=filename)
    obj.status = EOSourceStatusChoices.scheduledForDownload
    obj.save()

    task = task_download_file.s(filename=filename)
    job: AsyncResult = task.apply_async()

    context = {'card_info':
                   {'task_name': task.name,
                    'param': filename,
                    'job_id': job.task_id
                    },
               'previous_page': {'url': reverse('eo_engine:list-eosources'), 'label': 'Sources List'},
               'main_page': {'url': reverse('eo_engine:main-page'), 'label': "Main Page"}
               }
    return render(request, 'task_triggered.html', context)


def list_spiders(request):
    from scrapy.spiderloader import SpiderLoader
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    # requires SCRAPY_SETTINGS_MODULE env variable
    # currently it's set in DJ's manage.py
    scrapy_settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(scrapy_settings)

    context = {
        "spiders": spider_loader.list()
    }

    return render(request, 'spiders.html', context=context)


def trigger_spider(request, spider_name: str):
    from .tasks import task_init_spider

    task = task_init_spider.s(spider_name=spider_name)
    job: AsyncResult = task.apply_async()

    context = {'card_info':
                   {'task_name': task.name,
                    'param': spider_name,
                    'job_id': job.task_id
                    }
               }

    return render(request, 'task_triggered.html', context)
