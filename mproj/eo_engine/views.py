import logging
import os
from pathlib import Path
from typing import Literal, Callable

from celery.result import AsyncResult
from celery.utils.serialization import strtobool
from django.contrib import messages
from django.shortcuts import render, redirect

# Create your views here.
from django.urls import reverse

logger = logging.getLogger('eo_engine.frontend_ops')


def hello(request):
    return render(request, "hello.html")


def list_eosources(request):
    from .models import EOSource
    from .models import GeopTask
    # default order [product, date]
    qs = EOSource.objects.all().prefetch_related('task')

    context = {'eo_sources': qs,
               'valid_status_to_cancel': [GeopTask.TaskTypeChoices.STARTED.value,
                                          GeopTask.TaskTypeChoices.SUBMITTED.value]
               }

    return render(request, 'list_eosources.html', context=context)


def list_eoproducts(request):
    from .models import EOProduct
    qs = EOProduct.objects.all()

    context = {'eo_products': qs}

    return render(request, 'list_eoproducts.html', context=context)


def delete_file(request, resource_type: Literal['eo_source', 'eo_product'], pk: int):
    from eo_engine.common.db_ops import delete_eo_product, delete_eo_source
    from eo_engine.errors import AfriCultuReSFileNotExist, AfriCultuReSFileInUse

    fun: Callable
    if resource_type == "eo_source":
        fun = delete_eo_source
    else:
        fun = delete_eo_product

    try:
        result = fun(pk)
        messages.add_message(request, messages.info,
                             f"Removed {result['eo_source']} eo_source and {result['eo_product']}")
    except AfriCultuReSFileNotExist:
        messages.add_message(request, messages.ERROR,
                             "File does not exist to delete ")
    except AfriCultuReSFileInUse:
        messages.add_message(request, messages.ERROR,
                             "File in use, did not delete")

    finally:
        return redirect('eo_engine:list-eosources')



def trigger_generate_eoproduct(request, filename):
    from eo_engine.common.tasks import get_task_ref_from_name
    from .models import EOProduct, EOProductStateChoices
    eo_product = EOProduct.objects.get(filename=filename)
    task = get_task_ref_from_name(eo_product.task_name).s(eo_product_pk=eo_product.pk, **eo_product.task_kwargs)
    job: AsyncResult = task.apply_async()
    eo_product.state = EOProductStateChoices.Scheduled
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


def trigger_download_eosource(request, eo_source_pk):
    from .tasks import task_download_file
    from .models import EOSource, EOSourceStateChoices

    template_header_title = 'Download file'

    obj = EOSource.objects.get(pk=eo_source_pk)
    task = task_download_file.s(eo_source_pk=eo_source_pk)
    job: AsyncResult = task.apply_async()
    obj.state = EOSourceStateChoices.ScheduledForDownload
    obj.save()

    context = {
        'header': template_header_title,
        'card_info':
            {'task_name': task.name,
             'param': eo_source_pk,
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


def view_revoke_task(request, task_id: str):
    from eo_engine.models import GeopTask
    from eo_engine.common.tasks import revoke_task

    return_page = request.GET.get('return_page', '')
    confirm = strtobool(request.GET.get('confirm', 'False'))
    context = {
        "text": None,
        "return_page": return_page,
        "next": None
    }
    try:
        task = GeopTask.objects.get(task_id=task_id)
    except GeopTask.DoesNotExist:
        context.update(
            text=f'the task_id {task_id} was not found',
        )
        return render(request, 'task_revoke.html', context=context)

    if confirm is False:
        context.update(
            text=f'are you sure?',
            next=f"{reverse('eo_engine:revoke-task', kwargs={'task_id': task_id})}?confirm=true&return_page={return_page}"
        )
    elif confirm is True:
        context.update(
            text=f'task{task_id} revoked!',
            next=reverse('eo_engine:list-eosources')
        )
        revoke_task(task_id, terminate=True)

    return render(request, 'task_revoke.html', context=context)

    # q = GeopTask.objects.filter(~Q(status='SUCCESS')).filter(~Q(task_name__contains='schedule')).filter(
    #     task_kwargs__filename="c_gls_NDVI300_202107010000_GLOBE_OLCI_V2.0.1.nc")

    # from mproj import celery_app as app

    # app.control.revoke()

    # return None
