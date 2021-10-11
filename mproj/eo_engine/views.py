import logging
from typing import Literal, Callable

from celery.result import AsyncResult
from celery.utils.serialization import strtobool
from django.contrib import messages
from django.http import QueryDict
from django.shortcuts import render, redirect
# Create your views here.
from django.urls import reverse
from more_itertools import collapse

from eo_engine.common.tasks import get_task_ref_from_name
from eo_engine.models import EOSource, EOProduct

logger = logging.getLogger('eo_engine.frontend_ops')


def hello(request):
    from eo_engine.common.misc import list_spiders
    from eo_engine.tasks import task_init_spider, task_sftp_parse_remote_dir

    url_template = '{base_url}?{querystring}'

    task_init_spider_name = task_init_spider.name

    from eo_engine.models import EOProductGroupChoices
    from eo_engine.models import EOSourceGroupChoices
    context = {
        'eo_products_grps_value_label': EOProductGroupChoices.choices,
        'eo_sources_grps_value_label': EOSourceGroupChoices.choices,
    }

    scrappers = {}
    spider_list = list_spiders()
    for spider in spider_list:
        query_dictionary = QueryDict('', mutable=True)
        query_dictionary.update(
            task_name=task_init_spider_name,
            spider_name=spider
        )
        url = url_template.format(
            base_url=reverse("eo_engine:submit-task"),
            querystring=query_dictionary.urlencode()
        )
        scrappers[spider] = url

    # extra scrappers
    # scrappers[LABEL] = URL
    q = QueryDict('', mutable=True)
    q.update(
        task_name=task_sftp_parse_remote_dir.name,
        remote_dir='sftp://safmil.ipma.pt/home/safpt/OperationalChain/LSASAF_Products/DMET'
    )
    scrappers['LSAF'] = url_template.format(
        base_url=reverse("eo_engine:submit-task"),
        querystring=q.urlencode()
    )

    context.update(scrappers=scrappers)
    return render(request,
                  "home.html", context=context
                  )


def list_eosources(request, product_group=None):
    from .models import EOSource
    from .models import GeopTask
    # default order [product, date]
    qs = EOSource.objects.all().prefetch_related('task')
    if product_group:
        qs = qs.filter(group=product_group)

    context = {'eo_sources': qs,
               'valid_status_to_cancel': [GeopTask.TaskTypeChoices.STARTED.value,
                                          GeopTask.TaskTypeChoices.SUBMITTED.value]
               }

    return render(request, 'list_eosources.html', context=context)


def list_eoproducts(request, product_group=None):
    from .models import EOProduct
    qs = EOProduct.objects.all()
    if product_group:
        qs = qs.filter(group=product_group)
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
        messages.add_message(request=request,
                             level=messages.INFO,
                             message=f"Removed {str(result['eo_source'])} eo_source and {str(result['eo_product'])}")
    except AfriCultuReSFileNotExist:
        messages.add_message(request, messages.ERROR,
                             "File does not exist to delete ")
    except AfriCultuReSFileInUse:
        messages.add_message(request, messages.ERROR,
                             "File in use, did not delete")

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


def list_crawelers(request):
    from scrapy.spiderloader import SpiderLoader
    from scrapy.utils.project import get_project_settings
    # requires SCRAPY_SETTINGS_MODULE env variable
    # currently it's set in DJ's manage.py
    scrapy_settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(scrapy_settings)

    context = {
        "spiders": spider_loader.list()
    }

    return render(request, 'crawlers.html', context=context)


def submit_task(request):
    query_dictionary = QueryDict('', mutable=True)
    query_dictionary.update(**request.GET)
    next_page = query_dictionary.pop('next_page', reverse('eo_engine:main-page'))
    task_name = query_dictionary.pop('task_name').pop(0)

    task_kwargs = {}
    for k, v in query_dictionary.items():
        param = list(collapse(v))
        task_kwargs[k] = param[0] if len(param) == 1 else tuple(param)

    task = get_task_ref_from_name(task_name).s(**task_kwargs)
    job = task.apply_async()
    messages.add_message(
        request=request, level=messages.SUCCESS,
        message=f'Task {task.name} with task id: {job} successfully submitted')

    return redirect(next_page)


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


def utilities_save_rows(request):
    # 'save' all rows in EOSource/Product to trigger the post-save signal
    all_eosource = EOSource.objects.all()
    all_eoproduct = EOProduct.objects.all()
    for model in [all_eoproduct, all_eosource]:
        for e in model:
            e.save()
    messages.info(request, 'Done!')
    return redirect(reverse('eo_engine:main-page'))


def utilities_view_post_credentials(request):
    if request.method == "GET":
        context = {}
        from .models import Credentials
        from .forms import CredentialsUsernamePassowordForm, CredentialsAPIKEYForm
        all_credentials = Credentials.objects.all()
        forms = []
        for c in all_credentials.filter(type=Credentials.CredentialsTypeChoices.USERNAME_PASSWORD):
            forms.append(CredentialsUsernamePassowordForm(instance=c))
        context['userpass_forms'] = forms

        forms = []
        for c in all_credentials.filter(type=Credentials.CredentialsTypeChoices.API_KEY):
            forms.append(CredentialsAPIKEYForm(instance=c))
        context['apikey_forms'] = forms
        return render(request, "credentials.html", context=context)

    if request.method == "POST":
        try:
            messages.success(request, 'change submitted')
        except:
            messages.error(request, 'error')
        finally:
            return redirect(reverse('eo_engine:credentials'))
