import logging
from typing import Literal, Callable, Optional, Dict, Union

from celery.result import AsyncResult
from celery.utils.serialization import strtobool
from django.contrib import messages
from django.db import IntegrityError
from django.http import QueryDict
from django.shortcuts import render, redirect
from django.urls import reverse
from django.utils.datetime_safe import date
from more_itertools import collapse

from eo_engine.common.tasks import get_task_ref_from_name
from eo_engine.models import EOSource, EOProduct
from eo_engine.models.factories import create_wapor_object
from eo_engine.models.other import CrawlerConfiguration

logger = logging.getLogger('eo_engine.frontend_ops')
url_template = '{base_url}?{querystring}'


def create_query_dict(**kwargs) -> QueryDict:
    query_dict = QueryDict('', mutable=True)
    for k, v in kwargs.items():
        query_dict.__setitem__(k, v)
    return query_dict


def querystring_factory(**kwargs) -> str:
    return create_query_dict(**kwargs).urlencode()


def homepage(request):
    from eo_engine.common.misc import list_spiders

    submit_task_url = reverse("eo_engine:submit-task")
    from eo_engine.tasks import (task_init_spider,
                                 task_sftp_parse_remote_dir)

    task_init_spider_name = task_init_spider.name

    from eo_engine.models import EOProductGroupChoices
    from eo_engine.models import EOSourceGroupChoices
    context = {
        "sections": {
            "1": {
                "name": 'Tools',
                "section_elements": {
                    "1": {'name': 'Refresh Database',
                          'description': 'Refresh the database and trigger the '
                                         'marking of any derivative products that not there yet',
                          'urls': {"1": {'label': 'Refresh', 'url_str': reverse('eo_engine:refresh-rows')}}},
                    '2': {'name': 'Run Download-Available Task',
                          'description': 'Trigger a task that would download all the known remote available file asynchronously',
                          'urls': {"1": {'label': 'Refresh', 'url_str': 'url_str'}}},
                    '3': {
                        'name': 'Credential Manager',
                        'descrition': 'Add View or remove credentials that are used by the system when fetchin remote sources.',
                        'urls': {"1": {'label': 'Refresh', 'url_str': 'url_str'}}},
                    '4': {'name': 'Retrieve/Crawl AETI WaPOR Products',
                          'urls': {"1": {'label': 'Refresh', 'url_str': 'url_str'}}}
                },
                # "files": {'name': 'files'},
                # "crawlers": {'name': 'Spiders'},
                # "others": {'name': 'Latest Tasks'}
            },
            "2": {
                "name": "S02P02",
                "section_elements": {
                    '1': {'name': EOProductGroupChoices.AGRO_NDVI_300M_V3_AFR.label,
                          'description': f'AGRO/Auth NDVI 300 metres version 3 made from Copernicus it is used for input for the "{EOProductGroupChoices.AGRO_NDVI_1KM_V3_AFR.label}" product',
                          'urls': {
                              "1": {'label': 'Crawler', 'url_str': reverse('eo_engine:configure-crawler', kwargs={
                                  'group_name': EOSourceGroupChoices.C_GLS_NDVI_300M_V2_GLOB})},
                              "2": {'label': 'EO-Sources', 'url_str': reverse('eo_engine:list-eosources', kwargs={
                                  'product_group': EOSourceGroupChoices.C_GLS_NDVI_300M_V2_GLOB})},
                              "3": {'label': 'EO-Products', 'url_str': reverse('eo_engine:list-eoproducts', kwargs={
                                  'product_group': EOProductGroupChoices.AGRO_NDVI_300M_V3_AFR})}
                          }
                          }
                }
            }
        }
    }
    # context = {
    #     'eo_products_grps_value_label': EOProductGroupChoices.choices,
    #     'eo_sources_grps_value_label': EOSourceGroupChoices.choices,
    # }
    #
    # scrappers = {}
    # spider_list = list_spiders()
    # for spider in spider_list:
    #     query_dictionary = QueryDict('', mutable=True)
    #     query_dictionary.update(
    #         task_name=task_init_spider_name,
    #         spider_name=spider
    #     )
    #     url = url_template.format(
    #         base_url=submit_task_url,
    #         querystring=query_dictionary.urlencode()
    #     )
    #     scrappers[spider] = {
    #         'url': url,
    #         'is_configured': EOSourceMeta.objects.filter(group=spider).exists()
    #     }
    #
    # # extra scrappers
    # # scrappers[LABEL] = {url: url, is_configured: T/F}
    # q = QueryDict('', mutable=True)
    # q.update(
    #     task_name=task_sftp_parse_remote_dir.name,
    #     remote_dir='sftp://safmil.ipma.pt/home/safpt/OperationalChain/LSASAF_Products/DMET'
    # )
    # scrappers['LSAF'] = {
    #     'url': url_template.format(
    #         base_url=reverse("eo_engine:submit-task"),
    #         querystring=q.urlencode()),
    #     'is_configured': EOSourceMeta.objects.filter(group='LSAF').exists()
    # }
    #
    # context.update(scrappers=scrappers)
    return render(request, "homepage2.html", context=context)


def list_eosources(request, product_group=None):
    submit_task_url = reverse("eo_engine:submit-task")
    this_page = reverse("eo_engine:list-eosources", kwargs={'product_group': product_group})
    from .models import EOSource
    from .models import GeopTask
    from .tasks import task_download_file
    task_name = task_download_file.name
    # default order [product, date]
    qs = EOSource.objects.all().prefetch_related('task')
    if product_group:
        qs = qs.filter(group=product_group).order_by('filename')

    def build_eo_structure(qs):
        if not qs.exists():
            return []

        eo_source: EOSource
        for eo_source in qs:
            querystring = querystring_factory(task_name=task_name, eo_source_pk=eo_source.pk, next_page=this_page)
            url_string = f'{url_template.format(base_url=submit_task_url, querystring=querystring)}'
            yield eo_source, url_string

    context = {'eo_sources': build_eo_structure(qs),
               'valid_status_to_cancel':
                   [GeopTask.TaskTypeChoices.STARTED.value,
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
               'previous_page': {'url': reverse('eo_engine:list-eoproducts',
                                                kwargs={'product_group': 4}),
                                 'label': 'Products List'},
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
    # ../submit?task_name=task_name&next_page=/something&task_kw
    task_name: Optional[str] = None
    task_kwargs: Dict[str, Union[str, int, float]] = {}
    next_page: Optional[str] = None

    if request.method == 'GET':
        query_dictionary = QueryDict('', mutable=True)
        query_dictionary.update(**request.GET)
        task_name = query_dictionary.pop('task_name')[0]  # required
        next_page = list(collapse(query_dictionary.pop('next_page', None)))[0] or reverse(
            'eo_engine:main-page')  # default to main-page

        for k, v in query_dictionary.items():
            param = list(collapse(v))
            task_kwargs[k] = param[0] if len(param) == 1 else tuple(param)

    if request.method == 'POST':
        from .forms import RunTaskForm
        # TODO: finish submit task through form
        task_data = RunTaskForm(request.POST)
        # task_name = request.

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


def create_wapor_entry(request, product: str):
    from eo_engine.common.time import month_dekad_to_running_decad
    from .forms import WaporNdviForm
    product = product.lower()
    context = {
        'product': product
    }
    if product == 'ndvi':
        formKlass = WaporNdviForm

    else:
        raise
    if request.method == 'GET':
        # dekads = get('https://io.apps.fao.org/gismgr/api/v1/catalog/workspaces/WAPOR_2/dimensions/DEKAD/members',
        #              headers={'Content-Type': 'application/json'})
        context.update(form=formKlass())
        return render(request, 'utilities/create-wapor.html', context=context)
    if request.method == 'POST':
        def form_to_wapor_name(form) -> str:
            data = form.data
            prod_name = form.prod_name
            if isinstance(form, WaporNdviForm):
                dimension = form.dimension
                level = data['level'].upper()
                year = int(data['year'])
                year_str = str(year)
                month = int(data["month"])
                dekad = int(data["dekad"])
                yearly_dekad = month_dekad_to_running_decad(month=month, dekad=dekad)
                # 2101 -- year/year-dekad
                time_element = f'{data["year"][2:]}{yearly_dekad}'
                area = None if data['area'].lower() == 'africa' else data['area'].upper()
            else:
                raise
            if area:
                return f'{level}_{prod_name}_{dimension}_{time_element}_{area}.tif'
            return f'{level}_{prod_name}_{dimension}_{time_element}.tif'

        form = formKlass(request.POST)
        if form.is_valid():
            filename = form_to_wapor_name(form)
        else:
            context.update(form=form)
            return render(request, 'utilities/create-wapor.html', context=context)
        try:
            obj = create_wapor_object(filename)
            messages.success(request, obj)
        except IntegrityError as exp:
            messages.error(request, f'Could not create item: {exp}')
        return redirect(reverse('eo_engine:create-wapor', kwargs={'product': product}))


def configure_crawler(request, group_name: str):
    from .forms import EOSourceMetaForm
    context = {}
    instance, created = CrawlerConfiguration.objects.get_or_create(group=group_name,
                                                                   defaults={'from_date': date(2017, 1, 1)})
    if request.method == 'GET':
        form = EOSourceMetaForm(instance=instance)
        context.update(form=form)
        return render(request, 'configure/crawler.html', context=context)
    if request.method == 'POST':
        if 'cancel' in request.POST:
            return redirect(reverse("eo_engine:main-page"))

        f = EOSourceMetaForm(request.POST, instance=instance)
        f.save()
        if 'save_and_run' in request.POST:
            from eo_engine.tasks import task_init_spider
            task = task_init_spider.s(spider_name=group_name)
            job = task.apply_async()
            messages.add_message(
                request=request, level=messages.SUCCESS,
                message=f'Task {task.name} with task id: {job} successfully submitted')

        return redirect(reverse("eo_engine:main-page"))
