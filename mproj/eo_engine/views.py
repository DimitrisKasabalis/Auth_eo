import logging
import re
from celery.result import AsyncResult
from celery.utils.serialization import strtobool
from django.contrib import messages
from django.http import QueryDict
from django.shortcuts import render, redirect
from django.urls import reverse
from django.utils.datetime_safe import date, datetime
from django.utils.http import urlencode
from more_itertools import collapse
from typing import Literal, Callable, Optional, Dict, Union

from eo_engine.common.tasks import get_task_ref_from_name
from eo_engine.errors import AfriCultuReSError
from eo_engine.models import EOSource, EOProduct, EOProductStateChoices, EOSourceGroup, EOProductGroup
from eo_engine.models.factories import create_or_get_wapor_object_from_filename
from eo_engine.models.other import CrawlerConfiguration, Pipeline

logger = logging.getLogger('eo_engine.frontend_ops')
url_template = '{base_url}?{querystring}'


def create_query_dict(**kwargs) -> QueryDict:
    query_dict = QueryDict('', mutable=True)
    for k, v in kwargs.items():
        query_dict.__setitem__(k, v)
    return query_dict


def querystring_factory(**kwargs) -> str:
    return create_query_dict(**kwargs).urlencode()


def main_page(request):
    from eo_engine.models import Pipeline
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
                        'urls': {
                            "1": {'label': 'Credential Manager', 'url_str': reverse('eo_engine:credentials-list')}}},
                    '4': {'name': 'Retrieve/Crawl AETI WaPOR Products',
                          'urls': {"1": {'label': 'Refresh', 'url_str': 'url_str'}}}
                },
                # "files": {'name': 'files'},
                # "crawlers": {'name': 'Spiders'},
                # "others": {'name': 'Latest Tasks'}
            },
            # second section, work packages
            # each work package should have its own section
            "2": {
                "name": "S02P02",
                # pipelines for this section
                "section_elements": {idx: {
                    'name': v.name,
                    'description': v.description,
                    'urls': v.urls()
                } for idx, v in enumerate(Pipeline.objects.filter(package='S02P02').order_by('name'))}
            },
            "3": {
                "name": "S06P01",
                "section_elements": {idx: {
                    'name': v.name,
                    'description': v.description,
                    'urls': v.urls()
                } for idx, v in enumerate(Pipeline.objects.filter(package='S06P01').order_by('name'))}
            },
            "4": {
                "name": "S04P03",
                "section_elements": {idx: {
                    'name': v.name,
                    'description': v.description,
                    'urls': v.urls()
                } for idx, v in enumerate(Pipeline.objects.filter(package='S04P03').order_by('name'))}
            },
            "5": {
                "name": "S06P04",
                "section_elements": {idx: {
                    'name': v.name,
                    'description': v.description,
                    'urls': v.urls()
                } for idx, v in enumerate(Pipeline.objects.filter(package='S06P04').order_by('name'))}
            }
        }
    }

    return render(request, "homepage2.html", context=context)


def delete_file(request, resource_type: Literal['eo_source', 'eo_product'], pk: int):
    from eo_engine.common.db_ops import delete_eo_product, delete_eo_source
    from eo_engine.errors import AfriCultuReSFileDoesNotExist, AfriCultuReSFileInUse

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
    except AfriCultuReSFileDoesNotExist:
        messages.add_message(request, messages.ERROR,
                             "File does not exist to delete ")
    except AfriCultuReSFileInUse:
        messages.add_message(request, messages.ERROR,
                             "File in use, did not delete")

    return redirect('eo_engine:list-eosources')


def trigger_generate_product(request, filename):
    from eo_engine.common.tasks import get_task_ref_from_name
    from .models import EOProduct, EOProductStateChoices
    eo_product = EOProduct.objects.get(filename=filename)
    out_pipeline = eo_product.group.eoproductgroup.pipelines_from_output.first()
    task_name = out_pipeline.task_name
    task_kwargs = out_pipeline.task_kwargs
    task = get_task_ref_from_name(task_name).s(eo_product_pk=eo_product.pk, **task_kwargs)
    job: AsyncResult = task.apply_async()
    eo_product.state = EOProductStateChoices.SCHEDULED
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
    obj.state = EOSourceStateChoices.SCHEDULED_FOR_DOWNLOAD
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
        next_page = list(collapse(query_dictionary.pop('next_page', None)))[
                        0] or request.META.get('HTTP_REFERER', None) or reverse(
            "eo_engine:main-page")  # default to main-page

        for k, v in query_dictionary.items():
            param = list(collapse(v))
            task_kwargs[k] = param[0] if len(param) == 1 else tuple(param)

    if request.method == 'POST':
        from .forms import RunTaskForm
        # TODO: finish submit task through form
        task_data = RunTaskForm(request.POST)
        # task_name = request.

    task = get_task_ref_from_name(task_name).s(**task_kwargs)
    # if the task that we are about to submit is to download something,
    # mark it as scheduled
    from eo_engine.tasks import task_download_file
    if task_download_file == get_task_ref_from_name(task_name):
        from eo_engine.models import EOSourceStateChoices
        eo_source = EOSource.objects.get(pk=task_kwargs['eo_source_pk'])
        eo_source.state = EOSourceStateChoices.SCHEDULED_FOR_DOWNLOAD
        eo_source.save()

    job = task.apply_async()
    messages.add_message(
        request=request, level=messages.SUCCESS,
        message=f'Task {task.name} with task id: {job} successfully submitted')
    return redirect(next_page)


def trigger_crawler(request, group_name: str):
    group = EOSourceGroup.objects.get(name=group_name)

    if group.crawler_type == group.CrawlerTypeChoices.SCRAPY_SPIDER:
        from .tasks import task_init_spider

        task = task_init_spider.s(spider_name=group_name)
        job: AsyncResult = task.apply_async()
        context = {
            'card_info':
                {'task_name': task.name,
                 'param': group_name,
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
        except Exception:
            messages.error(request, 'error')
        finally:
            return redirect(reverse('credentials-list'))


def create_sentinel_entry(request, group_name: str):
    from .forms import SentinelForm
    pattern = re.compile(r'S06P01_S1_10M_(?P<LOCATION>KZN|BAG)', re.IGNORECASE)
    match = pattern.match(group_name)
    if match is None:
        raise

    group_dict = match.groupdict()
    location = group_dict['LOCATION']

    context = {
        'group_name': group_name,
        'location': location
    }
    form_class = SentinelForm

    if request.method == 'GET':
        context.update(form=form_class())
        return render(request, 'utilities/create-sentinel.html', context=context)
    if request.method == 'POST':
        from eo_engine.tasks import task_scan_sentinel_hub
        form = form_class(request.POST)
        if form.is_valid():
            data = form.data
            from_date_str = data['from_date']
            to_date_str = data['to_date']
            from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
            to_date = datetime.strptime(to_date_str, '%Y-%m-%d')

            task_scan_sentinel_hub(from_date=from_date, to_date=to_date, location=location)
            group = EOSourceGroup.objects.get(name=group_name)
            pipeline = group.pipelines_from_input.get()
            return redirect('eo_engine:pipeline-inputs-list', pipeline_pk=pipeline.pk)


def create_wapor_entry(request, group_name: str):
    from eo_engine.common.time import month_dekad_to_running_decad, day2dekad
    from .forms import WaporForm

    pat = re.compile(
        r'S06P04_WAPOR_(?P<LEVEL>(L1|L2))_(?P<PROD>(AETI|QUAL_LST|QUAL_NDVI))_D_(?P<LOCATION>(AFRICA|\w{3}|))',
        re.IGNORECASE)
    match = pat.match(group_name)
    if match is None:
        raise

    group_dict = match.groupdict()
    product_level = group_dict['LEVEL']
    product_name = group_dict['PROD']
    location = group_dict['LOCATION']
    product_dimension = 'D'  # it's always D (DEKAD) at this stage

    context = {
        'group_name': group_name,
        'product_name': product_name,
        'product_level': product_level,
        'product_location': location,
        'product_dimension': 'D'
    }

    form_class = WaporForm

    if request.method == 'GET':
        context.update(form=form_class())
        return render(request, 'utilities/create-wapor.html', context=context)
    if request.method == 'POST':
        form = form_class(request.POST)
        from datetime import datetime
        from dateutil.rrule import rrule, DAILY
        if form.is_valid():
            data = form.data
            from_date_str = data['from_date']
            to_date_str = data['to_date']
            from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
            to_date = datetime.strptime(to_date_str, '%Y-%m-%d')

            obj_created = 0
            obj_exist = 0
            dt: datetime
            for idx, dt in enumerate(rrule(DAILY, dtstart=from_date, until=to_date), 1):
                year = dt.year
                month = dt.month
                day = dt.day
                yearly_dekad = month_dekad_to_running_decad(month, day2dekad(day))
                YYKK = f'{str(year)[2:]}{yearly_dekad}'  # last two digits of the year + running dekad
                if location:
                    filename = f'{product_level}_{product_name}_{product_dimension}_{YYKK}_{location}.tif'
                else:
                    filename = f'{product_level}_{product_name}_{product_dimension}_{YYKK}.tif'
                obj, created = create_or_get_wapor_object_from_filename(filename)
                if created:
                    obj_created += 1
                else:
                    obj_exist += 1
            messages.success(request, f' Added {obj_created} entries in the database.')
        else:
            # form is not valid
            context.update(form=form)
            return render(request, 'utilities/create-wapor.html', context=context)

        return redirect(reverse('eo_engine:create-wapor', kwargs={'group_name': group_name}))


def configure_crawler(request, group_name: str):
    from .forms import EOSourceMetaForm
    group = EOSourceGroup.objects.get(name=group_name)
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
            # fire a scrapy_spider
            if group.crawler_type == group.CrawlerTypeChoices.SCRAPY_SPIDER:
                from eo_engine.tasks import task_init_spider
                task = task_init_spider.s(spider_name=group_name)
            # fire a sftp crawl
            elif group.crawler_type == group.CrawlerTypeChoices.OTHER_SFTP:
                from eo_engine.tasks import task_sftp_parse_remote_dir
                task = task_sftp_parse_remote_dir.s(group_name=group_name)
            else:
                raise AfriCultuReSError('+CONFIGURE_CRAWLER:BUG. Unknown type of crawler')

            job = task.apply_async()
            messages.add_message(
                request=request, level=messages.SUCCESS,
                message=f'Task {task.name} with task id: {job} successfully submitted')

        return redirect(reverse("eo_engine:main-page"))


def pipeline_inputs(request, pipeline_pk: int):
    pipeline = Pipeline.objects.get(pk=pipeline_pk)

    input_eo_source_groups = EOSourceGroup.objects.filter(id__in=pipeline.input_groups.all().values_list('id'))
    # EOSource.objects.filter(group__in=pipeline.input_group.all())
    input_eo_product_groups = EOProductGroup.objects.filter(id__in=pipeline.input_groups.all().values_list('id'))

    input_eo_source_data = [
        {"idx": idx,
         "type": 'source',
         "group": eo_source_group,
         'discover_url': eo_source_group.discover_url(),
         'entries': EOSource.objects.filter(group=eo_source_group).order_by('-reference_date', '-id')}
        for idx, eo_source_group in enumerate(input_eo_source_groups)
    ]
    # todo: properly
    input_eo_product_data = [
        {"idx": idx,
         "type": 'product',
         "group_name": eo_product_group.get_name_display,
         # 'discover_url': eo_product_group.discover_url(),
         'entries': EOProduct.objects.filter(group=eo_product_group).order_by('-reference_date', '-id')}
        for idx, eo_product_group in enumerate(input_eo_product_groups)
    ]
    context = {
        "pipeline_pk": pipeline_pk,
        "data": input_eo_source_data + input_eo_product_data
    }
    return render(request, 'list_eosources.html', context=context)


def pipeline_outputs(request, pipeline_pk: int):
    pipeline = Pipeline.objects.get(pk=pipeline_pk)

    group = pipeline.output_group
    output_group = group.eoproductgroup
    output_eo_product_qs = EOProduct.objects.filter(group=output_group).order_by('-reference_date')

    task_name = pipeline.task_name
    task_kwargs = pipeline.task_kwargs

    context = {
        'task_name': pipeline.task_name,
        "pipeline_pk": pipeline_pk,
        'group': output_group,
        'data': [
            {'pk': eo_product.pk,
             'filename': eo_product.filename,
             'state': eo_product.get_state_display,
             'generate_url': '?'.join((
                 reverse('eo_engine:submit-task'),
                 urlencode({'task_name': task_name,
                            'eo_product_pk': eo_product.pk,
                            **task_kwargs}))) if [EOProductStateChoices.AVAILABLE, EOProductStateChoices.READY,
                                                  EOProductStateChoices.FAILED].count(
                 eo_product.state) == 1 else None,
             'file_url': eo_product.file.url if eo_product.file else None,
             } for eo_product in output_eo_product_qs]}
    return render(request, 'list_eoproducts.html', context=context)
