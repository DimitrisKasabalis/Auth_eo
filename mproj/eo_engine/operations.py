from celery import shared_task, chain, group

from eo_engine.tasks import task_download_file, task_translate_h5_to_geotiff, task_warp_geotiff_to_nc
from eo_engine.models import EOSource, EOProduct
from eo_engine.models import EOSourceStatusChoices


@shared_task
def handle_vci_product(url: str):
    chain_task = chain(
        task_download_file(url=url), task_translate_h5_to_geotiff.s(), task_warp_geotiff_to_nc.s(), tast_upload_file.s(), task_ping_provider.s()
    )


@shared_task
def op_download_available_products():
    to_download_qs = EOSource.objects.filter(status=EOSourceStatusChoices.availableRemotely)
    group_job = group(
        task_download_file.s(filename=filename) for filename in to_download_qs.values_list('filename', flat=True)
    )

    result = group_job.apply_async()
    return result.id
