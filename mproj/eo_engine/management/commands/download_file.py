from django.core.management.base import BaseCommand, CommandError
from eo_engine.models import EOSource
from eo_engine.tasks import task_download_file


def _as_eo_source(value: str):
    if value.isdigit():
        return EOSource.objects.get(id=int(value))
    else:
        return EOSource.objects.get(filename=value)


class Command(BaseCommand):
    """ Download a remote data source. The remote resource can be identified by its database ID, or filename"""

    def add_arguments(self, parser):
        parser.add_argument('--as-task', action='store_true',
                            help='Don\'t do this command locally, but schedule a task instead')
        parser.add_argument('eo_source', type=_as_eo_source)

    def handle(self, *args, **options):
        eo_source = options['eo_source']
        as_task = options['as_task']

        eo_source_pk = eo_source.pk
        if as_task:
            task = task_download_file.s(eo_source_pk=eo_source_pk)
            job = task.apply_async()
            self.stdout(f"job submited with task_id: {job}")
            return

        task_download_file(eo_source_pk=eo_source_pk)
