from django.core.management.base import BaseCommand, CommandError
from eo_engine.models import EOProduct


def _as_EOProduct(value: str):
    if value.isdigit():
        return EOProduct.objects.get(id=int(value))
    else:
        return EOProduct.objects.get(filename=value)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('--as-task', action='store_true',
                            help='Don\'t do this command locally, but schedule a task instead')
        parser.add_argument('eo_prod', type=_as_EOProduct)

    def handle(self, *args, **options):
        eo_prod = options['eo_prod']
        as_task = options['as_task']

        eo_prod.make_product(as_task=as_task)
