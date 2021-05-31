from django.core.management.base import BaseCommand, CommandError
from eo_engine.tasks import task_start_scrape


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('spider_name', nargs=1)
        parser.add_argument('--as-task', action='store_true',
                            help='Don\'t do this command locally, but schedule a task instead')
        parser.add_argument('--kwargs', nargs='*')

    def handle(self, *args, **options):
        spider_name = options['spider_name'][0]
        as_task = options['as_task']
        kwargs = options['kwargs']
        print(options)
        if as_task:
            task = task_start_scrape.s(spider_name=spider_name)
            task.apply_async()
        else:
            task_start_scrape(spider_name=spider_name)
