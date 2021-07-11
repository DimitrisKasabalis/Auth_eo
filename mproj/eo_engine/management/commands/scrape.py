from django.core.management.base import BaseCommand, CommandError
from eo_engine.tasks import task_init_spider


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('spider_name', nargs=1)
        parser.add_argument('--as-task', action='store_true',
                            help='Don\'t do this command locally, but schedule a task instead')

    def handle(self, *args, **options):
        spider_name = options['spider_name'][0]
        as_task = options['as_task']
        # print(options)
        if as_task:
            task = task_init_spider.s(spider_name=spider_name)
            task.apply_async()
        else:
            task_init_spider(spider_name=spider_name)
