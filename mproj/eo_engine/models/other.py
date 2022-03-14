from django.db import models
from typing import Optional, Dict

from eo_engine.errors import AfriCultuReSError
from eo_engine.models import EOSourceGroupChoices


class Credentials(models.Model):
    class CredentialsTypeChoices(models.TextChoices):
        USERNAME_PASSWORD = 'USER/PASS', "USERNAME-PASSWORD"
        API_KEY = 'API-KEY', "API-KEY"

    domain = models.CharField(max_length=255, unique=True)
    username = models.CharField(max_length=255, null=True)
    password = models.CharField(max_length=255, null=True)
    api_key = models.CharField(max_length=2048, null=True)
    type = models.CharField(db_column='type', max_length=255,
                            choices=CredentialsTypeChoices.choices,
                            default=CredentialsTypeChoices.USERNAME_PASSWORD)


# general template of rules
class RuleMixin(models.Model):
    enabled = models.BooleanField(default=True)
    timestamp = models.DateTimeField(auto_now_add=True, editable=False, help_text='timestamp of when this row was made')
    last_modified = models.DateTimeField(auto_now=True, editable=False,
                                         help_text='timestamp of when this row last modified')

    class Meta:
        abstract = True


class CrawlerConfiguration(RuleMixin):
    group = models.TextField(choices=EOSourceGroupChoices.choices, unique=True)
    from_date = models.DateField()  # date where scan should begin from. assume 00:00


class Pipeline(RuleMixin):
    class PackageChoices(models.TextChoices):
        S02P02 = 'S02P02', 'Package S02P02'
        S04P01 = 'S04P01', 'Package S04P01'
        S04P03 = 'S04P03', 'Package S04P03'
        S06P01 = 'S06P01', 'Package S06P01'
        S06P04 = 'S06P04', 'Package S06P04'

    name = models.TextField(default='No Name')
    package = models.TextField(choices=PackageChoices.choices)
    description = models.TextField(default='No-Description')
    input_groups = models.ManyToManyField('EOGroup', related_name='pipelines_from_input',
                                          related_query_name='pipelines')
    output_group = models.ForeignKey('EOGroup', on_delete=models.DO_NOTHING, related_name='pipelines_from_output')
    output_filename_template = models.TextField()
    output_folder = models.TextField()
    task_name = models.TextField()
    task_kwargs = models.JSONField(default=dict)

    @property
    def service(self) -> str:
        return self.package[:3].replace('0', '')

    @property
    def product(self) -> str:
        return self.package[3:].replace('0', '')

    def output_filename(self, **kwargs) -> str:
        try:
            return self.output_filename_template.format(**kwargs)
        except KeyError as e:
            raise AfriCultuReSError(f'Could not interpolate template sting {self.output_filename_template}') from e

    def urls(self) -> dict:
        from django.shortcuts import reverse
        from eo_engine.models import EOSourceGroup

        def crawler() -> Optional[dict]:
            if self.input_groups.exists():
                crawler_type = self.input_groups.eosourcegroup.crawler_type
                if crawler_type == EOSourceGroup.CrawlerTypeChoices.SCRAPY_SPIDER:
                    return {'label': 'Crawler', 'url_str': reverse('eo_engine:crawler-configure', kwargs={
                        'group_name': self.input_groups.eosourcegroup.name})}
                elif crawler_type == EOSourceGroup.CrawlerTypeChoices.OTHER_SFTP:
                    return {'label': 'SFTP Dir Parse', 'url_str': 'none-yet'}
                elif crawler_type == EOSourceGroup.CrawlerTypeChoices.NONE:
                    return None
            return None

        return {
            '1': None,
            '2': {'label': 'Inputs ',
                  'url_str': reverse('eo_engine:pipeline-inputs-list', kwargs={'pipeline_pk': self.pk})},
            '3': {'label': 'Output Group',
                  'url_str': reverse('eo_engine:pipeline-outputs-list', kwargs={'pipeline_pk': self.pk})}}

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['output_group'], name='unique input/output'),
            models.UniqueConstraint(fields=['task_name', 'task_kwargs'], name='unique task/kw_task')
        ]
        ordering = ['package', 'name']


class Upload(RuleMixin):
    eo_product = models.ForeignKey('EOProduct', unique=False, on_delete=models.CASCADE,
                                   help_text='EO Product uploaded')
    upload_endpoint = models.TextField(verbose_name='PRAXIS sftp URL', help_text='URL of DRAXIS ftp endpoint',
                                       default='sftp://18.159.85.240:2310/')
    upload_path = models.TextField(help_text='path uploaded', null=True)

    upload_traceback_error = models.TextField(help_text='Traceback if upload failed', null=True)
    upload_duration_seconds = models.IntegerField(help_text='Time needed to upload the file to draxis', null=True)

    notification_endpoint = models.URLField(help_text='Praxis notification app endpoint',
                                            default='https://africultures-backend.draxis.gr/notify')
    notification_payload = models.JSONField(help_text='Notification payload', null=True)
    notification_send_timestamp = models.DateTimeField(
        help_text='Timestamp of notification payload was sent', null=True, )
    notification_send_return_code = models.IntegerField(null=True)
    notification_traceback_error = models.TextField(help_text='Traceback if upload failed', null=True)

    def payload_generator(self) -> Dict[str, str]:
        return {
            'product_name': self.eo_product.filename
        }

    def payload_generator_json(self) -> str:
        import json
        return json.dumps(self.payload_generator())
