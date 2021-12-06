from django.db import models

from eo_engine.common.misc import rec_dd
from eo_engine.models.eo_source import EOSourceGroupChoices


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


class FunctionalRules(models.Model):
    domain = models.CharField(max_length=100)
    rules = models.JSONField(default=dict)


class RuleMixin(models.Model):
    enabled = models.BooleanField(default=True)
    timestamp = models.DateTimeField(auto_now_add=True, editable=False)
    last_modified = models.DateTimeField(auto_now=True, editable=False)
    from_date = models.DateField()  # date where scan should begin from. assume 00:00
    notes = models.TextField(default='')

    class Meta:
        abstract = True


class EOSourceMeta(RuleMixin):
    group = models.TextField(choices=EOSourceGroupChoices.choices, unique=True)
