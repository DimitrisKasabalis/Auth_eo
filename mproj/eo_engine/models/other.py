from django.db import models


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
