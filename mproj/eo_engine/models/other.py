from django.db import models


class Credentials(models.Model):
    domain = models.TextField()
    username = models.CharField(max_length=255)
    password = models.CharField(max_length=255)


class FunctionalRules(models.Model):
    domain = models.CharField(max_length=100)
    rules = models.JSONField(default=dict)
