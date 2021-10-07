# Generated by Django 3.2.7 on 2021-10-07 08:00

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('eo_engine', '0036_auto_20211006_1315'),
    ]

    operations = [
        migrations.AlterField(
            model_name='eosource',
            name='state',
            field=models.CharField(choices=[('availableRemotely', 'Available on the Remote Server'), ('ScheduledForDownload', 'Scheduled For Download'), ('availableLocally', 'File is Available Locally'), ('BeingDownloaded', 'File is Being Downloaded'), ('FailedToDownload', 'Failed to Download'), ('Ignore', 'Action on this file has been canceled (Ignored/Revoked Action)')], default='availableRemotely', max_length=255),
        ),
    ]
