# Generated by Django 3.2 on 2021-05-17 17:15

from django.db import migrations, models
import eo_engine.models.eo_source


class Migration(migrations.Migration):

    dependencies = [
        ('eo_engine', '0003_auto_20210514_1755'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='eosource',
            name='extension',
        ),
        migrations.RemoveField(
            model_name='eosource',
            name='size',
        ),
        migrations.AlterField(
            model_name='eoproduct',
            name='location',
            field=models.FileField(upload_to='agro_products'),
        ),
        migrations.AlterField(
            model_name='eosource',
            name='file',
            field=models.FileField(editable=False, null=True, upload_to=eo_engine.models.eo_source._file_storage_path),
        ),
    ]
