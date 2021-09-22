# Generated by Django 3.2.5 on 2021-09-22 11:00

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('eo_engine', '0031_auto_20210831_0959'),
    ]

    operations = [
        migrations.AlterField(
            model_name='eoproduct',
            name='eo_products_inputs',
            field=models.ManyToManyField(related_name='depended_eo_product', related_query_name='depended_eo_products', to='eo_engine.EOProduct'),
        ),
        migrations.AlterField(
            model_name='eoproduct',
            name='eo_sources_inputs',
            field=models.ManyToManyField(related_name='eo_products', related_query_name='eo_product', to='eo_engine.EOSource'),
        ),
    ]
