# Generated by Django 3.2.8 on 2021-10-31 15:02

from django.db import migrations, IntegrityError
from itertools import product


def forwards_func(apps, schema_editor):
    from eo_engine.models.factories import create_wapor_object
    from eo_engine.common.contrib.waporv2 import well_known_bboxes
    products_ids = ['AETI', 'QUAL_LST', 'QUAL_NDVI']
    dimension_ids = ['D', ]
    yearly_dekads = list(range(1, 37))
    years = list(range(2021, 2022))
    areas = well_known_bboxes.keys()
    for product_id, dimension_id, year, yearly_dekad, area in product(products_ids,
                                                                             dimension_ids, years,
                                                                             yearly_dekads, areas):
        year_id = str(year)[2:]
        yearly_dekad_id = str(yearly_dekad).zfill(2)
        if area == 'africa':
            level = 'L1'
            filename = f'{level.upper()}_{product_id.upper()}_{dimension_id.upper()}_{year_id.upper()}{yearly_dekad_id.upper()}.tif'
        else:
            level = 'L2'
            filename = f'{level.upper()}_{product_id.upper()}_{dimension_id.upper()}_{year_id.upper()}{yearly_dekad_id.upper()}_{area.upper()}.tif'

        try:
            # print(filename)
            create_wapor_object(filename=filename)
            # print(f'--created entry: {filename}---')
        except IntegrityError:
            pass


def reverse_func(apps, schema_editor):
    EOSource = apps.get_model("eo_engine", "EOSource")
    qs = EOSource.objects.filter(domain='wapor')

    qs.delete()


class Migration(migrations.Migration):
    dependencies = [
        ('eo_engine', '0040_auto_20211031_1424'),
    ]

    operations = [
        migrations.RunPython(forwards_func, reverse_func)
    ]
