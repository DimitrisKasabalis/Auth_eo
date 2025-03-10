# Generated by Django 3.2.9 on 2022-02-26 12:42

import django.contrib.postgres.fields
import django.contrib.postgres.indexes
import django.core.validators
from django.db import migrations, models
import django.db.models.deletion
import eo_engine.models.eo_product
import eo_engine.models.eo_source


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CrawlerConfiguration',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('enabled', models.BooleanField(default=True)),
                ('timestamp', models.DateTimeField(auto_now_add=True, help_text='timestamp of when this row was made')),
                ('last_modified', models.DateTimeField(auto_now=True, help_text='timestamp of when this row last modified')),
                ('group', models.TextField(choices=[('S02P02_NDVI_300M_V2_GLOB_CGLS', 'Copernicus Global Land Service NDVI 300m v2'), ('S02P02_NDVI_300M_V3_AFR', 'Generated NDVI  300M v3'), ('S02P02_NDVI_1KM_V3_AFR', 'Generated  NDVI 1KM V3 Africa'), ('S02P02_LAI_300M_V1_GLOB_CGLS', 'Copernicus Global Land Service LAI 300m v1'), ('S02P02_LAI_300M_V1_AFR', 'Generated LAI 300M V1'), ('S02P02_NDVIA_250M_TUN_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (TUN)'), ('S02P02_NDVIA_250M_RWA_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (RWA)'), ('S02P02_NDVIA_250M_ETH_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (ETH)'), ('S02P02_NDVIA_250M_ZAF_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (ZAF)'), ('S02P02_NDVIA_250M_NER_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (NER)'), ('S02P02_NDVIA_250M_GHA_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (GHA)'), ('S02P02_NDVIA_250M_MOZ_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (MOZ)'), ('S02P02_NDVIA_250M_KEN_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (KEN)'), ('S04P01_LULC_500M_MCD12Q1_v6', '(MODIS S04P01) Land Cover 500M v6'), ('S04P03_FLD_375M_1D_VIIRS', 'FLD 375M 1D VIIRS'), ('S04P03_WB_10m_BAG', 'WB 10m BAG'), ('S06P01_WB_100M_V1_GLOB_CGLS', 'Copernicus Global Land Service Water Bodies Collection 100m Version 2'), ('S06P01_WB_300M_V2_GLOB_CGLS', 'Copernicus Global Land Service Water Bodies Collection 300m Version 2'), ('S06P01_S1_10M_KZN', 'Sentinel 10m KZN'), ('S06P01_S1_10M_BAG', 'Sentinel 10m BAG'), ('S06P04_ETAnom_5KM_M_GLOB_SSEBOP', 'S06P04: ETAnom 5KM M GLOB SSEBOP'), ('S06P04_ET_3KM_GLOB_MSG', 'S06P04: LSAF 3KM DMET'), ('S06P04_WAPOR_L1_AETI_D_AFRICA', 'WAPOR: L1 AETI D AFRICA'), ('S06P04_WAPOR_L1_QUAL_LST_D_AFRICA', 'WAPOR: L1_QUAL_LST_D_AFRICA'), ('S06P04_WAPOR_L1_QUAL_NDVI_D_AFRICA', 'WAPOR: L1_QUAL_NDVI_D_AFRICA'), ('S06P04_WAPOR_L2_AETI_D_ETH', 'WAPOR: WAPOR_L2_AETI_D_ETH'), ('S06P04_WAPOR_L2_AETI_D_GHA', 'WAPOR: L2_AETI_GHA'), ('S06P04_WAPOR_L2_AETI_D_KEN', 'WAPOR: L2_AETI_D_KEN'), ('S06P04_WAPOR_L2_AETI_D_MOZ', 'WAPOR: L2_AETI_D_MOZ'), ('S06P04_WAPOR_L2_AETI_D_RWA', 'WAPOR: L2_AETI_D_DRWA'), ('S06P04_WAPOR_L2_AETI_D_TUN', 'WAPOR: L2_AETI_D_TUN'), ('S06P04_WAPOR_L2_QUAL_LST_D_ETH', 'WAPOR: L2_QUAL_LST_D_ETH'), ('S06P04_WAPOR_L2_QUAL_LST_D_GHA', 'WAPOR: L2_QUAL_LST_D_GHA'), ('S06P04_WAPOR_L2_QUAL_LST_D_KEN', 'WAPOR: L2_QUAL_LST_D_KEN'), ('S06P04_WAPOR_L2_QUAL_LST_D_MOZ', 'WAPOR: L2_QUAL_LST_D_MOZ'), ('S06P04_WAPOR_L2_QUAL_LST_D_RWA', 'WAPOR: L2_QUAL_LST_D_RWA'), ('S06P04_WAPOR_L2_QUAL_LST_D_TUN', 'WAPOR: L2_QUAL_LST_D_TUN'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_ETH', 'WAPOR: L2_QUAL_NDVI_D_ETH'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_GHA', 'WAPOR: L2_QUAL_NDVI_D_GHA'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_KEN', 'WAPOR: L2_QUAL_NDVI_D_KEN'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_MOZ', 'WAPOR: L2_QUAL_NDVI_D_MOZ'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_RWA', 'WAPOR: L2_QUAL_NDVI_D_RWA'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_TUN', 'WAPOR: L2_QUAL_NDVI_D_TUN')], unique=True)),
                ('from_date', models.DateField()),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Credentials',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('domain', models.CharField(max_length=255, unique=True)),
                ('username', models.CharField(max_length=255, null=True)),
                ('password', models.CharField(max_length=255, null=True)),
                ('api_key', models.CharField(max_length=2048, null=True)),
                ('type', models.CharField(choices=[('USER/PASS', 'USERNAME-PASSWORD'), ('API-KEY', 'API-KEY')], db_column='type', default='USER/PASS', max_length=255)),
            ],
        ),
        migrations.CreateModel(
            name='EOGroup',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('description', models.TextField(default='No-Description')),
                ('indicator', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='EOProduct',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('filename', models.TextField(unique=True)),
                ('datetime_creation', models.DateTimeField(null=True)),
                ('reference_date', models.DateField()),
                ('file', models.FileField(max_length=2048, null=True, upload_to=eo_engine.models.eo_product._upload_to)),
                ('timestamp', models.DateTimeField(auto_now_add=True)),
                ('state', models.CharField(choices=[('AVAILABLE', 'AVAILABLE for generation.'), ('SCHEDULED', 'SCHEDULED For generation.'), ('FAILED', 'Generation was attempted but FAILED'), ('GENERATING', 'GENERATING...'), ('IGNORE', 'Skip generation (Ignored) .'), ('READY', 'Product is READY.'), ('MISSING_SOURCE', 'Some or all EOSource(s) Are Not Available')], default='AVAILABLE', max_length=255)),
                ('group', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='eo_engine.eogroup')),
            ],
            options={
                'ordering': ['group', 'filename'],
            },
        ),
        migrations.CreateModel(
            name='EOSource',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('datetime_seen', models.DateTimeField(auto_created=True, help_text='datetime of when it was seen')),
                ('state', models.CharField(choices=[('AVAILABLE_REMOTELY', 'Available on the Remote Server'), ('SCHEDULED_FOR_DOWNLOAD', 'Scheduled For Download'), ('AVAILABLE_LOCALLY', 'File is Available Locally'), ('DOWNLOADING', 'Downloading File...'), ('DOWNLOAD_FAILED', 'Download Failed'), ('IGNORE', 'Action on this file has been canceled (Ignored/Revoked Action)'), ('DEFERRED', 'Download has been deferred for later')], default='AVAILABLE_REMOTELY', max_length=255)),
                ('file', models.FileField(editable=False, max_length=2048, null=True, upload_to=eo_engine.models.eo_source._file_storage_path)),
                ('filename', models.CharField(max_length=255, unique=True)),
                ('domain', models.CharField(max_length=200)),
                ('filesize_reported', models.BigIntegerField(validators=[django.core.validators.MinValueValidator(0)])),
                ('reference_date', models.DateField(help_text='product reference date')),
                ('url', models.URLField(help_text='Resource URL')),
                ('credentials', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='eo_engine.credentials')),
            ],
            options={
                'ordering': ['filename', '-reference_date', 'group__name'],
            },
        ),
        migrations.CreateModel(
            name='GeopGroupTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('group_task_id', models.UUIDField(db_index=True, editable=False, unique=True)),
                ('root_id', models.UUIDField(db_index=True, editable=False, null=True)),
                ('timestamp', models.DateTimeField(auto_now_add=True)),
                ('datetime_started', models.DateTimeField(null=True)),
                ('datetime_finished', models.DateTimeField(null=True)),
                ('op_name', models.TextField(blank=True, null=True)),
                ('op_args', django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=255), blank=True, null=True, size=None)),
                ('op_kwargs', models.JSONField(default=dict)),
                ('op_initiator', models.TextField(default='', max_length=255)),
            ],
            options={
                'ordering': ['timestamp'],
                'unique_together': {('group_task_id', 'root_id')},
            },
        ),
        migrations.CreateModel(
            name='EOProductGroup',
            fields=[
                ('eogroup_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='eo_engine.eogroup')),
                ('name', models.TextField(choices=[('S02P02_NDVI_300M_V2_GLOB_CGLS', 'Copernicus Global Land Service NDVI 300m v2'), ('S02P02_NDVI_300M_V3_AFR', 'Generated NDVI  300M v3'), ('S02P02_NDVI_1KM_V3_AFR', 'Generated  NDVI 1KM V3 Africa'), ('S02P02_LAI_300M_V1_GLOB_CGLS', 'Copernicus Global Land Service LAI 300m v1'), ('S02P02_LAI_300M_V1_AFR', 'Generated LAI 300M V1'), ('S02P02_NDVIA_250M_TUN_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (TUN)'), ('S02P02_NDVIA_250M_RWA_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (RWA)'), ('S02P02_NDVIA_250M_ETH_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (ETH)'), ('S02P02_NDVIA_250M_ZAF_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (ZAF)'), ('S02P02_NDVIA_250M_NER_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (NER)'), ('S02P02_NDVIA_250M_GHA_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (GHA)'), ('S02P02_NDVIA_250M_MOZ_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (MOZ)'), ('S02P02_NDVIA_250M_KEN_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (KEN)'), ('S04P01_LULC_500M_MCD12Q1_v6', '(MODIS S04P01) Land Cover 500M v6'), ('S04P03_FLD_375M_1D_VIIRS', 'FLD 375M 1D VIIRS'), ('S04P03_WB_10m_BAG', 'WB 10m BAG'), ('S06P01_WB_100M_V1_GLOB_CGLS', 'Copernicus Global Land Service Water Bodies Collection 100m Version 2'), ('S06P01_WB_300M_V2_GLOB_CGLS', 'Copernicus Global Land Service Water Bodies Collection 300m Version 2'), ('S06P01_S1_10M_KZN', 'Sentinel 10m KZN'), ('S06P01_S1_10M_BAG', 'Sentinel 10m BAG'), ('S06P04_ETAnom_5KM_M_GLOB_SSEBOP', 'S06P04: ETAnom 5KM M GLOB SSEBOP'), ('S06P04_ET_3KM_GLOB_MSG', 'S06P04: LSAF 3KM DMET'), ('S06P04_WAPOR_L1_AETI_D_AFRICA', 'WAPOR: L1 AETI D AFRICA'), ('S06P04_WAPOR_L1_QUAL_LST_D_AFRICA', 'WAPOR: L1_QUAL_LST_D_AFRICA'), ('S06P04_WAPOR_L1_QUAL_NDVI_D_AFRICA', 'WAPOR: L1_QUAL_NDVI_D_AFRICA'), ('S06P04_WAPOR_L2_AETI_D_ETH', 'WAPOR: WAPOR_L2_AETI_D_ETH'), ('S06P04_WAPOR_L2_AETI_D_GHA', 'WAPOR: L2_AETI_GHA'), ('S06P04_WAPOR_L2_AETI_D_KEN', 'WAPOR: L2_AETI_D_KEN'), ('S06P04_WAPOR_L2_AETI_D_MOZ', 'WAPOR: L2_AETI_D_MOZ'), ('S06P04_WAPOR_L2_AETI_D_RWA', 'WAPOR: L2_AETI_D_DRWA'), ('S06P04_WAPOR_L2_AETI_D_TUN', 'WAPOR: L2_AETI_D_TUN'), ('S06P04_WAPOR_L2_QUAL_LST_D_ETH', 'WAPOR: L2_QUAL_LST_D_ETH'), ('S06P04_WAPOR_L2_QUAL_LST_D_GHA', 'WAPOR: L2_QUAL_LST_D_GHA'), ('S06P04_WAPOR_L2_QUAL_LST_D_KEN', 'WAPOR: L2_QUAL_LST_D_KEN'), ('S06P04_WAPOR_L2_QUAL_LST_D_MOZ', 'WAPOR: L2_QUAL_LST_D_MOZ'), ('S06P04_WAPOR_L2_QUAL_LST_D_RWA', 'WAPOR: L2_QUAL_LST_D_RWA'), ('S06P04_WAPOR_L2_QUAL_LST_D_TUN', 'WAPOR: L2_QUAL_LST_D_TUN'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_ETH', 'WAPOR: L2_QUAL_NDVI_D_ETH'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_GHA', 'WAPOR: L2_QUAL_NDVI_D_GHA'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_KEN', 'WAPOR: L2_QUAL_NDVI_D_KEN'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_MOZ', 'WAPOR: L2_QUAL_NDVI_D_MOZ'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_RWA', 'WAPOR: L2_QUAL_NDVI_D_RWA'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_TUN', 'WAPOR: L2_QUAL_NDVI_D_TUN')], unique=True)),
            ],
            bases=('eo_engine.eogroup',),
        ),
        migrations.CreateModel(
            name='EOSourceGroup',
            fields=[
                ('eogroup_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='eo_engine.eogroup')),
                ('name', models.TextField(choices=[('S02P02_NDVI_300M_V2_GLOB_CGLS', 'Copernicus Global Land Service NDVI 300m v2'), ('S02P02_NDVI_300M_V3_AFR', 'Generated NDVI  300M v3'), ('S02P02_NDVI_1KM_V3_AFR', 'Generated  NDVI 1KM V3 Africa'), ('S02P02_LAI_300M_V1_GLOB_CGLS', 'Copernicus Global Land Service LAI 300m v1'), ('S02P02_LAI_300M_V1_AFR', 'Generated LAI 300M V1'), ('S02P02_NDVIA_250M_TUN_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (TUN)'), ('S02P02_NDVIA_250M_RWA_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (RWA)'), ('S02P02_NDVIA_250M_ETH_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (ETH)'), ('S02P02_NDVIA_250M_ZAF_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (ZAF)'), ('S02P02_NDVIA_250M_NER_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (NER)'), ('S02P02_NDVIA_250M_GHA_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (GHA)'), ('S02P02_NDVIA_250M_MOZ_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (MOZ)'), ('S02P02_NDVIA_250M_KEN_GMOD', 'MODIS GMOD09Q1 NDVI Anomaly (KEN)'), ('S04P01_LULC_500M_MCD12Q1_v6', '(MODIS S04P01) Land Cover 500M v6'), ('S04P03_FLD_375M_1D_VIIRS', 'FLD 375M 1D VIIRS'), ('S04P03_WB_10m_BAG', 'WB 10m BAG'), ('S06P01_WB_100M_V1_GLOB_CGLS', 'Copernicus Global Land Service Water Bodies Collection 100m Version 2'), ('S06P01_WB_300M_V2_GLOB_CGLS', 'Copernicus Global Land Service Water Bodies Collection 300m Version 2'), ('S06P01_S1_10M_KZN', 'Sentinel 10m KZN'), ('S06P01_S1_10M_BAG', 'Sentinel 10m BAG'), ('S06P04_ETAnom_5KM_M_GLOB_SSEBOP', 'S06P04: ETAnom 5KM M GLOB SSEBOP'), ('S06P04_ET_3KM_GLOB_MSG', 'S06P04: LSAF 3KM DMET'), ('S06P04_WAPOR_L1_AETI_D_AFRICA', 'WAPOR: L1 AETI D AFRICA'), ('S06P04_WAPOR_L1_QUAL_LST_D_AFRICA', 'WAPOR: L1_QUAL_LST_D_AFRICA'), ('S06P04_WAPOR_L1_QUAL_NDVI_D_AFRICA', 'WAPOR: L1_QUAL_NDVI_D_AFRICA'), ('S06P04_WAPOR_L2_AETI_D_ETH', 'WAPOR: WAPOR_L2_AETI_D_ETH'), ('S06P04_WAPOR_L2_AETI_D_GHA', 'WAPOR: L2_AETI_GHA'), ('S06P04_WAPOR_L2_AETI_D_KEN', 'WAPOR: L2_AETI_D_KEN'), ('S06P04_WAPOR_L2_AETI_D_MOZ', 'WAPOR: L2_AETI_D_MOZ'), ('S06P04_WAPOR_L2_AETI_D_RWA', 'WAPOR: L2_AETI_D_DRWA'), ('S06P04_WAPOR_L2_AETI_D_TUN', 'WAPOR: L2_AETI_D_TUN'), ('S06P04_WAPOR_L2_QUAL_LST_D_ETH', 'WAPOR: L2_QUAL_LST_D_ETH'), ('S06P04_WAPOR_L2_QUAL_LST_D_GHA', 'WAPOR: L2_QUAL_LST_D_GHA'), ('S06P04_WAPOR_L2_QUAL_LST_D_KEN', 'WAPOR: L2_QUAL_LST_D_KEN'), ('S06P04_WAPOR_L2_QUAL_LST_D_MOZ', 'WAPOR: L2_QUAL_LST_D_MOZ'), ('S06P04_WAPOR_L2_QUAL_LST_D_RWA', 'WAPOR: L2_QUAL_LST_D_RWA'), ('S06P04_WAPOR_L2_QUAL_LST_D_TUN', 'WAPOR: L2_QUAL_LST_D_TUN'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_ETH', 'WAPOR: L2_QUAL_NDVI_D_ETH'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_GHA', 'WAPOR: L2_QUAL_NDVI_D_GHA'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_KEN', 'WAPOR: L2_QUAL_NDVI_D_KEN'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_MOZ', 'WAPOR: L2_QUAL_NDVI_D_MOZ'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_RWA', 'WAPOR: L2_QUAL_NDVI_D_RWA'), ('S06P04_WAPOR_L2_QUAL_NDVI_D_TUN', 'WAPOR: L2_QUAL_NDVI_D_TUN')], unique=True)),
                ('date_regex', models.TextField(help_text='RegEx that extracts the date element (as the yymmdd or yyyymmdd named group). If not provided the ref date field must have a way to be populated')),
                ('crawler_type', models.TextField(choices=[('NONE', 'Not using crawling'), ('OTHER (PYMODIS)', 'PYMODIS API'), ('OTHER (SENTINEL)', 'Sentinel'), ('OTHER (SFTP)', 'SFTP Crawler'), ('OTHER (WAPOR)', 'Wapor on demand'), ('SCRAPY_SPIDER', 'Scrappy Spider')], default='NONE')),
            ],
            bases=('eo_engine.eogroup',),
        ),
        migrations.CreateModel(
            name='Upload',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('enabled', models.BooleanField(default=True)),
                ('timestamp', models.DateTimeField(auto_now_add=True, help_text='timestamp of when this row was made')),
                ('last_modified', models.DateTimeField(auto_now=True, help_text='timestamp of when this row last modified')),
                ('upload_endpoint', models.TextField(default='sftp://18.159.85.240:2310/', help_text='URL of DRAXIS ftp endpoint', verbose_name='PRAXIS sftp URL')),
                ('upload_path', models.TextField(help_text='path uploaded', null=True)),
                ('upload_traceback_error', models.TextField(help_text='Traceback if upload failed', null=True)),
                ('upload_duration_seconds', models.IntegerField(help_text='Time needed to upload the file to draxis', null=True)),
                ('notification_endpoint', models.URLField(default='https://africultures-backend.draxis.gr/notify', help_text='Praxis notification app endpoint')),
                ('notification_payload', models.JSONField(help_text='Notification payload', null=True)),
                ('notification_send_timestamp', models.DateTimeField(help_text='Timestamp of notification payload was sent', null=True)),
                ('notification_send_return_code', models.IntegerField(null=True)),
                ('notification_traceback_error', models.TextField(help_text='Traceback if upload failed', null=True)),
                ('eo_product', models.ForeignKey(help_text='EO Product uploaded', on_delete=django.db.models.deletion.CASCADE, to='eo_engine.eoproduct')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Pipeline',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('enabled', models.BooleanField(default=True)),
                ('timestamp', models.DateTimeField(auto_now_add=True, help_text='timestamp of when this row was made')),
                ('last_modified', models.DateTimeField(auto_now=True, help_text='timestamp of when this row last modified')),
                ('name', models.TextField(default='No Name')),
                ('package', models.TextField(choices=[('S02P02', 'Package S02P02'), ('S04P01', 'Package S04P01'), ('S04P03', 'Package S04P03'), ('S06P01', 'Package S06P01'), ('S06P04', 'Package S06P04')])),
                ('description', models.TextField(default='No-Description')),
                ('output_filename_template', models.TextField()),
                ('output_folder', models.TextField()),
                ('task_name', models.TextField()),
                ('task_kwargs', models.JSONField(default=dict)),
                ('input_groups', models.ManyToManyField(related_name='pipelines_from_input', related_query_name='pipelines', to='eo_engine.EOGroup')),
                ('output_group', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, related_name='pipelines_from_output', to='eo_engine.eogroup')),
            ],
            options={
                'ordering': ['package', 'name'],
            },
        ),
        migrations.CreateModel(
            name='GeopTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('task_id', models.UUIDField(db_index=True, editable=False, unique=True)),
                ('task_name', models.TextField()),
                ('task_args', models.TextField(default='[]')),
                ('parent_id', models.UUIDField(db_index=True, editable=False, null=True)),
                ('status', models.TextField(choices=[('SUBMITTED', 'Submitted'), ('STARTED', 'Started'), ('SUCCESS', 'Success'), ('FAILURE', 'Failure'), ('REVOKED', 'Revoked'), ('RETRY', 'Retry'), ('UNKNOWN', 'Unknown')], db_index=True, default='SUBMITTED')),
                ('datetime_submitted', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('datetime_started', models.DateTimeField(blank=True, null=True)),
                ('datetime_finished', models.DateTimeField(blank=True, null=True)),
                ('time_to_complete', models.TextField(blank=True, default='')),
                ('retries', models.IntegerField(default=0)),
                ('task_kwargs', models.JSONField(default=dict)),
                ('root_id', models.UUIDField(db_index=True, editable=False, null=True)),
                ('eo_product', models.ManyToManyField(related_name='task', related_query_name='task', to='eo_engine.EOProduct')),
                ('eo_source', models.ManyToManyField(related_name='task', related_query_name='task', to='eo_engine.EOSource')),
                ('group_task', models.ForeignKey(null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='geop_tasks', related_query_name='geop_task', to='eo_engine.geopgrouptask', to_field='group_task_id')),
            ],
            options={
                'ordering': ['datetime_submitted'],
                'get_latest_by': ['datetime_submitted'],
            },
        ),
        migrations.AddConstraint(
            model_name='pipeline',
            constraint=models.UniqueConstraint(fields=('output_group',), name='unique input/output'),
        ),
        migrations.AddConstraint(
            model_name='pipeline',
            constraint=models.UniqueConstraint(fields=('task_name', 'task_kwargs'), name='unique task/kw_task'),
        ),
        migrations.AddIndex(
            model_name='geoptask',
            index=django.contrib.postgres.indexes.GinIndex(fields=['task_kwargs'], name='eo_engine_g_task_kw_f78125_gin'),
        ),
        migrations.AddField(
            model_name='eosource',
            name='group',
            field=models.ManyToManyField(to='eo_engine.EOSourceGroup'),
        ),
        migrations.AlterUniqueTogether(
            name='eoproduct',
            unique_together={('reference_date', 'group')},
        ),
    ]
