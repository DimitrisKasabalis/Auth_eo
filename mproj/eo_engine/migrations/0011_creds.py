from django.db import migrations


def forwards_func(apps, schema_editor):
    Credentials = apps.get_model("eo_engine", "Credentials")

    Credentials.objects.bulk_create([
        Credentials(domain="ftp.globalland.cls.fr", username="vesnikos", password="vYxDWrstV265PDF"),
    ])


def reverse_func(apps, schema_editor):
    Credentials = apps.get_model("eo_engine", "Credentials")
    Credentials.objects.filter(domain="ftp.globalland.cls.fr").delete()


class Migration(migrations.Migration):
    dependencies = [
        ('eo_engine', '0010_auto_20210626_1801'),
    ]

    operations = [
        migrations.RunPython(forwards_func, reverse_func)
    ]
