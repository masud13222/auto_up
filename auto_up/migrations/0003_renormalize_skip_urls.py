# Data migration: refresh normalized_url after canonical_skip_url logic changes.

from django.db import migrations


def forwards(apps, schema_editor):
    from auto_up.url_match import canonical_skip_url

    AutoUpSkipUrl = apps.get_model("auto_up", "AutoUpSkipUrl")
    alias = schema_editor.connection.alias

    for row in AutoUpSkipUrl.objects.using(alias).iterator():
        new_n = canonical_skip_url(row.url or "")
        if not new_n or new_n == row.normalized_url:
            continue
        collides = (
            AutoUpSkipUrl.objects.using(alias)
            .exclude(pk=row.pk)
            .filter(normalized_url=new_n)
            .exists()
        )
        if collides:
            continue
        AutoUpSkipUrl.objects.using(alias).filter(pk=row.pk).update(normalized_url=new_n)


def backwards(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("auto_up", "0002_skip_url_list"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
