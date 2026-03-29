from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("upload", "0010_alter_mediatask_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="mediatask",
            name="site_sync_snapshot",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text=(
                    "Last known target-site state used for future duplicate updates. "
                    "Stores a normalized snapshot of published movie/series downloads so "
                    "later extra-URL updates can merge against it."
                ),
            ),
        ),
    ]
