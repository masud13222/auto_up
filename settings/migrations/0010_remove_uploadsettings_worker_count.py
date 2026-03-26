# Generated manually — queue workers are configured via Q_CLUSTER_WORKERS env only.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("settings", "0009_backupsettings"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="uploadsettings",
            name="worker_count",
        ),
    ]
