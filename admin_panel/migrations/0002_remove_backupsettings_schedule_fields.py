# Generated manually

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("admin_panel", "0001_backupsettings"),
    ]

    operations = [
        migrations.RemoveField(model_name="backupsettings", name="frequency"),
        migrations.RemoveField(model_name="backupsettings", name="daily_run_at"),
        migrations.RemoveField(model_name="backupsettings", name="interval_hours"),
    ]
