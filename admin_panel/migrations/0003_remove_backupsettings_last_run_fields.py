# Generated manually

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("admin_panel", "0002_remove_backupsettings_schedule_fields"),
    ]

    operations = [
        migrations.RemoveField(model_name="backupsettings", name="last_backup_at"),
        migrations.RemoveField(model_name="backupsettings", name="last_backup_ok"),
        migrations.RemoveField(model_name="backupsettings", name="last_backup_note"),
    ]
