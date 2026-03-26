from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("admin_panel", "0003_remove_backupsettings_last_run_fields"),
        ("settings", "0009_backupsettings"),
    ]

    operations = [
        migrations.DeleteModel(name="BackupSettings"),
    ]
