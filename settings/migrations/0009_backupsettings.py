# Generated manually — BackupSettings moved from admin_panel to settings.

import django.db.models.deletion
from django.db import migrations, models


def copy_backup_from_admin_panel(apps, schema_editor):
    Old = apps.get_model("admin_panel", "BackupSettings")
    New = apps.get_model("settings", "BackupSettings")
    old = Old.objects.filter(pk=1).first()
    if not old:
        return
    New.objects.update_or_create(
        pk=1,
        defaults={
            "google_config_id": old.google_config_id,
            "drive_backup_folder_id": old.drive_backup_folder_id or "",
            "is_enabled": old.is_enabled,
        },
    )


def remove_legacy_q_schedule(apps, schema_editor):
    try:
        Schedule = apps.get_model("django_q", "Schedule")
        Schedule.objects.filter(name="admin_panel.db_backup").delete()
    except (LookupError, Exception):
        # django_q optional / table order — ignore
        pass


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("settings", "0008_flixbd_integration"),
        ("admin_panel", "0003_remove_backupsettings_last_run_fields"),
    ]

    operations = [
        migrations.CreateModel(
            name="BackupSettings",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "drive_backup_folder_id",
                    models.CharField(
                        max_length=255,
                        help_text="Google Drive folder ID where backup archives are uploaded.",
                    ),
                ),
                (
                    "is_enabled",
                    models.BooleanField(
                        default=True,
                        help_text="If off, the backup schedule is removed and the job no-ops.",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "google_config",
                    models.ForeignKey(
                        blank=True,
                        help_text="Drive OAuth credentials. Leave empty to use any available GoogleConfig when the backup job runs.",
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="settings.googleconfig",
                    ),
                ),
            ],
            options={
                "verbose_name": "Backup settings",
                "verbose_name_plural": "Backup settings",
            },
        ),
        migrations.RunPython(copy_backup_from_admin_panel, noop_reverse),
        migrations.RunPython(remove_legacy_q_schedule, noop_reverse),
    ]
