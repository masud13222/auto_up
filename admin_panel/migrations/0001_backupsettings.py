# Generated manually for BackupSettings

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("settings", "0008_flixbd_integration"),
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
                        help_text="Google Drive folder ID where backup archives are uploaded.",
                        max_length=255,
                    ),
                ),
                (
                    "is_enabled",
                    models.BooleanField(
                        default=True,
                        help_text="If off, the backup runner should skip execution.",
                    ),
                ),
                (
                    "frequency",
                    models.CharField(
                        choices=[
                            ("daily", "Daily at a fixed time"),
                            ("interval", "Every N hours"),
                        ],
                        default="daily",
                        help_text="How often the backup job is intended to run (enforced by scheduler / django-q).",
                        max_length=20,
                    ),
                ),
                (
                    "daily_run_at",
                    models.TimeField(
                        blank=True,
                        help_text="For Daily: time of day (server local clock) to run backup.",
                        null=True,
                    ),
                ),
                (
                    "interval_hours",
                    models.PositiveSmallIntegerField(
                        blank=True,
                        help_text="For Every N hours: minimum hours between backups (1–168).",
                        null=True,
                    ),
                ),
                (
                    "last_backup_at",
                    models.DateTimeField(
                        blank=True,
                        editable=False,
                        help_text="Last successful or attempted run (set by backup task).",
                        null=True,
                    ),
                ),
                (
                    "last_backup_ok",
                    models.BooleanField(
                        blank=True,
                        editable=False,
                        help_text="Whether the last run succeeded.",
                        null=True,
                    ),
                ),
                (
                    "last_backup_note",
                    models.TextField(
                        blank=True,
                        default="",
                        editable=False,
                        help_text="Error message or short status from the last run.",
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
    ]
