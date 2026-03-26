from django.core.exceptions import ValidationError
from django.db import models


class BackupSettings(models.Model):
    """
    Singleton (pk=1): database backup to Google Drive using the same OAuth as
    ``settings.GoogleConfig`` (same flow as ``upload.service.uploader.DriveUploader``).

    A future task / management command should read this row, run pg_dump (or export),
    upload the file to ``drive_backup_folder_id``, and update last_backup_* fields.
    Schedule when to run via ``frequency`` + ``daily_run_at`` / ``interval_hours``,
    or hook django-q ``Schedule`` to that task at the same cadence.
    """

    class Frequency(models.TextChoices):
        DAILY = "daily", "Daily at a fixed time"
        INTERVAL = "interval", "Every N hours"

    google_config = models.ForeignKey(
        "settings.GoogleConfig",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text="Drive OAuth credentials. Leave empty to use any available GoogleConfig when the backup job runs.",
    )
    drive_backup_folder_id = models.CharField(
        max_length=255,
        help_text="Google Drive folder ID where backup archives are uploaded.",
    )
    is_enabled = models.BooleanField(
        default=True,
        help_text="If off, the backup runner should skip execution.",
    )
    frequency = models.CharField(
        max_length=20,
        choices=Frequency.choices,
        default=Frequency.DAILY,
        help_text="How often the backup job is intended to run (enforced by scheduler / django-q).",
    )
    daily_run_at = models.TimeField(
        null=True,
        blank=True,
        help_text="For Daily: time of day (server local clock) to run backup.",
    )
    interval_hours = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="For Every N hours: minimum hours between backups (1–168).",
    )
    last_backup_at = models.DateTimeField(
        null=True,
        blank=True,
        editable=False,
        help_text="Last successful or attempted run (set by backup task).",
    )
    last_backup_ok = models.BooleanField(
        null=True,
        blank=True,
        editable=False,
        help_text="Whether the last run succeeded.",
    )
    last_backup_note = models.TextField(
        blank=True,
        default="",
        editable=False,
        help_text="Error message or short status from the last run.",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Backup settings"
        verbose_name_plural = "Backup settings"

    def clean(self):
        super().clean()
        if self.frequency == self.Frequency.DAILY:
            if not self.daily_run_at:
                raise ValidationError(
                    {"daily_run_at": "Set a time when frequency is Daily."}
                )
        elif self.frequency == self.Frequency.INTERVAL:
            h = self.interval_hours
            if h is None or h < 1 or h > 168:
                raise ValidationError(
                    {
                        "interval_hours": "Set interval between 1 and 168 hours "
                        "when frequency is Every N hours."
                    }
                )

    def save(self, *args, **kwargs):
        self.pk = 1
        super().save(*args, **kwargs)

    def __str__(self):
        fid = self.drive_backup_folder_id or ""
        tail = fid[-8:] if len(fid) >= 8 else fid or "unset"
        return f"Backup (Drive folder …{tail})"
