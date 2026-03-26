from django.db import models


class BackupSettings(models.Model):
    """
    Singleton (pk=1): Drive folder + OAuth selection for DB backups.

    When configuration is valid, ``ensure_backup_schedule`` registers django-q task
    ``admin_panel.db_backup``. Success/failure history: Django Q → Successful tasks / Failures.
    """

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
        help_text="If off, the backup schedule is removed and the job no-ops.",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Backup settings"
        verbose_name_plural = "Backup settings"

    def save(self, *args, **kwargs):
        self.pk = 1
        super().save(*args, **kwargs)

    def __str__(self):
        fid = self.drive_backup_folder_id or ""
        tail = fid[-8:] if len(fid) >= 8 else fid or "unset"
        return f"Backup (Drive folder …{tail})"
