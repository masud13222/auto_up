import os
from django.db import models
from django.core.exceptions import ValidationError

def validate_json_extension(value):
    ext = os.path.splitext(value.name)[1]
    if not ext.lower() == '.json':
        raise ValidationError('Only .json files are allowed.')

# Create your models here.
class GoogleConfig(models.Model):
    name = models.CharField(max_length=255, default='Default Google Config')
    config_file = models.FileField(upload_to='google_configs/', validators=[validate_json_extension], help_text="Upload your Google token.json or credential JSON file")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Google Config"
        verbose_name_plural = "Google Configs"

    def __str__(self):
        return self.name

class UploadSettings(models.Model):
    upload_folder_id = models.CharField(max_length=255, help_text="Google Drive Folder ID for uploads")
    extra_res_below = models.BooleanField(
        default=False,
        help_text="Allow non-standard resolutions below 720p (e.g. 520p, 360p, 240p). 480p is always included."
    )
    extra_res_above = models.BooleanField(
        default=False,
        help_text="Allow resolutions above 1080p (e.g. 2160p, 4K)"
    )
    max_extra_resolutions = models.PositiveIntegerField(
        default=0,
        help_text="Max extra resolutions to include (0 = unlimited)"
    )
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Upload Settings"
        verbose_name_plural = "Upload Settings"

    def save(self, *args, **kwargs):
        self.pk = 1
        super(UploadSettings, self).save(*args, **kwargs)

    def __str__(self):
        return f"Upload Settings (Folder: {self.upload_folder_id})"


class FlixBDSettings(models.Model):
    """
    Singleton config for the FlixBD target site API.
    Set API URL and API Key from Admin → Settings → FlixBD Settings.
    """
    api_url = models.URLField(
        max_length=500,
        help_text="Base URL of the FlixBD site (e.g. https://flixbd.test)"
    )
    api_key = models.CharField(
        max_length=255,
        help_text="API key from Admin → Settings → Integrations on the FlixBD site"
    )
    is_enabled = models.BooleanField(
        default=True,
        help_text="Enable or disable auto-publishing to FlixBD"
    )
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "FlixBD Settings"
        verbose_name_plural = "FlixBD Settings"

    def save(self, *args, **kwargs):
        self.pk = 1
        super().save(*args, **kwargs)

    def __str__(self):
        return f"FlixBD Settings ({'enabled' if self.is_enabled else 'disabled'}) — {self.api_url}"


class BackupSettings(models.Model):
    """
    Singleton (pk=1): Drive folder + OAuth selection for DB backups.

    When configuration is valid, ``ensure_backup_schedule`` registers django-q task
    ``settings.db_backup``. History: Django Q → Successful tasks / Failures.
    """

    google_config = models.ForeignKey(
        GoogleConfig,
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

