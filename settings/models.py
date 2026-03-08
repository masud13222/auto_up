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
    worker_count = models.PositiveIntegerField(default=1, help_text="Number of queue workers (requires qcluster restart)")
    extra_res_below = models.BooleanField(
        default=False,
        help_text="Allow resolutions below 480p (e.g. 360p, 240p)"
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
        return f"Upload Settings (Workers: {self.worker_count}, Folder: {self.upload_folder_id})"
