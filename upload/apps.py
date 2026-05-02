from django.apps import AppConfig

from upload.startup.recovery import schedule_upload_startup_hooks


class UploadConfig(AppConfig):
    name = "upload"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        schedule_upload_startup_hooks()
