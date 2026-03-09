from django.apps import AppConfig


class AutoUpConfig(AppConfig):
    name = 'auto_up'
    default_auto_field = 'django.db.models.BigAutoField'
    verbose_name = 'Auto Upload'

    def ready(self):
        """Register the scheduled scraping task on startup."""
        try:
            from auto_up.scheduler import ensure_scheduled
            ensure_scheduled()
        except Exception:
            pass
