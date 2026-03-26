import logging
import os
import threading

from django.apps import AppConfig

logger = logging.getLogger(__name__)


def _ensure_scheduled_deferred():
    try:
        from auto_up.scheduler import ensure_scheduled

        ensure_scheduled()
    except Exception as e:
        logger.debug(f"Could not register auto-scrape schedule on startup: {e}")


class AutoUpConfig(AppConfig):
    name = "auto_up"
    default_auto_field = "django.db.models.BigAutoField"
    verbose_name = "Auto Upload"

    def ready(self):
        """Register the scheduled scraping task shortly after startup (avoids DB during app init).

        Skipped in Gunicorn workers.
        """
        if os.environ.get("GUNICORN_WORKER_PROCESS"):
            return
        threading.Timer(1.0, _ensure_scheduled_deferred).start()
