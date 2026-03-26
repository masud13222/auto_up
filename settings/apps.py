import logging
import os
import sys
import threading

from django.apps import AppConfig

logger = logging.getLogger(__name__)


def _is_server_or_queue_process():
    joined = " ".join(sys.argv).lower()
    if "runserver" in joined or "qcluster" in joined or "gunicorn" in joined:
        return True
    return os.path.basename(sys.argv[0]).lower() == "gunicorn"


def _ensure_backup_schedule_deferred():
    try:
        from settings.scheduler import ensure_backup_schedule

        ensure_backup_schedule()
    except Exception as e:
        logger.debug("ensure_backup_schedule on startup: %s", e)


class SettingsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "settings"
    verbose_name = "Settings"

    def ready(self):
        import settings.signals  # noqa: F401

        if not _is_server_or_queue_process():
            return
        threading.Timer(1.0, _ensure_backup_schedule_deferred).start()
