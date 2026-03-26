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
        from admin_panel.scheduler import ensure_backup_schedule

        ensure_backup_schedule()
    except Exception as e:
        logger.debug(f"ensure_backup_schedule on startup: {e}")


class AdminPanelConfig(AppConfig):
    name = "admin_panel"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        import admin_panel.signals  # noqa: F401

        # Gunicorn workers must run this too — otherwise backup Schedule is never created on web-only deploys.
        if not _is_server_or_queue_process():
            return
        threading.Timer(1.0, _ensure_backup_schedule_deferred).start()
