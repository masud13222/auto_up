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


def _ensure_llm_cleanup_schedule_deferred():
    try:
        from llm.scheduler import ensure_llm_usage_cleanup_schedule

        ensure_llm_usage_cleanup_schedule()
    except Exception as e:
        logger.debug("ensure_llm_usage_cleanup_schedule on startup: %s", e)


class LlmConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "llm"
    verbose_name = "LLM"

    def ready(self):
        if os.environ.get("GUNICORN_WORKER_PROCESS"):
            return
        if not _is_server_or_queue_process():
            return
        threading.Timer(1.0, _ensure_llm_cleanup_schedule_deferred).start()
