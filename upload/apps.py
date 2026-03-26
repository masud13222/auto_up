import logging
import os
import shutil
import sys
import threading

from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


def _is_server_or_queue_process():
    """Stuck requeue / queue wipe only for real app processes, not one-off management commands."""
    joined = " ".join(sys.argv).lower()
    if "runserver" in joined or "qcluster" in joined or "gunicorn" in joined:
        return True
    return os.path.basename(sys.argv[0]).lower() == "gunicorn"


def _clean_downloads_folder():
    try:
        downloads_dir = str(settings.DOWNLOADS_DIR)
        if os.path.isdir(downloads_dir):
            for item in os.listdir(downloads_dir):
                item_path = os.path.join(downloads_dir, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                except OSError:
                    pass
            logger.info("Downloads directory cleaned on startup.")
    except Exception:
        pass


def _upload_startup_cleanup():
    """Runs shortly after process start so Django app init is finished (no RuntimeWarning)."""
    try:
        from django.db import transaction
        from django_q.models import OrmQ
        from django_q.tasks import async_task

        from .models import MediaTask

        with transaction.atomic():
            stuck = list(
                MediaTask.objects.select_for_update(skip_locked=True).filter(
                    status__in=["processing", "pending"]
                )
            )

            if not stuck:
                _clean_downloads_folder()
                return

            deleted, _ = OrmQ.objects.all().delete()
            if deleted:
                logger.debug(f"Removed {deleted} stale Q entries before re-queuing")

            count = len(stuck)
            for task in stuck:
                logger.info(
                    f"Auto-resuming {task.status} task: "
                    f"{task.title or task.url[:50]} (pk={task.pk})"
                )
                task.status = "pending"
                task.save(update_fields=["status", "updated_at"])

                q_id = async_task(
                    "upload.tasks.process_media_task",
                    task.pk,
                    task_name=f"Resume: {task.title or task.url[:50]}",
                )
                task.task_id = q_id or ""
                task.save(update_fields=["task_id"])

            if count:
                logger.warning(f"Auto-resumed {count} task(s) (pending+processing).")

    except Exception as e:
        logger.debug(f"Startup cleanup skipped: {e}")

    _clean_downloads_folder()


class UploadConfig(AppConfig):
    name = "upload"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        """Re-queue stuck tasks + clean downloads after app init (deferred — avoids DB during setup)."""
        if not _is_server_or_queue_process():
            return
        # Gunicorn workers: no stuck-task DB work (qcluster handles queue); only clean downloads
        if os.environ.get("GUNICORN_WORKER_PROCESS"):
            threading.Timer(0.5, _clean_downloads_folder).start()
            return

        # Defer past django.setup() to silence "Accessing the database during app initialization"
        threading.Timer(1.0, _upload_startup_cleanup).start()
