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


def _is_recent_processing_task(task, now, *, window_seconds: int = 90) -> bool:
    updated_at = getattr(task, "updated_at", None)
    if updated_at is None:
        return False
    try:
        return (now - updated_at).total_seconds() <= window_seconds
    except Exception:
        return False


def _queued_media_task_pks() -> set[int]:
    """Best-effort set of MediaTask pks already present in django-q OrmQ."""
    from django_q.models import OrmQ
    from django_q.signing import SignedPackage

    queued: set[int] = set()
    for row in OrmQ.objects.only("payload"):
        try:
            task = SignedPackage.loads(row.payload)
        except Exception:
            continue
        if not isinstance(task, dict):
            continue
        func = str(task.get("func") or "")
        args = task.get("args") or ()
        if "process_media_task" not in func or not args:
            continue
        try:
            queued.add(int(args[0]))
        except (TypeError, ValueError, IndexError):
            continue
    return queued


def _upload_startup_cleanup():
    """Runs shortly after process start so Django app init is finished (no RuntimeWarning)."""
    try:
        from django.db import transaction
        from django.utils import timezone
        from django_q.tasks import async_task

        from .models import MediaTask

        with transaction.atomic():
            now = timezone.now()
            stuck = list(
                MediaTask.objects.select_for_update(skip_locked=True).filter(
                    status__in=["processing", "pending"]
                )
            )

            if not stuck:
                _clean_downloads_folder()
                return

            queued_task_pks = _queued_media_task_pks()

            resumed = 0
            skipped_already_queued = 0
            skipped_recent_processing = 0
            for task in stuck:
                if task.pk in queued_task_pks:
                    skipped_already_queued += 1
                    logger.info(
                        "Startup resume skipped: %s task already queued/running: %s (pk=%s)",
                        task.status,
                        task.title or task.url[:50],
                        task.pk,
                    )
                    continue
                if task.status == "processing" and _is_recent_processing_task(task, now):
                    skipped_recent_processing += 1
                    logger.info(
                        "Startup resume skipped: processing task looks active already: %s (pk=%s)",
                        task.title or task.url[:50],
                        task.pk,
                    )
                    continue
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
                resumed += 1

            if resumed:
                logger.warning(f"Auto-resumed {resumed} task(s) (pending+processing).")
            if skipped_already_queued or skipped_recent_processing:
                logger.info(
                    "Startup resume skipped %s already-queued task(s) and %s recently-active processing task(s).",
                    skipped_already_queued,
                    skipped_recent_processing,
                )

    except Exception as e:
        logger.debug(f"Startup cleanup skipped: {e}")

    _clean_downloads_folder()


class UploadConfig(AppConfig):
    name = "upload"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        """Re-queue stuck tasks + clean downloads after app init (deferred — avoids DB during setup)."""
        from .django_q_priority import install_django_q_priority
        from .django_q_pusher_backpressure import install_django_q_pusher_backpressure

        install_django_q_priority()
        install_django_q_pusher_backpressure()

        if not _is_server_or_queue_process():
            return
        # Gunicorn workers: no stuck-task DB work (qcluster handles queue); only clean downloads
        if os.environ.get("GUNICORN_WORKER_PROCESS"):
            threading.Timer(0.5, _clean_downloads_folder).start()
            return

        # Defer past django.setup() to silence "Accessing the database during app initialization"
        threading.Timer(1.0, _upload_startup_cleanup).start()
