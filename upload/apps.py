import os
import shutil
import logging
from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class UploadConfig(AppConfig):
    name = 'upload'
    default_auto_field = 'django.db.models.BigAutoField'

    def ready(self):
        """Auto cleanup on server start: re-queue stuck tasks + clean downloads."""
        try:
            from .models import MediaTask
            from django_q.tasks import async_task
            from django_q.models import OrmQ
            from django.db import transaction

            # ── DB-level lock: only ONE worker process will handle re-queuing ──
            # select_for_update(skip_locked=True) means other workers skip silently.
            with transaction.atomic():
                stuck = list(
                    MediaTask.objects.select_for_update(skip_locked=True)
                    .filter(status__in=['processing', 'pending'])
                )

                if not stuck:
                    # Another worker already grabbed the lock (or no stuck tasks)
                    return

                # Bulk-delete ALL stale Resume queue entries before re-queuing.
                # This prevents duplicate accumulation across container restarts.
                deleted, _ = OrmQ.objects.filter(
                    func='upload.tasks.process_media_task'
                ).delete()
                if deleted:
                    logger.debug(f"Removed {deleted} stale Resume Q entries before re-queuing")

                count = len(stuck)
                for task in stuck:
                    logger.info(
                        f"Auto-resuming {task.status} task: "
                        f"{task.title or task.url[:50]} (pk={task.pk})"
                    )




                    task.status = 'pending'
                    task.save(update_fields=['status', 'updated_at'])

                    q_id = async_task(
                        'upload.tasks.process_media_task',
                        task.pk,
                        task_name=f'Resume: {task.title or task.url[:50]}',
                    )
                    task.task_id = q_id or ''
                    task.save(update_fields=['task_id'])

                if count:
                    logger.warning(f"Auto-resumed {count} task(s) (pending+processing).")

            # Clean downloads folder (leftover partial files)
            downloads_dir = str(settings.DOWNLOADS_DIR)
            if os.path.isdir(downloads_dir):
                for item in os.listdir(downloads_dir):
                    item_path = os.path.join(downloads_dir, item)
                    try:
                        if os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                        else:
                            os.remove(item_path)
                    except Exception:
                        pass
                logger.info("Downloads directory cleaned on startup.")

        except Exception as e:
            logger.debug(f"Startup cleanup skipped: {e}")
