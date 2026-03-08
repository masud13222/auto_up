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
        # Avoid running twice in dev (autoreload runs ready() twice)
        if os.environ.get('RUN_MAIN') != 'true':
            return

        try:
            from .models import MediaTask
            from django_q.tasks import async_task

            # Re-queue processing tasks (they were interrupted by restart)
            processing = MediaTask.objects.filter(status='processing')
            for task in processing:
                logger.info(f"Auto-resuming interrupted task: {task.title or task.url[:50]} (pk={task.pk})")
                task.status = 'pending'
                task.save(update_fields=['status', 'updated_at'])

                q_id = async_task(
                    'upload.tasks.process_media_task',
                    task.pk,
                    task_name=f'Resume: {task.title or task.url[:50]}',
                )
                task.task_id = q_id or ''
                task.save(update_fields=['task_id'])

            if processing.count():
                logger.warning(f"Auto-resumed {processing.count()} interrupted task(s).")

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
