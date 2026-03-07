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
        """Auto cleanup on server start: reset stuck tasks + clean downloads."""
        # Avoid running twice in dev (autoreload runs ready() twice)
        if os.environ.get('RUN_MAIN') != 'true':
            return

        try:
            from .models import MovieTask

            # Reset stuck tasks
            stuck = MovieTask.objects.filter(status__in=['processing', 'pending'])
            count = stuck.update(status='failed', error_message='Reset on server restart')
            if count:
                logger.warning(f"Reset {count} stuck task(s) to failed.")

            # Clean downloads folder
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
