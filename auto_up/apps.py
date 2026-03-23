import os
from django.apps import AppConfig

import logging
logger = logging.getLogger(__name__)


class AutoUpConfig(AppConfig):
    name = 'auto_up'
    default_auto_field = 'django.db.models.BigAutoField'
    verbose_name = 'Auto Upload'

    def ready(self):
        """Register the scheduled scraping task on startup.

        Skipped in Gunicorn worker forks to avoid DB queries during
        worker initialization (which can cause worker timeouts on
        slow DB cold starts like Neon serverless).

        Runs in:
          - manage.py qcluster  (the scheduler consumer)
          - manage.py runserver (dev)
        Skipped in:
          - Gunicorn worker processes (GUNICORN_WORKER_PROCESS=1)
        """
        # Skip in Gunicorn worker processes (env set by CMD in Dockerfile)
        if os.environ.get('GUNICORN_WORKER_PROCESS'):
            return
        try:
            from auto_up.scheduler import ensure_scheduled
            ensure_scheduled()
        except Exception as e:
            # Log but don't crash — scheduler failure should not prevent startup
            logger.debug(f"Could not register auto-scrape schedule on startup: {e}")
