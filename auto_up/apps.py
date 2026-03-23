import os
from django.apps import AppConfig


class AutoUpConfig(AppConfig):
    name = 'auto_up'
    default_auto_field = 'django.db.models.BigAutoField'
    verbose_name = 'Auto Upload'

    def ready(self):
        """Register the scheduled scraping task on startup.

        Only runs in the main process (not in Gunicorn worker forks)
        to avoid DB queries during worker initialization.
        """
        # Skip in Gunicorn worker processes — they inherit the schedule
        # from the master process. Only run in manage.py or qcluster.
        if os.environ.get('GUNICORN_WORKER_PROCESS'):
            return
        try:
            from auto_up.scheduler import ensure_scheduled
            ensure_scheduled()
        except Exception:
            pass
