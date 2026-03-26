"""
django-q Schedule for database backup — created/removed from BackupSettings state.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from django.utils import timezone

logger = logging.getLogger(__name__)

BACKUP_SCHEDULE_NAME = "admin_panel.db_backup"
BACKUP_FUNC = "admin_panel.tasks.run_database_backup"


def backup_config_ready() -> bool:
    """True when a daily backup schedule should exist."""
    from admin_panel.models import BackupSettings
    from settings.models import GoogleConfig

    row = BackupSettings.objects.filter(pk=1).first()
    if not row or not row.is_enabled:
        return False
    if not (row.drive_backup_folder_id or "").strip():
        return False
    if row.google_config_id:
        return True
    return GoogleConfig.objects.exists()


def ensure_backup_schedule():
    """
    Create django-q daily schedule when backup config is valid; remove it otherwise.
    First next_run is tomorrow (UTC) so a cold server start does not run backup immediately.
    """
    try:
        from django_q.models import Schedule
    except Exception as e:
        logger.debug(f"ensure_backup_schedule skipped: {e}")
        return

    try:
        ready = backup_config_ready()
        existing = Schedule.objects.filter(name=BACKUP_SCHEDULE_NAME).first()

        if not ready:
            if existing:
                existing.delete()
                logger.info("Removed DB backup django-q schedule (backup disabled or incomplete).")
            return

        if existing:
            return

        first_run = timezone.now() + timedelta(days=1)
        Schedule.objects.create(
            name=BACKUP_SCHEDULE_NAME,
            func=BACKUP_FUNC,
            schedule_type=Schedule.DAILY,
            repeats=-1,
            next_run=first_run,
        )
        logger.info(
            "Created DB backup django-q schedule (daily, first run %s UTC).",
            first_run.isoformat(),
        )
    except Exception as e:
        logger.warning("ensure_backup_schedule failed: %s", e, exc_info=True)
