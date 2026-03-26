"""
Scheduler module for auto-upload.

Registers a Django-Q Schedule to run auto_scrape_and_queue periodically.
Uses safe startup logic — only creates the schedule if it doesn't exist.
"""

import logging
from datetime import timedelta

from django.utils import timezone

logger = logging.getLogger(__name__)

# Default interval in minutes
DEFAULT_INTERVAL_MINUTES = 45


def ensure_scheduled():
    """
    Ensure the auto-scrape schedule exists in Django-Q.
    Called from apps.py ready() — safe to call multiple times.

    Creates a repeating schedule that runs auto_scrape_and_queue()
    every N minutes (default: 30).
    """
    try:
        from django_q.models import Schedule

        schedule_name = "auto_up.auto_scrape"

        # Check if schedule already exists
        existing = Schedule.objects.filter(name=schedule_name).first()

        if existing:
            logger.debug(
                f"Auto-scrape schedule already exists: "
                f"every {existing.minutes} min, next run: {existing.next_run}"
            )
            return

        # Create the schedule
        schedule = Schedule.objects.create(
            name=schedule_name,
            func="auto_up.tasks.auto_scrape_and_queue",
            schedule_type=Schedule.MINUTES,
            minutes=DEFAULT_INTERVAL_MINUTES,
            repeats=-1,  # Run forever
        )

        logger.info(
            f"Created auto-scrape schedule: every {DEFAULT_INTERVAL_MINUTES} min "
            f"(schedule pk={schedule.pk})"
        )

    except Exception as e:
        # Don't crash the app if scheduling fails (e.g. DB not ready)
        logger.debug(f"Could not register auto-scrape schedule: {e}")


def update_interval(minutes: int):
    """
    Update the auto-scrape interval. Can be called from admin or management command.

    Args:
        minutes: New interval in minutes (minimum 5)
    """
    from django_q.models import Schedule

    if minutes < 5:
        raise ValueError("Interval must be at least 5 minutes")

    schedule_name = "auto_up.auto_scrape"
    schedule = Schedule.objects.filter(name=schedule_name).first()

    if schedule:
        schedule.minutes = minutes
        schedule.save(update_fields=["minutes"])
        logger.info(f"Updated auto-scrape interval to {minutes} minutes")
    else:
        # Create if doesn't exist
        Schedule.objects.create(
            name=schedule_name,
            func="auto_up.tasks.auto_scrape_and_queue",
            schedule_type=Schedule.MINUTES,
            minutes=minutes,
            repeats=-1,
            next_run=timezone.now() + timedelta(minutes=minutes),
        )
        logger.info(f"Created auto-scrape schedule: every {minutes} minutes")


def pause_schedule():
    """Pause the auto-scrape schedule."""
    from django_q.models import Schedule

    updated = Schedule.objects.filter(name="auto_up.auto_scrape").update(repeats=0)
    if updated:
        logger.info("Auto-scrape schedule paused")
    else:
        logger.warning("No auto-scrape schedule found to pause")


def resume_schedule():
    """Resume the auto-scrape schedule."""
    from django_q.models import Schedule

    updated = Schedule.objects.filter(name="auto_up.auto_scrape").update(repeats=-1)
    if updated:
        logger.info("Auto-scrape schedule resumed")
    else:
        logger.warning("No auto-scrape schedule found to resume")
