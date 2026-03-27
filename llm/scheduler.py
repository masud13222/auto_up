"""
django-q Schedule for pruning old LLMUsage rows (retention).

Mirrors settings.backup: daily Schedule, created once on startup if missing.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from django.utils import timezone

logger = logging.getLogger(__name__)

LLM_USAGE_CLEANUP_SCHEDULE_NAME = "llm.cleanup_old_usage"
LLM_USAGE_CLEANUP_FUNC = "llm.tasks.cleanup_old_llm_usage"


def ensure_llm_usage_cleanup_schedule():
    """
    Ensure a daily django-q Schedule exists to delete LLMUsage older than retention.

    If the schedule does not exist, create it with first next_run tomorrow (UTC),
    same as DB backup — avoids running heavy delete immediately on cold start.
    """
    try:
        from django_q.models import Schedule
    except Exception as e:
        logger.debug("ensure_llm_usage_cleanup_schedule skipped: %s", e)
        return

    try:
        existing = Schedule.objects.filter(name=LLM_USAGE_CLEANUP_SCHEDULE_NAME).first()
        if existing:
            logger.debug(
                "LLM usage cleanup schedule already exists: next run %s",
                existing.next_run,
            )
            return

        first_run = timezone.now() + timedelta(days=1)
        Schedule.objects.create(
            name=LLM_USAGE_CLEANUP_SCHEDULE_NAME,
            func=LLM_USAGE_CLEANUP_FUNC,
            schedule_type=Schedule.DAILY,
            repeats=-1,
            next_run=first_run,
        )
        logger.info(
            "Created LLM usage cleanup django-q schedule (daily, first run %s UTC).",
            first_run.isoformat(),
        )
    except Exception as e:
        logger.warning("ensure_llm_usage_cleanup_schedule failed: %s", e, exc_info=True)
