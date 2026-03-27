"""
django-q entry points for scheduled LLM maintenance jobs.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from django.utils import timezone

from .models import LLMUsage

logger = logging.getLogger(__name__)

# Rows older than this are deleted by cleanup_old_llm_usage (django-q schedule).
LLM_USAGE_RETENTION_DAYS = 7


def cleanup_old_llm_usage() -> int:
    """
    Delete LLMUsage rows older than LLM_USAGE_RETENTION_DAYS.
    Called by django-q Schedule ``llm.cleanup_old_usage`` (daily).
    Returns number of deleted LLMUsage rows.
    """
    cutoff = timezone.now() - timedelta(days=LLM_USAGE_RETENTION_DAYS)
    deleted, _details = LLMUsage.objects.filter(created_at__lt=cutoff).delete()
    # delete() total may include related rows; LLMUsage has none — ``deleted`` is the batch count.
    if deleted:
        logger.info(
            "cleanup_old_llm_usage: removed %s row(s) older than %s days (cutoff=%s)",
            deleted,
            LLM_USAGE_RETENTION_DAYS,
            cutoff.isoformat(),
        )
    else:
        logger.debug(
            "cleanup_old_llm_usage: no rows older than %s days",
            LLM_USAGE_RETENTION_DAYS,
        )
    return deleted
