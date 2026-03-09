"""
Database search module for auto-upload.

Searches MediaTask for existing entries matching an extracted title,
both with and without year. Returns all results along with which query
type produced them.
"""

import logging
from upload.models import MediaTask

logger = logging.getLogger(__name__)


def search_existing(name: str, year: str = None) -> dict:
    """
    Search MediaTask for matching entries using two strategies:
      1. Name only (broader search)
      2. Name + year (more specific)

    Returns:
        {
            "name_only_results": [
                {"task": MediaTask, "query": "name_only", "title": ..., "status": ...},
                ...
            ],
            "name_year_results": [
                {"task": MediaTask, "query": "name_with_year", "title": ..., "status": ...},
                ...
            ],
            "has_matches": bool,
        }
    """
    base_qs = MediaTask.objects.exclude(result__isnull=True)

    # Search 1: Name only
    name_only_qs = base_qs.filter(title__icontains=name).order_by("-updated_at")[:10]
    name_only_results = [
        {
            "task_pk": task.pk,
            "query": "name_only",
            "title": task.title,
            "status": task.status,
            "content_type": task.content_type,
            "url": task.url,
        }
        for task in name_only_qs
    ]

    # Search 2: Name + Year (if year available)
    name_year_results = []
    if year:
        try:
            year_int = int(year)
            name_year_qs = base_qs.filter(
                title__icontains=name, result__year=year_int
            ).order_by("-updated_at")[:10]
            name_year_results = [
                {
                    "task_pk": task.pk,
                    "query": "name_with_year",
                    "title": task.title,
                    "status": task.status,
                    "content_type": task.content_type,
                    "url": task.url,
                }
                for task in name_year_qs
            ]
        except (ValueError, TypeError):
            logger.warning(f"Invalid year value: {year}")

    has_matches = bool(name_only_results) or bool(name_year_results)

    if has_matches:
        logger.info(
            f"DB search for '{name}' (year={year}): "
            f"{len(name_only_results)} name-only, "
            f"{len(name_year_results)} name+year matches"
        )
    else:
        logger.debug(f"DB search for '{name}' (year={year}): no matches found")

    return {
        "name_only_results": name_only_results,
        "name_year_results": name_year_results,
        "has_matches": has_matches,
    }
