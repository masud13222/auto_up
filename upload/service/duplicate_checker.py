"""
DB helpers for duplicate-related flows: fuzzy MediaTask search and resolution lists.

Main upload pipeline injects match context into the **combined** LLM prompt
(``get_combined_system_prompt``) — no separate duplicate LLM call there.

For a legacy two-step flow (title + DB + ``DUPLICATE_CHECK_PROMPT`` LLM only), see
``upload.service.duplicate_check_legacy.check_duplicate``.
"""

import logging

from django.db.models import Q

from upload.models import MediaTask
from upload.tasks.helpers import is_drive_link
from llm.schema.blocked_names import (
    LEGACY_SITE_ROW_ID_JSON_KEY,
    TARGET_SITE_ROW_ID_JSON_KEY,
)

logger = logging.getLogger(__name__)

FUZZY_THRESHOLD = 85


def _resolution_has_drive_links(entries) -> bool:
    """True if this resolution tier has at least one Google Drive URL (published state)."""
    if isinstance(entries, list):
        return any(
            isinstance(entry, dict) and is_drive_link((entry or {}).get("u"))
            for entry in entries
        )
    return is_drive_link(entries)


def coerce_matched_task_pk(value) -> int | None:
    """
    Normalize LLM `matched_task_id` for MediaTask.pk lookup.
    Accepts positive int or numeric string; rejects bool, float, empty, junk.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            v = int(s)
            return v if v > 0 else None
    return None


def coerce_target_site_row_id(value) -> int | None:
    """Normalize LLM target site row id (duplicate_check). Same rules as positive int coercion."""
    return coerce_matched_task_pk(value)


def coerce_flixbd_task_id(value) -> int | None:
    """Backward-compatible alias for coerce_target_site_row_id."""
    return coerce_target_site_row_id(value)


def site_row_id_from_duplicate_result(dup: dict | None) -> int | None:
    """Read site row id from duplicate_check; accepts current and legacy JSON keys."""
    if not dup or not isinstance(dup, dict):
        return None
    v = dup.get(TARGET_SITE_ROW_ID_JSON_KEY)
    if v is None:
        v = dup.get(LEGACY_SITE_ROW_ID_JSON_KEY)
    return coerce_target_site_row_id(v)


def _get_search_keywords(name: str) -> list[str]:
    """Generate progressively broader search keywords from a name."""
    words = name.strip().split()
    queries = [name]
    for i in range(len(words) - 1, 0, -1):
        partial = " ".join(words[:i])
        if partial != name and len(partial) >= 3:
            queries.append(partial)
    return queries


def _search_db(name: str, year: str = None, exclude_pk: int = None) -> list:
    """
    Search MediaTask for matching entries using fuzzy matching.
    Fetches broader candidates from DB, then scores with rapidfuzz.
    Returns only matches above FUZZY_THRESHOLD, sorted by score (best first).
    """
    from rapidfuzz import fuzz

    # Include pending rows too when they already have structured result data.
    # This lets later seasons see the earliest queued task as a duplicate candidate.
    base_qs = MediaTask.objects.filter(
        status__in=["pending", "processing", "partial", "completed"]
    ).exclude(result__isnull=True)
    if exclude_pk:
        base_qs = base_qs.exclude(pk=exclude_pk)

    keywords = _get_search_keywords(name)
    candidates = {}  # pk -> (task, score)

    for keyword in keywords:
        if year:
            try:
                qs = base_qs.filter(
                    Q(title__icontains=keyword) | Q(website_title__icontains=keyword),
                    result__year=int(year),
                ).order_by("-updated_at")[:10]
                for task in qs:
                    if task.pk not in candidates:
                        q = name.lower()
                        scores = []
                        if task.title:
                            scores.append(fuzz.partial_ratio(q, task.title.lower()))
                        if task.website_title:
                            scores.append(fuzz.partial_ratio(q, task.website_title.lower()))
                        score = max(scores) if scores else 0
                        if score >= FUZZY_THRESHOLD:
                            candidates[task.pk] = (task, score)
            except (ValueError, TypeError):
                pass

        qs = base_qs.filter(
            Q(title__icontains=keyword) | Q(website_title__icontains=keyword)
        ).order_by("-updated_at")[:10]
        for task in qs:
            if task.pk not in candidates:
                q = name.lower()
                scores = []
                if task.title:
                    scores.append(fuzz.partial_ratio(q, task.title.lower()))
                if task.website_title:
                    scores.append(fuzz.partial_ratio(q, task.website_title.lower()))
                score = max(scores) if scores else 0
                if score >= FUZZY_THRESHOLD:
                    candidates[task.pk] = (task, score)

    sorted_matches = sorted(candidates.values(), key=lambda x: x[1], reverse=True)
    matches = [task for task, score in sorted_matches]

    if matches:
        logger.debug(
            "DB fuzzy match for %r: %s found (scores: %s)",
            name,
            len(matches),
            [s for _, s in sorted_matches],
        )

    return matches


def _get_existing_resolutions(task: MediaTask) -> list:
    """Extract resolution keys from existing task's result. Filters out null values."""
    result = task.result or {}

    dl = result.get("download_links", {})
    if dl:
        return sorted(
            {
                str(k).strip().lower()
                for k, entries in dl.items()
                if _resolution_has_drive_links(entries)
            }
        )

    resolutions = set()
    for season in result.get("seasons", []):
        for item in season.get("download_items", []):
            resolutions.update(
                str(k).strip().lower()
                for k, entries in item.get("resolutions", {}).items()
                if _resolution_has_drive_links(entries)
            )

    return sorted(resolutions)
