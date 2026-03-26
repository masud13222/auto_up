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

logger = logging.getLogger(__name__)

FUZZY_THRESHOLD = 85


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

    base_qs = MediaTask.objects.filter(status__in=["completed", "processing"]).exclude(result__isnull=True)
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
        return sorted(k for k, v in dl.items() if v)

    resolutions = set()
    for season in result.get("seasons", []):
        for item in season.get("download_items", []):
            resolutions.update(k for k, v in item.get("resolutions", {}).items() if v)

    return sorted(resolutions)
