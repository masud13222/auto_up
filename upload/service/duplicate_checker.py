"""
DB helpers for duplicate-related flows: fuzzy MediaTask search and resolution lists.

The upload pipeline injects match context into the combined LLM prompt
(``get_combined_system_prompt``); duplicate detection is part of that single call.
"""

import logging

from django.db.models import Q

from constant import (
    DB_DUPLICATE_LLM_MAX_CANDIDATES,
    DB_SEARCH_QUERY_SLICE_UPLOAD,
    FUZZY_THRESHOLD_DB,
)
from upload.models import MediaTask
from upload.utils.media_entry_helpers import is_drive_link
from llm.utils.search_queries import build_search_queries
from llm.schema.blocked_names import TARGET_SITE_ROW_ID_JSON_KEY

logger = logging.getLogger(__name__)

FUZZY_THRESHOLD = FUZZY_THRESHOLD_DB


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


def site_row_id_from_duplicate_result(dup: dict | None) -> int | None:
    """Read target site row id from duplicate_check."""
    if not dup or not isinstance(dup, dict):
        return None
    return coerce_target_site_row_id(dup.get(TARGET_SITE_ROW_ID_JSON_KEY))


def _get_search_keywords(name: str) -> list[str]:
    """Generate progressively broader search keywords from a name."""
    words = name.strip().split()
    queries = [name]
    for i in range(len(words) - 1, 0, -1):
        partial = " ".join(words[:i])
        if partial != name and len(partial) >= 3:
            queries.append(partial)
    return queries


def _db_candidate_fuzzy_score(
    name: str,
    year: str | None,
    task: MediaTask,
    alt_name: str | None = None,
) -> int:
    from rapidfuzz import fuzz

    def score_for_base(base: str) -> int:
        qn = (base or "").strip().lower()
        texts: list[str] = []
        if task.title:
            texts.append(task.title.lower())
        if task.website_title:
            texts.append(task.website_title.lower())
        if not texts or not qn:
            return 0
        best = 0
        for t in texts:
            best = max(best, fuzz.partial_ratio(qn, t))
            if year:
                ys = str(year).strip()
                if ys:
                    qy = f"{(base or '').strip()} {ys}".lower()
                    if qy != qn:
                        best = max(best, fuzz.partial_ratio(qy, t))
        return int(best)

    best = score_for_base(name)
    alt = (alt_name or "").strip()
    if alt and alt.lower() != (name or "").strip().lower():
        best = max(best, score_for_base(alt))
    return int(best)


def _search_db(
    name: str,
    year: str = None,
    season_tag: str = None,
    exclude_pk: int = None,
    search_debug: dict | None = None,
    alt_name: str | None = None,
) -> list:
    """
    Search MediaTask for duplicate candidates.

    1) DB broad fetch: name-only keyword queries, then (if year) name+year queries — merged by pk.
    2) Single fuzzy pass on merged tasks (name and optional name+year vs titles); keep >= FUZZY_THRESHOLD_DB.
    3) Return at most ``DB_DUPLICATE_LLM_MAX_CANDIDATES`` tasks (best fuzzy score first) for the LLM.
    """
    # Include pending rows too when they already have structured result data.
    # This lets later seasons see the earliest queued task as a duplicate candidate.
    base_qs = MediaTask.objects.filter(
        status__in=["pending", "processing", "partial", "completed"]
    ).exclude(result__isnull=True)
    if exclude_pk:
        base_qs = base_qs.exclude(pk=exclude_pk)

    query_specs = build_search_queries(
        name, year=year, season_tag=season_tag, alt_name=alt_name
    )
    if search_debug is not None:
        search_debug.clear()
        search_debug["name"] = (name or "").strip()
        search_debug["alt_name"] = (alt_name or "").strip() or None
        search_debug["year"] = str(year).strip() if year is not None and str(year).strip() else None
        search_debug["season_tag"] = (season_tag or "").strip() or None
        search_debug["query_specs"] = [dict(spec) for spec in query_specs]
        search_debug["phase_queries"] = []
    if not query_specs:
        return []
    merged: dict[int, MediaTask] = {}
    order: list[int] = []
    matched_by: dict[int, set[str]] = {}
    priority_by_pk: dict[int, int] = {}

    def _ingest_qs(qs, tag: str, priority: int):
        for task in qs:
            if task.pk not in merged:
                merged[task.pk] = task
                order.append(task.pk)
                matched_by[task.pk] = set()
                priority_by_pk[task.pk] = priority
            else:
                priority_by_pk[task.pk] = max(priority_by_pk.get(task.pk, 0), priority)
            matched_by[task.pk].add(tag)

    for spec in query_specs:
        query_text = spec["q"]
        tag = spec["tag"]
        priority = int(spec["priority"])
        keywords = _get_search_keywords(query_text)
        if search_debug is not None:
            search_debug["phase_queries"].append(
                {
                    "tag": tag,
                    "priority": priority,
                    "query": query_text,
                    "keywords": list(keywords),
                }
            )
        for keyword in keywords:
            qs = base_qs.filter(
                Q(title__icontains=keyword) | Q(website_title__icontains=keyword)
            ).order_by("-updated_at")[:DB_SEARCH_QUERY_SLICE_UPLOAD]
            _ingest_qs(qs, tag, priority)

    scored: list[tuple[MediaTask, int, int]] = []
    for pk in order:
        task = merged[pk]
        score = _db_candidate_fuzzy_score(name, year, task, alt_name=alt_name)
        if score >= FUZZY_THRESHOLD_DB:
            scored.append((task, score, priority_by_pk.get(pk, 0)))

    scored.sort(key=lambda x: (x[2], x[1]), reverse=True)
    matches = [t for t, _, _ in scored[:DB_DUPLICATE_LLM_MAX_CANDIDATES]]
    if search_debug is not None:
        search_debug["merged_candidate_count"] = len(order)
        search_debug["after_fuzzy_count"] = len(scored)
        search_debug["llm_max_db_rows"] = DB_DUPLICATE_LLM_MAX_CANDIDATES

    if matches:
        logger.debug(
            "DB fuzzy match for %r: %s found (scores: %s)",
            name,
            len(matches),
            [s for _, s in scored[: len(matches)]],
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
