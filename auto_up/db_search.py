"""
Database search module for auto-upload.

Searches MediaTask for existing entries matching an extracted title,
using fuzzy matching (rapidfuzz) for robust results. Returns deduplicated
rich results including resolution info, episode details, and website title
so the LLM can make informed decisions.
"""

import logging
from django.db.models import Q
from rapidfuzz import fuzz
from constant import (
    AUTO_UP_DB_LLM_MAX_CANDIDATES,
    DB_SEARCH_QUERY_SLICE_AUTO_UP,
    FUZZY_THRESHOLD_DB,
)
from upload.models import MediaTask
from upload.utils.media_entry_helpers import download_source_urls
from llm.utils.search_queries import build_search_queries

logger = logging.getLogger(__name__)

FUZZY_THRESHOLD = FUZZY_THRESHOLD_DB


def _extract_rich_info(task: MediaTask) -> dict:
    """
    Extract rich info from a MediaTask's result field for LLM comparison.
    Per-episode resolution detail for TV shows.
    """
    result = task.result or {}
    info = {}

    # Website title
    website_title = result.get("website_movie_title") or result.get("website_tvshow_title") or ""
    if website_title:
        info["website_title"] = website_title

    # Year from result
    year = result.get("year")
    if year:
        info["year"] = year

    # ── Movie resolutions ──
    download_links = result.get("download_links", {})
    if download_links:
        # Only count resolutions with actual non-null download URLs
        info["resolutions"] = sorted(
            k
            for k, v in download_links.items()
            if (
                bool(download_source_urls(v))
                or any(
                    download_source_urls((entry or {}).get("u"))
                    for entry in (v if isinstance(v, list) else [])
                )
            )
        )

    # ── TV Show details ──
    seasons = result.get("seasons", [])
    if seasons:
        episode_count = 0
        episodes = []
        season_numbers = []

        for season in seasons:
            season_num = season.get("season_number")
            if season_num is not None:
                season_numbers.append(season_num)

            for item in season.get("download_items", []):
                episode_count += 1
                label = item.get("label", "")
                resolutions = item.get("resolutions", {})
                # Only count resolutions with actual non-null download URLs
                ep_res = sorted(
                    k
                    for k, v in resolutions.items()
                    if (
                        bool(download_source_urls(v))
                        or any(
                            download_source_urls((entry or {}).get("u"))
                            for entry in (v if isinstance(v, list) else [])
                        )
                    )
                )

                episodes.append(f"{label}: {','.join(ep_res)}")

        info["season_numbers"] = season_numbers
        info["total_episodes"] = episode_count
        info["episodes"] = episodes

    return info


def _get_search_keywords(name: str) -> list[str]:
    """
    Generate search keywords from a name for broader DB candidate fetching.
    Returns list of queries to try, from most specific to broadest.
    
    Example: "Bachelor Point 5" → ["Bachelor Point 5", "Bachelor Point", "Bachelor"]
    """
    words = name.strip().split()
    queries = [name]  # Full name always first

    # Add progressively shorter prefixes (min 1 word)
    for i in range(len(words) - 1, 0, -1):
        partial = " ".join(words[:i])
        if partial != name and len(partial) >= 3:
            queries.append(partial)

    return queries


def _fuzzy_score(query: str, candidate_title: str, candidate_web_title: str) -> int:
    """
    Compute best fuzzy match score between query and candidate's titles.
    Uses partial_ratio for substring matching ("Bachelor Point" in "Bachelor Point 5").
    Returns the best score (0-100).
    """
    q = query.lower()
    scores = []

    if candidate_title:
        scores.append(fuzz.partial_ratio(q, candidate_title.lower()))
    if candidate_web_title:
        scores.append(fuzz.partial_ratio(q, candidate_web_title.lower()))

    return max(scores) if scores else 0


def _fuzzy_score_merged(
    name: str,
    year: str | None,
    season_tag: str | None,
    candidate_title: str,
    candidate_web_title: str,
    alt_name: str | None = None,
) -> int:
    s = 0
    texts = []
    if candidate_title:
        texts.append(candidate_title.lower())
    if candidate_web_title:
        texts.append(candidate_web_title.lower())
    if not texts:
        return s
    for spec in build_search_queries(
        name, year=year, season_tag=season_tag, alt_name=alt_name
    ):
        q = spec["q"].lower()
        for t in texts:
            s = max(s, fuzz.partial_ratio(q, t))
    return int(s)


def _fetch_candidates(
    base_qs,
    name: str,
    year: str = None,
    season_tag: str | None = None,
    alt_name: str | None = None,
) -> dict:
    """
    Broad DB fetch in two phases (name-only keywords, then name+year keywords), merged by pk,
    then a single fuzzy pass (name and optional name+year vs titles).
    Returns {pk: (task, matched_by_list, score)}.
    """
    candidates: dict = {}
    query_specs = build_search_queries(
        name, year=year, season_tag=season_tag, alt_name=alt_name
    )
    merged: dict[int, tuple] = {}  # pk -> (task, matched_by list)
    order: list[int] = []
    priority_by_pk: dict[int, int] = {}

    def _ingest(qs, tag: str, priority: int) -> None:
        for task in qs:
            if task.pk not in merged:
                merged[task.pk] = (task, [tag])
                order.append(task.pk)
                priority_by_pk[task.pk] = priority
            else:
                _, tags = merged[task.pk]
                if tag not in tags:
                    tags.append(tag)
                priority_by_pk[task.pk] = max(priority_by_pk.get(task.pk, 0), priority)

    for spec in query_specs:
        query_text = spec["q"]
        tag = spec["tag"]
        priority = int(spec["priority"])
        keywords = _get_search_keywords(query_text)
        for keyword in keywords:
            qs = base_qs.filter(
                Q(title__icontains=keyword) | Q(website_title__icontains=keyword)
            ).order_by("-updated_at")[:DB_SEARCH_QUERY_SLICE_AUTO_UP]
            _ingest(qs, tag, priority)

    for pk in order:
        task, matched_by = merged[pk]
        score = _fuzzy_score_merged(
            name, year, season_tag, task.title, task.website_title, alt_name=alt_name
        )
        if score >= FUZZY_THRESHOLD_DB:
            candidates[task.pk] = (task, matched_by, score, priority_by_pk.get(task.pk, 0))

    return candidates, query_specs


def search_existing(
    name: str,
    year: str = None,
    season_tag: str | None = None,
    alt_name: str | None = None,
) -> dict:
    """
    Search MediaTask for matching entries using fuzzy matching.
    
    Strategy:
      1. Broad fetch: name-only keyword queries, then (if year) name+year queries — merged by pk
      2. One fuzzy pass per merged row (name and optional name+year vs titles); keep >= ``FUZZY_THRESHOLD_DB``
      3. Return at most ``AUTO_UP_DB_LLM_MAX_CANDIDATES`` rows (best fuzzy score first), deduplicated

    Returns a DEDUPLICATED list — same PK never appears twice.
    Each result has a `matched_by` field showing which queries hit.
    """
    base_qs = MediaTask.objects.exclude(result__isnull=True)
    candidates, query_specs = _fetch_candidates(
        base_qs, name, year, season_tag=season_tag, alt_name=alt_name
    )

    # Build result list, sorted by fuzzy score (best first)
    seen_pks = {}
    for pk, (task, matched_by, score, priority) in sorted(
        candidates.items(),
        key=lambda x: (x[1][3], x[1][2]),
        reverse=True,
    ):
        rich = _extract_rich_info(task)
        seen_pks[pk] = {
            "task_pk": pk,
            "matched_by": matched_by,
            "fuzzy_score": score,
            "title": task.title,
            "status": task.status,
            "content_type": task.content_type,
            "url": task.url,
            **rich,
        }

    results = list(seen_pks.values())[:AUTO_UP_DB_LLM_MAX_CANDIDATES]
    has_matches = bool(results)

    if has_matches:
        logger.info(
            f"DB search for '{name}' (year={year}): "
            f"{len(results)} unique match(es) "
            f"(scores: {[r['fuzzy_score'] for r in results]})"
        )
    else:
        logger.debug(f"DB search for '{name}' (year={year}): no matches found")

    return {
        "results": results,
        "has_matches": has_matches,
        "search_debug": {
            "name": (name or "").strip(),
            "year": str(year).strip() if year is not None and str(year).strip() else None,
            "season_tag": (season_tag or "").strip() or None,
            "query_specs": [dict(spec) for spec in query_specs],
            "llm_max_db_rows": AUTO_UP_DB_LLM_MAX_CANDIDATES,
        },
    }
