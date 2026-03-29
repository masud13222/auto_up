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
from upload.models import MediaTask
from upload.tasks.helpers import download_source_urls

logger = logging.getLogger(__name__)

# Minimum fuzzy score to consider a match
FUZZY_THRESHOLD = 85


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


def _fetch_candidates(base_qs, name: str, year: str = None) -> dict:
    """
    Fetch candidate tasks from DB using keyword search + fuzzy filtering.
    Returns {pk: (task, matched_by)} dict.
    """
    candidates = {}  # pk -> (task, matched_by_list)
    keywords = _get_search_keywords(name)

    # Search 1: Name only (try all keyword variants)
    for keyword in keywords:
        qs = base_qs.filter(
            Q(title__icontains=keyword) | Q(website_title__icontains=keyword)
        ).order_by("-updated_at")[:15]
        for task in qs:
            if task.pk not in candidates:
                score = _fuzzy_score(name, task.title, task.website_title)
                if score >= FUZZY_THRESHOLD:
                    candidates[task.pk] = (task, ["name_only"], score)

    # Search 2: Name + Year (if year available)
    if year:
        try:
            year_int = int(year)
            for keyword in keywords:
                qs = base_qs.filter(
                    Q(title__icontains=keyword) | Q(website_title__icontains=keyword),
                    result__year=year_int,
                ).order_by("-updated_at")[:15]
                for task in qs:
                    if task.pk not in candidates:
                        score = _fuzzy_score(name, task.title, task.website_title)
                        if score >= FUZZY_THRESHOLD:
                            candidates[task.pk] = (task, ["name_with_year"], score)
                    else:
                        _, matched_by, _ = candidates[task.pk]
                        if "name_with_year" not in matched_by:
                            matched_by.append("name_with_year")
        except (ValueError, TypeError):
            logger.warning(f"Invalid year value: {year}")

    return candidates


def search_existing(name: str, year: str = None) -> dict:
    """
    Search MediaTask for matching entries using fuzzy matching.
    
    Strategy:
      1. Fetch candidates from DB using keyword search (broad)
      2. Score each candidate with rapidfuzz partial_ratio
      3. Keep only matches above FUZZY_THRESHOLD (75)
      4. Return deduplicated list with rich info

    Returns a DEDUPLICATED list — same PK never appears twice.
    Each result has a `matched_by` field showing which queries hit.
    """
    base_qs = MediaTask.objects.exclude(result__isnull=True)
    candidates = _fetch_candidates(base_qs, name, year)

    # Build result list, sorted by fuzzy score (best first)
    seen_pks = {}
    for pk, (task, matched_by, score) in sorted(candidates.items(), key=lambda x: x[1][2], reverse=True):
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

    results = list(seen_pks.values())
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
    }
