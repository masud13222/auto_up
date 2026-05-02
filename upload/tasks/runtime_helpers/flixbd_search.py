"""
FlixBD search: query building, fuzzy scoring, phased API merge, and slim rows for the LLM.
"""

from __future__ import annotations

import logging
import re

from constant import (
    FLIXBD_FUZZY_THRESHOLD,
    FLIXBD_LLM_MAX_RESULTS,
    FLIXBD_SEARCH_PER_PAGE,
)
from llm.utils.search_queries import build_search_queries

from .entry_helpers import _normalize_flixbd_row_id

logger = logging.getLogger(__name__)


def flixbd_slim_qualities_from_download_links(download_links: dict | None) -> list[str]:
    """
    Build string fragments for :func:`normalize_flixbd_resolution_keys`.

    Movies: ``download_links.qualities`` (comma-separated string or list).
    Series (FlixBD API v1): ``download_links.episodes_range`` (list of lines like
    ``S01: 1080p,480p,720p`` or ``S01 Episode 01-04: 1080p,480p,720p``).
    """
    dl = download_links if isinstance(download_links, dict) else {}
    qualities: list[str] = []
    qualities_raw = dl.get("qualities")
    if isinstance(qualities_raw, str):
        qualities = [q.strip() for q in qualities_raw.split(",") if q.strip()]
    elif isinstance(qualities_raw, list):
        qualities = [str(q).strip() for q in qualities_raw if str(q).strip()]
    if qualities:
        return qualities
    ep_raw = dl.get("episodes_range")
    if isinstance(ep_raw, list):
        return [str(x).strip() for x in ep_raw if str(x).strip()]
    if isinstance(ep_raw, str) and ep_raw.strip():
        return [ep_raw.strip()]
    return []


def normalize_flixbd_resolution_keys(qualities: list) -> list[str]:
    """
    Map API strings like 'HD 720p, HD 1080p' to canonical keys ['720p', '1080p'] for LLM comparison
    against extracted download_links keys (480p, 720p, 1080p).
    """
    if not qualities:
        return []
    blob = " ".join(str(q) for q in qualities)
    seen: set[str] = set()
    out: list[str] = []
    for m in re.finditer(r"\b(480|720|1080|1440|2160)\s*p\b", blob, re.I):
        key = f"{m.group(1)}p"
        if key not in seen:
            seen.add(key)
            out.append(key)
    if re.search(r"\b4\s*k\b", blob, re.I) and "2160p" not in seen:
        seen.add("2160p")
        out.append("2160p")
    order = {"480p": 0, "720p": 1, "1080p": 2, "1440p": 3, "2160p": 4}
    return sorted(out, key=lambda k: order.get(k, 99))


def flixbd_search_query(name: str, year: str | int | None = None) -> str:
    """
    Build FlixBD search API ``q`` string: cleaned title, plus year when present
    and not already redundant at the end of the title (e.g. avoid ``Bandi 2026 2026``).
    """
    n = (name or "").strip()
    if not n:
        return ""
    if year is None:
        return n
    ys = str(year).strip()
    if not ys:
        return n
    if n.endswith(ys):
        return n
    return f"{n} {ys}"


def _flixbd_title_fuzzy_score(
    name: str,
    year: str | int | None,
    title: str,
    season_tag: str | None = None,
    alt_name: str | None = None,
) -> int:
    from rapidfuzz import fuzz

    t = (title or "").lower()
    if not t:
        return 0
    best = 0
    for spec in build_search_queries(
        name, year=year, season_tag=season_tag, alt_name=alt_name
    ):
        q = spec["q"].lower()
        if q:
            best = max(best, fuzz.partial_ratio(q, t))
    return int(best)


def _flixbd_merge_two_phase_raw(
    name: str,
    year: str | int | None,
    season_tag: str | None = None,
    *,
    alt_name: str | None = None,
    per_page: int,
    api_url: str,
    api_key: str,
) -> tuple[list[dict], list[str], int]:
    """
    Run FlixBD search across prioritized queries and merge rows by ``id``.

    Returns ``(merged_items, queries_used, phases_without_payload_count)``.
    """
    from upload.service.flixbd_api_base import flixbd_search_response_dict

    merged: list[dict] = []
    seen: set[int | str] = set()
    queries_run: list[str] = []
    phases_no_payload = 0

    def _phase(q: str) -> None:
        nonlocal phases_no_payload
        q = (q or "").strip()
        if not q:
            return
        if queries_run and queries_run[-1] == q:
            return
        queries_run.append(q)
        body = flixbd_search_response_dict(
            api_url, api_key, {"q": q, "type": "all", "per_page": per_page, "page": 1}
        )
        if not body:
            phases_no_payload += 1
            return
        for item in body.get("data", []) or []:
            if not isinstance(item, dict):
                continue
            nk = _normalize_flixbd_row_id(item.get("id"))
            if nk is None:
                continue
            if nk in seen:
                continue
            seen.add(nk)
            row = dict(item)
            row["id"] = nk
            merged.append(row)

    query_specs = build_search_queries(
        name, year=year, season_tag=season_tag, alt_name=alt_name
    )
    for spec in sorted(query_specs, key=lambda item: int(item["priority"]), reverse=True):
        _phase(spec["q"])

    return merged, queries_run, phases_no_payload


def fetch_flixbd_results(
    name: str,
    *,
    year: str | int | None = None,
    season_tag: str | None = None,
    alt_name: str | None = None,
    fetch_debug: dict | None = None,
) -> list:
    """
    FlixBD: prioritized API phases (name, name+year, name+year+season_tag when available), merged by ``id``,
    then fuzzy filter on titles (>= ``FLIXBD_FUZZY_THRESHOLD``), best scores first, capped at
    ``FLIXBD_LLM_MAX_RESULTS``. Slim rows for the LLM (no match_score field).

    If ``fetch_debug`` is a dict, it is cleared and filled with ``name``, ``year``, ``queries``,
    ``merged_raw_count``, ``after_fuzzy_count``, ``status``, optional ``message``.
    """
    def _mark(status: str, message: str | None = None) -> None:
        if fetch_debug is not None:
            fetch_debug["status"] = status
            if message:
                fetch_debug["message"] = message

    if fetch_debug is not None:
        fetch_debug.clear()
        fetch_debug["name"] = name
        fetch_debug["alt_name"] = (alt_name or "").strip() or None
        fetch_debug["year"] = str(year).strip() if year is not None and str(year).strip() else None
        fetch_debug["season_tag"] = str(season_tag).strip() if season_tag else None

    q_label = (name or "").strip()
    if not q_label:
        _mark("skipped", "empty search query")
        return []

    try:
        from upload.service import flixbd_client as fx

        api_url, api_key = fx._get_config()

        raw_merged, queries_run, phases_no_payload = _flixbd_merge_two_phase_raw(
            name,
            year,
            season_tag=season_tag,
            alt_name=alt_name,
            per_page=FLIXBD_SEARCH_PER_PAGE,
            api_url=api_url,
            api_key=api_key,
        )

        if fetch_debug is not None:
            fetch_debug["queries"] = list(queries_run)
            fetch_debug["per_phase_per_page"] = FLIXBD_SEARCH_PER_PAGE
            fetch_debug["merged_raw_count"] = len(raw_merged)
            fetch_debug["llm_max_flixbd_rows"] = FLIXBD_LLM_MAX_RESULTS
            fetch_debug["fuzzy_threshold"] = FLIXBD_FUZZY_THRESHOLD

        if not raw_merged:
            if queries_run and phases_no_payload >= len(queries_run):
                logger.debug("FlixBD search: no usable JSON for phases %s", queries_run)
                _mark("no_payload", "No usable JSON from search API")
            else:
                logger.info("FlixBD search: no merged rows for %r (queries=%s)", name, queries_run)
                _mark("empty", "API returned no rows for merged phases")
            return []

        scored: list[tuple[int, dict]] = []
        for item in raw_merged:
            fid = item.get("id")
            item_title = item.get("title", "") or ""
            if fid is None:
                logger.debug("FlixBD search: skipping hit without id: %r", item_title[:80])
                continue
            download_links = item.get("download_links") or {}

            row: dict = {
                "id": fid,
                "title": item_title,
            }
            # Pass through API download_links only (qualities / episodes_range). Omit derived
            # resolution_keys — same info is parseable from download_links; saves LLM tokens.
            if download_links:
                row["download_links"] = dict(download_links)
            rd = item.get("release_date")
            if rd is not None and rd != "":
                row["release_date"] = rd

            fs = _flixbd_title_fuzzy_score(
                name, year, item_title, season_tag=season_tag, alt_name=alt_name
            )
            if fs >= FLIXBD_FUZZY_THRESHOLD:
                scored.append((fs, row))

        scored.sort(key=lambda x: x[0], reverse=True)
        if fetch_debug is not None:
            fetch_debug["passed_fuzzy_count"] = len(scored)
        results = [r for _, r in scored[:FLIXBD_LLM_MAX_RESULTS]]

        if fetch_debug is not None:
            fetch_debug["after_fuzzy_count"] = len(results)

        if not results:
            if raw_merged:
                _mark("empty", "No merged row passed fuzzy threshold")
                logger.info(
                    "FlixBD search: 0 rows after fuzzy (threshold=%s) for %r; merged=%s",
                    FLIXBD_FUZZY_THRESHOLD,
                    name,
                    len(raw_merged),
                )
            else:
                _mark("parsed_empty", "Merged list empty after parse")
        else:
            _mark("ok")

        logger.info(
            "FlixBD search: %s result(s) (cap=%s, fuzzy>=%s) for %r queries=%s",
            len(results),
            FLIXBD_LLM_MAX_RESULTS,
            FLIXBD_FUZZY_THRESHOLD,
            name,
            queries_run,
        )
        return results

    except RuntimeError as e:
        logger.debug("FlixBD search skipped: %s", e)
        _mark("skipped", str(e))
        return []
    except Exception as e:
        logger.warning("FlixBD search error for %r: %s", name, e)
        _mark("error", str(e))
        return []
