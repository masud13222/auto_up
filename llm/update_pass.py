"""
Pass-2: Delta-only update filtering.

Called ONLY when Pass-1 duplicate_check returns action="update".
Takes the full extracted data + existing coverage from the matched DB candidate,
sends both to LLM with a focused delta-filter prompt, and returns only the missing parts.
"""

import json
import logging

from llm.services import LLMService
from llm.json_repair import repair_json
from llm.schema.update_schema import get_update_system_prompt

logger = logging.getLogger(__name__)

_COMPACT = {"separators": (",", ":")}
_JSON_RETRY_MAX = 1
_JSON_RETRY_SUFFIX = (
    "\n\nReturn a single valid JSON object only (no markdown fences, no text outside JSON). "
    "Previous attempt produced invalid JSON."
)


def _build_existing_summary(content_type: str, db_candidate: dict) -> dict:
    """Extract the relevant coverage data from a DB candidate for comparison.

    DB candidates built by build_db_candidate() have a different structure
    than raw result dicts:
      Movie candidate: {"id":..., "resolutions": ["480p","720p",...]}
      TV candidate:    {"id":..., "tv_items": [{"season_number":1,"episode_range":"01-08","resolutions":["720p","1080p"]},..]}
    We also support raw result dicts (with download_links / seasons).
    """
    if content_type == "movie":
        # Try raw result shape first
        existing_links = db_candidate.get("download_links") or db_candidate.get("result", {}).get("download_links", {})
        if isinstance(existing_links, dict) and existing_links:
            summary = {}
            for res_key, entries in existing_links.items():
                if isinstance(entries, list) and entries:
                    summary[res_key] = [f"{len(entries)} file(s)"]
            if summary:
                return {"download_links": summary}

        # Fallback: candidate format from build_db_candidate (resolutions = flat list of keys)
        res_list = db_candidate.get("resolutions")
        if isinstance(res_list, list) and res_list:
            return {"download_links": {r: ["present"] for r in res_list}}

        return {"download_links": {}}

    # TV: try raw result shape first
    seasons_raw = db_candidate.get("seasons") or db_candidate.get("result", {}).get("seasons", [])
    if isinstance(seasons_raw, list) and seasons_raw:
        seasons_summary = []
        for s in seasons_raw:
            if not isinstance(s, dict):
                continue
            snum = s.get("season_number")
            items = []
            for item in s.get("download_items", []):
                if not isinstance(item, dict):
                    continue
                items.append({
                    "type": item.get("type", ""),
                    "episode_range": item.get("episode_range", ""),
                    "resolutions": {
                        k: f"{len(v)} file(s)"
                        for k, v in (item.get("resolutions") or {}).items()
                        if isinstance(v, list)
                    },
                })
            if items:
                seasons_summary.append({"season_number": snum, "download_items": items})
        if seasons_summary:
            return {"seasons": seasons_summary}

    # Fallback: candidate format from build_db_candidate (tv_items = flat list)
    tv_items = db_candidate.get("tv_items")
    if isinstance(tv_items, list) and tv_items:
        seasons_map: dict[int, list] = {}
        for ti in tv_items:
            if not isinstance(ti, dict):
                continue
            snum = ti.get("season_number")
            res = ti.get("resolutions", [])
            item_entry = {
                "type": ti.get("type", ""),
                "episode_range": ti.get("episode_range", ""),
                "resolutions": {r: ["present"] for r in (res if isinstance(res, list) else [])},
            }
            seasons_map.setdefault(snum, []).append(item_entry)
        return {
            "seasons": [
                {"season_number": sn, "download_items": items}
                for sn, items in sorted(seasons_map.items(), key=lambda x: x[0] or 0)
            ]
        }

    return {"seasons": []}


def _build_extracted_summary(content_type: str, extracted_data: dict) -> dict:
    """Keep the full extracted data for the LLM to compare against existing."""
    if content_type == "movie":
        return {"download_links": extracted_data.get("download_links", {})}
    return {"seasons": extracted_data.get("seasons", [])}


def _build_user_prompt(
    content_type: str,
    existing_summary: dict,
    extracted_data: dict,
    update_details: dict | None = None,
) -> str:
    extracted_summary = _build_extracted_summary(content_type, extracted_data)
    parts = [
        f"EXISTING (what the DB already has):\n"
        f"```json\n{json.dumps(existing_summary, **_COMPACT, ensure_ascii=False)}\n```",
    ]
    if isinstance(update_details, dict):
        parts.append(
            f"\nUPDATE HINT (from analysis step — what is expected to be missing):\n"
            f"```json\n{json.dumps(update_details, **_COMPACT, ensure_ascii=False)}\n```"
        )
    parts.append(
        f"\nEXTRACTED (full data from page):\n"
        f"```json\n{json.dumps(extracted_summary, **_COMPACT, ensure_ascii=False)}\n```\n\n"
        f"Return ONLY the delta — items/resolutions in EXTRACTED but NOT in EXISTING."
    )
    return "\n".join(parts)


def _parse_delta_response(raw: str) -> dict:
    return repair_json(raw)


def compute_update_delta(
    content_type: str,
    extracted_data: dict,
    db_candidate: dict,
    *,
    update_details: dict | None = None,
    dup_search_context: dict | None = None,
) -> dict | None:
    """Run Pass-2 LLM call to filter extracted_data down to only the missing delta.

    Args:
        content_type: "movie" or "tvshow"
        extracted_data: Full data from Pass-1 extraction.
        db_candidate: The matched DB candidate dict (must contain download_links or seasons).
        update_details: Optional structured breakdown from Pass-1 duplicate_check
                        describing exactly what needs updating.
        dup_search_context: Optional extra context (DB candidates list + FlixBD results)
                            passed alongside for richer existing coverage.

    Returns:
        The delta dict (with download_links or seasons containing only missing parts),
        or None if the LLM call fails (caller should fall back to full data).
    """
    try:
        existing_summary = _build_existing_summary(content_type, db_candidate)

        is_empty_existing = (
            (content_type == "movie" and not existing_summary.get("download_links"))
            or (content_type == "tvshow" and not existing_summary.get("seasons"))
        )
        if is_empty_existing:
            logger.warning("Pass-2 skipped: existing candidate has no coverage data to compare against.")
            return None

        system_prompt = get_update_system_prompt(content_type)
        user_prompt = _build_user_prompt(content_type, existing_summary, extracted_data, update_details)

        logger.info(
            "Pass-2 delta filter: content_type=%s, existing_keys=%s",
            content_type,
            list(existing_summary.keys()),
        )

        raw_response = LLMService.generate_completion(
            prompt=user_prompt,
            system_prompt=system_prompt,
            purpose="update_delta",
        )

        result = None
        last_err = None
        current = raw_response
        for attempt in range(_JSON_RETRY_MAX + 1):
            try:
                result = _parse_delta_response(current)
                break
            except Exception as e:
                last_err = e
                if attempt < _JSON_RETRY_MAX:
                    logger.warning("Pass-2 JSON parse failed (attempt %d): %s. Retrying.", attempt + 1, e)
                    current = LLMService.generate_completion(
                        prompt=user_prompt + _JSON_RETRY_SUFFIX,
                        system_prompt=system_prompt,
                        purpose="update_delta",
                    )

        if result is None:
            logger.error("Pass-2 delta filter: all JSON parse attempts failed: %s", last_err)
            return None

        delta = result.get("delta", result)
        if not isinstance(delta, dict):
            logger.error("Pass-2 delta filter: 'delta' is not a dict: %s", type(delta))
            return None

        if content_type == "movie":
            links = delta.get("download_links", {})
            if not isinstance(links, dict) or not links:
                logger.info("Pass-2 result: no missing movie resolutions (or empty delta).")
                return {"download_links": {}}
            logger.info("Pass-2 result: %d missing movie resolution(s): %s", len(links), list(links.keys()))
            return {"download_links": links}

        seasons = delta.get("seasons", [])
        if not isinstance(seasons, list):
            seasons = []
        item_count = sum(len(s.get("download_items", [])) for s in seasons if isinstance(s, dict))
        logger.info("Pass-2 result: %d season(s), %d download item(s) in delta.", len(seasons), item_count)
        return {"seasons": seasons}

    except Exception as e:
        logger.error("Pass-2 delta filter failed: %s", e, exc_info=True)
        return None
