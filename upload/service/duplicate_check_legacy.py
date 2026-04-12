"""
Legacy standalone duplicate check: title fetch + DB search + **second** LLM call
using ``DUPLICATE_CHECK_PROMPT`` (``llm.schema.duplicate_schema``).

**Not used by the main upload pipeline.** Production uses a single combined LLM call
(``get_combined_system_prompt`` + ``detect_and_extract``) for extract + duplicate_check.

Import ``check_duplicate`` from here only if you need this two-step flow (e.g. scripts, experiments).
"""

from __future__ import annotations

import json
import logging

from llm.json_repair import repair_json
from llm.schema.blocked_names import TARGET_SITE_ROW_ID_JSON_KEY
from llm.schema.duplicate_schema import DUPLICATE_CHECK_PROMPT
from llm.services import LLMService
from llm.utils.name_extractor import extract_title_info
from upload.models import MediaTask
from upload.utils.web_scrape import WebScrapeService

from .duplicate_checker import _get_existing_resolutions, _search_db, coerce_matched_task_pk

logger = logging.getLogger(__name__)


def check_duplicate(url: str, current_task_pk: int = None) -> dict:
    """
    Full duplicate detection pipeline (legacy):
    1. Fetch page title (no LLM)
    2. Extract clean name + year
    3. Search DB for matches
    4. If match found, LLM compares (separate call from main extract)

    Returns:
        {
            "action": "skip" | "process" | "replace" | "replace_items" | "update",
            "reason": "...",
            "existing_task": MediaTask or None,
            "extracted_name": "...",
            "extracted_year": "..." or None,
            "website_title": "..."
        }
    """
    website_title = WebScrapeService.cinefreak_title(url)
    if not website_title:
        logger.info("Could not fetch title for %s, proceeding as new", url)
        return {
            "action": "process",
            "reason": "Could not fetch page title",
            "existing_task": None,
            "extracted_name": None,
            "extracted_year": None,
            "website_title": None,
        }

    logger.info("Website title: %s", website_title)

    info = extract_title_info(website_title)
    name = info.title
    year = info.year
    logger.info("Extracted: name=%r, year=%r", name, year)

    if not name:
        return {
            "action": "process",
            "reason": "Could not extract title name",
            "existing_task": None,
            "extracted_name": None,
            "extracted_year": year,
            "website_title": website_title,
        }

    matches = _search_db(name, year, exclude_pk=current_task_pk)

    if not matches:
        logger.info("No existing match found for %r (%r). New content.", name, year)
        return {
            "action": "process",
            "reason": f"No existing entry found for '{name}'",
            "existing_task": None,
            "extracted_name": name,
            "extracted_year": year,
            "website_title": website_title,
        }

    logger.info(
        "Found %d candidate(s): %s",
        len(matches),
        ", ".join(f"[{t.pk}] {t.title}" for t in matches),
    )

    result = _llm_compare(matches, name, year, website_title)

    raw_matched = result.get("matched_task_id")
    matched_pk = coerce_matched_task_pk(raw_matched)
    if raw_matched is not None and matched_pk is None:
        logger.warning("Invalid matched_task_id=%r; treating as process", raw_matched)
        result["matched_task_id"] = None
        result["action"] = "process"
    elif matched_pk is not None:
        result["matched_task_id"] = matched_pk

    existing_task = None
    if matched_pk is not None:
        candidate_map = {t.pk: t for t in matches}
        existing_task = candidate_map.get(matched_pk)
        if not existing_task:
            logger.warning(
                "LLM returned matched_task_id=%s not in candidates %s; treating as process",
                matched_pk,
                list(candidate_map.keys()),
            )
            result["action"] = "process"
    else:
        result["matched_task_id"] = None

    if result["action"] in ("update", "replace") and existing_task is None:
        logger.warning(
            "PipelineWarning: duplicate_check_legacy action=%s but no existing_task — "
            "full process only; clearing missing_resolutions",
            result["action"],
        )
        result["action"] = "process"
        result["missing_resolutions"] = []
    elif result["action"] == "skip" and existing_task is None:
        logger.warning("duplicate_check_legacy: skip without resolved existing_task — forcing process")
        result["action"] = "process"

    result["existing_task"] = existing_task if result["action"] != "process" else None
    result["extracted_name"] = name
    result["extracted_year"] = year
    result["website_title"] = website_title

    logger.info("Duplicate check result: action=%s, reason=%s", result["action"], result["reason"])
    return result


def _llm_compare(matches: list, new_name: str, new_year: str, new_website_title: str) -> dict:
    """Use LLM to compare new vs multiple existing candidates (DUPLICATE_CHECK_PROMPT)."""
    candidates = []
    for task in matches:
        result_data = task.result or {}
        is_tvshow = task.content_type == "tvshow" if task.content_type else bool(result_data.get("seasons"))
        resolutions = (
            _get_existing_resolutions(task) if not is_tvshow else []
        )

        candidate = {
            "id": task.pk,
            "title": task.title,
            "year": result_data.get("year"),
            "type": "tvshow" if is_tvshow else "movie",
        }
        if not is_tvshow:
            candidate["resolutions"] = resolutions

        if is_tvshow:
            episodes = []
            tv_items = []
            for season in result_data.get("seasons", []):
                season_num = season.get("season_number")
                for item in season.get("download_items", []):
                    label = item.get("label", "")
                    item_type = item.get("type")
                    episode_range = item.get("episode_range")
                    res = item.get("resolutions", {})
                    ep_res = sorted(k for k, v in res.items() if v)
                    episodes.append(
                        f"S{season_num} {item_type} {episode_range or '-'} {label}: {','.join(ep_res)}"
                    )
                    tv_items.append(
                        {
                            "season_number": season_num,
                            "type": item_type,
                            "episode_range": episode_range,
                            "label": label,
                            "resolutions": ep_res,
                        }
                    )
            candidate["episode_count"] = len(tv_items)
            candidate["episodes"] = episodes
            candidate["tv_items"] = tv_items

        candidates.append(candidate)

    comparison_data = json.dumps(
        {
            "new_website_title": new_website_title,
            "new_name": new_name,
            "new_year": new_year,
            "candidates": candidates,
        },
        ensure_ascii=False,
    )

    logger.info(
        "LLM comparing: %r (%s) vs %d candidate(s)",
        new_name,
        new_year,
        len(candidates),
    )

    try:
        raw = LLMService.generate_completion(
            prompt=comparison_data,
            system_prompt=DUPLICATE_CHECK_PROMPT,
            purpose="duplicate_check",
        )
        result = repair_json(raw)

        action = result.get("action", "process")
        reason = result.get("reason", "LLM decision")
        matched_task_id = result.get("matched_task_id")
        detected_new_type = result.get("detected_new_type", "movie")
        missing_resolutions = result.get("missing_resolutions", [])
        has_new_episodes = result.get("has_new_episodes", False)

        if action not in ("skip", "update", "replace", "replace_items", "process"):
            action = "process"
            reason = f"Invalid LLM action, defaulting to process: {result}"

        return {
            "action": action,
            "reason": reason,
            "matched_task_id": matched_task_id,
            TARGET_SITE_ROW_ID_JSON_KEY: result.get(TARGET_SITE_ROW_ID_JSON_KEY)
            or result.get("flixbd_task_id"),
            "detected_new_type": detected_new_type,
            "missing_resolutions": missing_resolutions,
            "has_new_episodes": has_new_episodes,
        }

    except Exception as e:
        logger.warning("LLM comparison failed, defaulting to process: %s", e)
        return {
            "action": "process",
            "reason": f"LLM comparison error: {e}",
            "matched_task_id": None,
            TARGET_SITE_ROW_ID_JSON_KEY: None,
            "detected_new_type": "movie",
            "missing_resolutions": [],
            "has_new_episodes": False,
        }
