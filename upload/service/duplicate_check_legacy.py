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
from llm.schema.duplicate_schema import DUPLICATE_CHECK_PROMPT
from llm.services import LLMService
from llm.utils.name_extractor import extract_title_info
from upload.models import MediaTask
from upload.utils.web_scrape import WebScrapeService

from .duplicate_checker import _get_existing_resolutions, _search_db

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
            "action": "skip" | "process" | "replace" | "update",
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

    existing_task = matches[0]
    logger.info("Found existing match: [%s] %s", existing_task.pk, existing_task.title)

    result = _llm_compare(existing_task, name, year, website_title)
    result["existing_task"] = existing_task if result["action"] != "process" else None
    result["extracted_name"] = name
    result["extracted_year"] = year
    result["website_title"] = website_title

    logger.info("Duplicate check result: action=%s, reason=%s", result["action"], result["reason"])
    return result


def _llm_compare(existing_task: MediaTask, new_name: str, new_year: str, new_website_title: str) -> dict:
    """Use LLM to compare new vs existing content (DUPLICATE_CHECK_PROMPT)."""
    result_data = existing_task.result or {}
    existing_resolutions = _get_existing_resolutions(existing_task)

    if existing_task.content_type:
        is_tvshow = existing_task.content_type == "tvshow"
    else:
        is_tvshow = bool(result_data.get("seasons"))

    episode_count = 0
    episodes = []
    if is_tvshow:
        for season in result_data.get("seasons", []):
            for item in season.get("download_items", []):
                episode_count += 1
                label = item.get("label", "")
                res = item.get("resolutions", {})
                ep_res = sorted(k for k, v in res.items() if v)
                episodes.append(f"{label}: {','.join(ep_res)}")

    comparison_data = json.dumps(
        {
            "new_website_title": new_website_title,
            "new_name": new_name,
            "new_year": new_year,
            "existing_title": existing_task.title,
            "existing_resolutions": existing_resolutions,
            "existing_type": "tvshow" if is_tvshow else "movie",
            "existing_episode_count": episode_count,
            "existing_episodes": episodes,
        },
        ensure_ascii=False,
    )

    logger.info(
        "LLM comparing: %r vs existing %r (type=%s, res=%s, episodes=%s)",
        new_name,
        existing_task.title,
        "tvshow" if is_tvshow else "movie",
        existing_resolutions,
        episode_count,
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
        detected_new_type = result.get("detected_new_type", "movie")
        missing_resolutions = result.get("missing_resolutions", [])
        has_new_episodes = result.get("has_new_episodes", False)

        if action not in ("skip", "update", "replace", "process"):
            action = "process"
            reason = f"Invalid LLM action, defaulting to process: {result}"

        return {
            "action": action,
            "reason": reason,
            "detected_new_type": detected_new_type,
            "missing_resolutions": missing_resolutions,
            "has_new_episodes": has_new_episodes,
        }

    except Exception as e:
        logger.warning("LLM comparison failed, defaulting to process: %s", e)
        return {
            "action": "process",
            "reason": f"LLM comparison error: {e}",
            "detected_new_type": "movie",
            "missing_resolutions": [],
            "has_new_episodes": False,
        }
