"""
LLM-based filtering for auto-upload.

Sends scraped items + DB search results (with full resolution/episode info)
to the LLM, which decides what should be processed and what should be skipped.
"""

import json
import logging

from llm.services import LLMService
from llm.json_repair import repair_json
from upload.service.info import _save_duplicate_usage_snapshot_to_latest_usage
from auto_up.schema import AUTO_FILTER_SYSTEM_PROMPT

logger = logging.getLogger(__name__)


def _build_db_result_entry(r: dict) -> dict:
    """
    Build a single DB result entry for LLM payload.
    Includes all rich info: website_title, resolutions, episodes, etc.
    """
    entry = {
        "task_pk": r["task_pk"],
        "matched_by": r.get("matched_by", []),
        "title": r["title"],
        "status": r["status"],
        "content_type": r["content_type"],
        "url": r["url"],
    }

    # Rich fields — only include if present
    if r.get("website_title"):
        entry["website_title"] = r["website_title"]
    if r.get("year"):
        entry["year"] = r["year"]

    # Movie resolutions
    if r.get("resolutions"):
        entry["resolutions"] = r["resolutions"]

    # TV Show episode-level detail
    if r.get("season_numbers"):
        entry["season_numbers"] = r["season_numbers"]
    if r.get("total_episodes"):
        entry["total_episodes"] = r["total_episodes"]
    if r.get("episodes"):
        entry["episodes"] = r["episodes"]

    return entry


def filter_items_with_llm(items: list[dict]) -> list[dict]:
    """
    Send all scraped items (with their DB search results) to the LLM
    for filtering decisions.

    Args:
        items: List of dicts, each containing:
            - raw_title: str
            - clean_name: str
            - year: str or None
            - season_tag: str or None
            - url: str
            - db_results: dict from db_search.search_existing()
                          (now includes rich info: website_title, resolutions,
                           episode_count, episode_labels, season_numbers)

    Returns:
        List of items that should be processed, each with:
            - url: str
            - raw_title: str
            - action: "process"
            - reason: str
            - priority: str
    """
    if not items:
        logger.info("No items to filter")
        return []

    # Build the prompt payload with full rich data
    payload = []
    for item in items:
        db_results = item.get("db_results", {})
        flixbd_results = item.get("flixbd_results", [])

        entry = {
            "raw_title": item["raw_title"],
            "clean_name": item["clean_name"],
            "year": item.get("year"),
            "season_tag": item.get("season_tag"),
            "url": item["url"],
            "db_results": {
                "results": [
                    _build_db_result_entry(r)
                    for r in db_results.get("results", [])
                ],
                "has_matches": db_results.get("has_matches", False),
            },
        }

        # Only include flixbd_results if FlixBD returned something
        if flixbd_results:
            entry["flixbd_results"] = flixbd_results

        payload.append(entry)

    prompt = json.dumps(payload, ensure_ascii=False)

    logger.info(f"Sending {len(payload)} items to LLM for filtering...")

    try:
        raw_response = LLMService.generate_completion(
            prompt=prompt,
            system_prompt=AUTO_FILTER_SYSTEM_PROMPT,
            purpose='auto_filter',
        )

        result = repair_json(raw_response)
        decisions = result.get("decisions", [])
        if not isinstance(decisions, list):
            logger.warning(
                "auto_filter: expected decisions to be a list, got %s; using empty list",
                type(decisions).__name__,
            )
            decisions = []

        _save_duplicate_usage_snapshot_to_latest_usage(
            dup_result={"decisions": decisions},
            db_match_candidates=None,
            flixbd_results=None,
            purpose="auto_filter",
            response_text=raw_response,
            extra_context={"auto_filter_items": payload} if payload else None,
        )

        logger.info(f"LLM returned {len(decisions)} decisions")

        # Build a URL→item lookup for enriching results
        url_to_item = {item["url"]: item for item in items}

        # Collect items that should be processed
        to_process = []
        skipped = 0

        for decision in decisions:
            action = decision.get("action", "process")
            url = decision.get("url", "")
            reason = decision.get("reason", "")
            priority = decision.get("priority", "normal")

            if action == "process":
                original_item = url_to_item.get(url, {})
                to_process.append({
                    "url": url,
                    "raw_title": original_item.get("raw_title", ""),
                    "action": "process",
                    "reason": reason,
                    "priority": priority,
                })
                logger.info(f"  → PROCESS [{priority}]: {original_item.get('raw_title', url)[:60]} — {reason}")
            else:
                skipped += 1
                logger.info(f"  → SKIP: {url_to_item.get(url, {}).get('raw_title', url)[:60]} — {reason}")

        logger.info(
            f"Filter results: {len(to_process)} to process, {skipped} skipped"
        )
        return to_process

    except Exception as e:
        logger.error(f"LLM filtering failed: {e}", exc_info=True)

        # Fallback: process everything (better safe than sorry)
        logger.warning("Falling back to processing ALL items due to LLM failure")
        return [
            {
                "url": item["url"],
                "raw_title": item["raw_title"],
                "action": "process",
                "reason": "LLM filter failed, defaulting to process",
                "priority": "normal",
            }
            for item in items
        ]
