"""
Pass-2: Delta-only update filtering.

Called when Pass-1 returns action="update".
Sends Pass-1 data + search context → LLM returns filtered data (only missing
downloads) and action ("update" or "skip").
"""

from __future__ import annotations

import json
import logging
from typing import Any

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


def compute_update_delta(
    content_type: str,
    pass1_data: dict,
    dup_search_context: dict,
    *,
    persist_usage: bool = True,
    capture_usage_events: list[dict[str, Any]] | None = None,
) -> dict | None:
    """Run Pass-2 LLM call.

    Args:
        content_type: "movie" or "tvshow"
        pass1_data: The `data` dict from Pass-1 (seasons/download_links + metadata).
        dup_search_context: Search context (db_match_candidates + flixbd_results).

    Returns:
        {"action": "update"|"skip", "data": {...}} or None on failure.
    """
    try:
        system_prompt = get_update_system_prompt(content_type)

        user_prompt = (
            f"PASS-1 DATA:\n"
            f"```json\n{json.dumps(pass1_data, **_COMPACT, ensure_ascii=False)}\n```\n\n"
            f"SEARCH CONTEXT (what already exists):\n"
            f"```json\n{json.dumps(dup_search_context, **_COMPACT, ensure_ascii=False)}\n```\n\n"
            f"Return filtered data with only missing downloads. Set action to skip if nothing is missing."
        )

        logger.info("Pass-2 delta filter: content_type=%s", content_type)

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
                result = repair_json(current)
                break
            except Exception as e:
                last_err = e
                if attempt < _JSON_RETRY_MAX:
                    logger.warning("Pass-2 JSON parse failed (attempt %d): %s. Retrying.", attempt + 1, e)
                    current = LLMService.generate_completion(
                        prompt=user_prompt + _JSON_RETRY_SUFFIX,
                        system_prompt=system_prompt,
                        purpose="update_delta",
                        persist_usage=persist_usage,
                        capture_usage_events=capture_usage_events,
                    )

        if result is None:
            logger.error("Pass-2: all JSON parse attempts failed: %s", last_err)
            return None

        if not isinstance(result, dict):
            logger.error("Pass-2: result is not a dict: %s", type(result))
            return None

        action = result.get("action", "update")
        reason = result.get("reason", "")
        data = result.get("data", result)

        if action == "skip":
            logger.info("Pass-2 decided: action=skip, reason=%s", reason)
            return {"action": "skip", "reason": reason, "data": data if isinstance(data, dict) else {}}

        if not isinstance(data, dict):
            logger.error("Pass-2: 'data' is not a dict: %s", type(data))
            return None

        if content_type == "movie":
            links = data.get("download_links", {})
            count = len(links) if isinstance(links, dict) else 0
            logger.info("Pass-2 result: %d movie resolution(s), reason=%s", count, reason)
        else:
            seasons = data.get("seasons", [])
            items = sum(len(s.get("download_items", [])) for s in seasons if isinstance(s, dict))
            logger.info("Pass-2 result: %d season(s), %d item(s), reason=%s", len(seasons), items, reason)

        return {"action": "update", "reason": reason, "data": data}

    except Exception as e:
        logger.error("Pass-2 delta filter failed: %s", e, exc_info=True)
        return None
