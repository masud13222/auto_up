"""
Pass-2: Delta-only update filtering.

Called when Pass-1 returns action="update".
Sends the Pass-1 response + search context to a focused LLM prompt.
LLM returns the same structure as Pass-1 but with only the missing/new data.
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


def compute_update_delta(
    content_type: str,
    pass1_data: dict,
    dup_search_context: dict,
) -> dict | None:
    """Run Pass-2 LLM call: take Pass-1 data + search context,
    return only the delta (missing parts).

    Args:
        content_type: "movie" or "tvshow"
        pass1_data: Full data dict from Pass-1 extraction.
        dup_search_context: The search context (db_match_candidates +
                            flixbd_results + update_details).

    Returns:
        The delta data dict (same structure as pass1_data but only missing parts),
        or None if the LLM call fails (caller should fall back to full data).
    """
    try:
        system_prompt = get_update_system_prompt(content_type)

        user_prompt = (
            f"PASS-1 RESPONSE (full extracted data):\n"
            f"```json\n{json.dumps(pass1_data, **_COMPACT, ensure_ascii=False)}\n```\n\n"
            f"SEARCH CONTEXT (what already exists):\n"
            f"```json\n{json.dumps(dup_search_context, **_COMPACT, ensure_ascii=False)}\n```\n\n"
            f"Return ONLY the delta — items/resolutions in PASS-1 RESPONSE but NOT in SEARCH CONTEXT."
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
                    )

        if result is None:
            logger.error("Pass-2 delta filter: all JSON parse attempts failed: %s", last_err)
            return None

        data = result.get("data", result)
        if not isinstance(data, dict):
            logger.error("Pass-2 delta filter: 'data' is not a dict: %s", type(data))
            return None

        if content_type == "movie":
            links = data.get("download_links", {})
            if not isinstance(links, dict) or not links:
                logger.info("Pass-2 result: empty movie delta.")
                return {"download_links": {}}
            logger.info("Pass-2 result: %d missing movie resolution(s): %s", len(links), list(links.keys()))
            return {"download_links": links}

        seasons = data.get("seasons", [])
        if not isinstance(seasons, list):
            seasons = []
        item_count = sum(len(s.get("download_items", [])) for s in seasons if isinstance(s, dict))
        logger.info("Pass-2 result: %d season(s), %d download item(s) in delta.", len(seasons), item_count)
        return {"seasons": seasons}

    except Exception as e:
        logger.error("Pass-2 delta filter failed: %s", e, exc_info=True)
        return None
