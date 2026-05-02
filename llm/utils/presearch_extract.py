from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from django.utils import timezone

from llm.json_repair import repair_json
from llm.schema.presearch_models import PresearchOutput
from llm.schema.presearch_schema import build_presearch_user_prompt, get_presearch_system_prompt
from llm.schema.response_validate import (
    LLM_SCHEMA_RETRY_MAX,
    VALIDATION_RETRY_SUFFIX,
    format_validation_detail,
)
from llm.services import LLMService
from llm.utils.name_extractor import TitleInfo

logger = logging.getLogger(__name__)

PRESEARCH_MARKDOWN_MAX = 3000


@dataclass
class SearchExtractResult:
    content_type: str
    primary_name: str
    alt_name: str | None
    year: str | None
    season_tag: str | None
    raw_snippet: str

    def as_title_info(self) -> TitleInfo:
        return TitleInfo(
            title=self.primary_name,
            year=self.year,
            season_tag=self.season_tag,
            content_tag=None,
            raw=self.raw_snippet,
        )


def _persist_presearch_parsed_on_latest_usage(
    result: SearchExtractResult,
    *,
    markdown_input_chars: int,
) -> None:
    """Attach repaired presearch fields to the LLMUsage row created by the same call."""
    try:
        from llm.models import LLMUsage

        cutoff = timezone.now() - timedelta(seconds=90)
        row = (
            LLMUsage.objects.filter(
                purpose="presearch_extract",
                created_at__gte=cutoff,
                success=True,
            )
            .order_by("-pk")
            .first()
        )
        if not row:
            logger.warning("presearch_extract: no recent LLMUsage row to attach parsed result")
            return
        payload = {
            "content_type": result.content_type,
            "primary_name": result.primary_name,
            "alt_name": result.alt_name,
            "year": result.year,
            "season_tag": result.season_tag,
            "markdown_input_chars": markdown_input_chars,
        }
        row.presearch_result_json = json.dumps(payload, indent=2, ensure_ascii=False)
        row.save(update_fields=["presearch_result_json"])
    except Exception as e:
        logger.warning("presearch_extract: could not save parsed result on LLMUsage: %s", e)


def _empty_to_none(s: str | None) -> str | None:
    if s is None:
        return None
    t = str(s).strip()
    return t or None


def _parse_and_validate_presearch(raw: str) -> PresearchOutput:
    """Repair JSON syntax, then enforce schema with Pydantic (matches JSON Schema in prompt)."""
    data = repair_json(raw)
    if not isinstance(data, dict):
        raise ValueError("presearch response is not a JSON object")
    return PresearchOutput.model_validate(data)


def extract_presearch_from_markdown(
    markdown_snippet: str,
    *,
    persist_usage: bool = True,
    debug_capture: dict[str, Any] | None = None,
    capture_usage_events: list[dict[str, Any]] | None = None,
) -> SearchExtractResult:
    snippet = (markdown_snippet or "")[:PRESEARCH_MARKDOWN_MAX]
    if not snippet.strip():
        raise ValueError("empty markdown snippet")

    system_prompt = get_presearch_system_prompt()
    user_prompt = build_presearch_user_prompt(snippet)

    if debug_capture is not None:
        debug_capture["system_prompt"] = system_prompt
        debug_capture["user_prompt"] = user_prompt

    validated: PresearchOutput | None = None
    last_error: Exception | None = None
    current_raw = ""

    for attempt in range(LLM_SCHEMA_RETRY_MAX + 1):
        if attempt == 0:
            current_raw = LLMService.generate_completion(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=0.05,
                purpose="presearch_extract",
                persist_usage=persist_usage,
                capture_usage_events=capture_usage_events,
            )
        else:
            assert last_error is not None
            fix_prompt = user_prompt + VALIDATION_RETRY_SUFFIX.format(
                detail=format_validation_detail(last_error)
            )
            logger.warning(
                "presearch_extract: parse/validation failed (attempt %s/%s): %s — retrying LLM",
                attempt,
                LLM_SCHEMA_RETRY_MAX + 1,
                last_error,
            )
            current_raw = LLMService.generate_completion(
                prompt=fix_prompt,
                system_prompt=system_prompt,
                temperature=0.05,
                purpose="presearch_extract",
                persist_usage=persist_usage,
                capture_usage_events=capture_usage_events,
            )

        if debug_capture is not None:
            debug_capture["raw_response"] = current_raw
            debug_capture["validation_attempt"] = attempt + 1

        try:
            validated = _parse_and_validate_presearch(current_raw)
            break
        except Exception as e:
            last_error = e
            if attempt >= LLM_SCHEMA_RETRY_MAX:
                raise

    if validated is None:
        raise last_error or RuntimeError("presearch_extract: no validated output")

    alt = _empty_to_none(validated.alt_name)
    year = _empty_to_none(validated.year)
    st = _empty_to_none(validated.season_tag)
    ct = validated.content_type
    if ct == "movie":
        st = None

    result = SearchExtractResult(
        content_type=ct,
        primary_name=validated.name,
        alt_name=alt,
        year=year,
        season_tag=st,
        raw_snippet=snippet,
    )
    if persist_usage:
        _persist_presearch_parsed_on_latest_usage(
            result,
            markdown_input_chars=len(snippet),
        )
    return result
