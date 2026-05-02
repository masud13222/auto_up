from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from llm.json_repair import repair_json
from llm.schema.presearch_schema import build_presearch_user_prompt, get_presearch_system_prompt
from llm.services import LLMService
from llm.utils.name_extractor import TitleInfo

PRESEARCH_MARKDOWN_MAX = 3500


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


def _empty_to_none(s: str | None) -> str | None:
    if s is None:
        return None
    t = str(s).strip()
    return t or None


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

    raw = LLMService.generate_completion(
        prompt=user_prompt,
        system_prompt=system_prompt,
        temperature=0.05,
        purpose="presearch_extract",
        persist_usage=persist_usage,
        capture_usage_events=capture_usage_events,
    )
    if debug_capture is not None:
        debug_capture["system_prompt"] = system_prompt
        debug_capture["user_prompt"] = user_prompt
        debug_capture["raw_response"] = raw
    data = repair_json(raw)
    if not isinstance(data, dict):
        raise ValueError("presearch response is not an object")

    raw_ct = (data.get("content_type") or "").strip().lower()
    if raw_ct not in ("movie", "tvshow"):
        raise ValueError(f"presearch content_type must be movie or tvshow, got {raw_ct!r}")
    ct = raw_ct

    name = (data.get("name") or "").strip()
    if not name:
        raise ValueError("presearch missing name")

    alt = _empty_to_none(data.get("alt_name"))
    year = _empty_to_none(data.get("year"))
    st = _empty_to_none(data.get("season_tag"))
    if ct == "movie":
        st = None

    return SearchExtractResult(
        content_type=ct,
        primary_name=name,
        alt_name=alt,
        year=year,
        season_tag=st,
        raw_snippet=snippet,
    )
