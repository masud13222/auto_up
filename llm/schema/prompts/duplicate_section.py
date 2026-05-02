"""Duplicate-check Markdown block (DB + FlixBD context + schema + typed examples)."""

from __future__ import annotations

from typing import Literal

from ..blocked_names import SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY
from ..duplicate_schema import duplicate_schema
from ..json_encoding import json_compact

from .combined_movie import (
    build_combined_movie_duplicate_examples,
    build_combined_movie_duplicate_pre_schema,
)
from .combined_tv import (
    build_combined_tv_duplicate_examples,
    build_combined_tv_duplicate_pre_schema,
)


_LLM_STRIP_DB_CANDIDATE_KEYS = frozenset({"episodes", "resolutions", "episode_count"})


def db_candidates_for_llm_prompt(candidates: list | None) -> list[dict]:
    """Drop verbose DB candidate keys for LLM prompts only. Full rows stay in duplicate_context_json."""
    if not candidates:
        return []
    return [
        {k: v for k, v in row.items() if k not in _LLM_STRIP_DB_CANDIDATE_KEYS}
        for row in candidates
        if isinstance(row, dict)
    ]


def build_duplicate_section(
    *,
    locked_content_type: Literal["movie", "tvshow"],
    db_match_candidates: list | None,
    flixbd_results: list | None,
) -> str:
    db_for_prompt = db_candidates_for_llm_prompt(db_match_candidates)
    if not db_for_prompt and not flixbd_results:
        return ""

    site = SITE_NAME
    rk = TARGET_SITE_ROW_ID_JSON_KEY
    ctx_parts: list[str] = []

    if db_for_prompt:
        ctx_parts.append(
            f"### DB Candidates ({len(db_for_prompt)}):\n```json\n"
            f"{json_compact(db_for_prompt)}\n```"
        )
    if flixbd_results:
        ctx_parts.append(
            f"### {site} search results (top {len(flixbd_results)}):\n"
            f"```json\n{json_compact(flixbd_results)}\n```\n"
            f"(Row `id` → use as `{rk}`, never as `matched_task_id`.)"
        )

    dup_json = json_compact(duplicate_schema)

    if locked_content_type == "movie":
        pre = build_combined_movie_duplicate_pre_schema(site, rk)
        ex = build_combined_movie_duplicate_examples(site, rk)
    else:
        pre = build_combined_tv_duplicate_pre_schema(site, rk)
        ex = build_combined_tv_duplicate_examples(site, rk)

    return f"""
---
## DUPLICATE CHECK

{chr(10).join(ctx_parts)}
{pre}
```json
{dup_json}
```

{ex}

"""
