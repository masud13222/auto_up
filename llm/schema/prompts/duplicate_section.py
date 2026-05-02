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


def build_duplicate_section(
    *,
    locked_content_type: Literal["movie", "tvshow"],
    db_match_candidates: list | None,
    flixbd_results: list | None,
) -> str:
    if not db_match_candidates and not flixbd_results:
        return ""

    site = SITE_NAME
    rk = TARGET_SITE_ROW_ID_JSON_KEY
    ctx_parts: list[str] = []

    if db_match_candidates:
        ctx_parts.append(
            f"### DB Candidates ({len(db_match_candidates)}):\n```json\n"
            f"{json_compact(db_match_candidates)}\n```"
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
