"""
Public entrypoint for the combined extract + optional duplicate-check system prompt.

Concrete prompt text is built in ``llm.schema.prompts``; this module only wires pieces
by content type and duplicate context.
"""

from __future__ import annotations

from typing import Literal

from .prompts.combined_movie import build_combined_movie_extract_body
from .prompts.combined_tv import build_combined_tv_extract_body
from .prompts.duplicate_section import build_duplicate_section
from .prompts.shared import build_resolution_note, core_rules_block_shared, seo_block_shared


def get_combined_system_prompt(
    locked_content_type: Literal["movie", "tvshow"],
    extra_below: bool = False,
    extra_above: bool = False,
    max_extra: int = 0,
    db_match_candidates: list | None = None,
    flixbd_results: list | None = None,
) -> str:
    res_note = build_resolution_note(extra_below, extra_above, max_extra)
    has_dup = bool(db_match_candidates or flixbd_results)
    dup_section = (
        build_duplicate_section(
            locked_content_type=locked_content_type,
            db_match_candidates=db_match_candidates,
            flixbd_results=flixbd_results,
        )
        if has_dup
        else ""
    )

    core = core_rules_block_shared()
    seo = seo_block_shared()

    if locked_content_type == "movie":
        body = build_combined_movie_extract_body(core, seo, res_note)
        output_line = (
            '{"content_type":"movie","data":{...},"duplicate_check":{...}}'
            if has_dup
            else '{"content_type":"movie","data":{...}}'
        )
        intro = "You are a structured data extraction function. Extract **movie** metadata and download links. Return ONLY valid JSON. `content_type` must be exactly `\"movie\"`."
    else:
        body = build_combined_tv_extract_body(core, seo, res_note)
        output_line = (
            '{"content_type":"tvshow","data":{...},"duplicate_check":{...}}'
            if has_dup
            else '{"content_type":"tvshow","data":{...}}'
        )
        intro = "You are a structured data extraction function. Extract **TV show** metadata and download links. Return ONLY valid JSON. `content_type` must be exactly `\"tvshow\"`."

    return f"""{intro}

{body}
{dup_section}
## OUTPUT:
{output_line}
Return ONLY the JSON."""


COMBINED_SYSTEM_PROMPT = get_combined_system_prompt(locked_content_type="movie")
