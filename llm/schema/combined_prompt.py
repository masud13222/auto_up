import json
from typing import Literal

from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY
from .duplicate_schema import duplicate_schema
from .movie_schema import (
    build_combined_movie_duplicate_examples,
    build_combined_movie_duplicate_pre_schema,
    build_combined_movie_extract_body,
)
from .tvshow_schema import (
    build_combined_tv_duplicate_examples,
    build_combined_tv_duplicate_pre_schema,
    build_combined_tv_extract_body,
)

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)
_COMPACT = {"separators": (",", ":")}


def _build_resolution_note(extra_below: bool = False, extra_above: bool = False, max_extra: int = 0) -> str:
    parts = ["Base: 480p, 720p, 1080p always included when present."]
    if extra_below:
        parts.append("Below 720p: ON (include 520p, 360p, 240p etc).")
    else:
        parts.append("Below 720p: OFF.")
    if extra_above:
        parts.append("Above 1080p: ON (include 2160p/4K).")
    else:
        parts.append("Above 1080p: OFF.")
    if max_extra > 0:
        parts.append(f"Max extras beyond base: {max_extra}.")
    return " ".join(parts)


def core_rules_block_shared() -> str:
    """Rules common to combined movie and TV extract steps."""
    return f"""### CORE RULES (follow strictly, in this order):
1. Return ONLY valid JSON. No markdown fences, no extra text.
2. Use only what is explicit in the Markdown. Never guess. Omit missing fields (no null, no empty strings).
3. Download URLs: copy each URL exactly as written in the Markdown link target. Do not shorten, decode, rebuild, or alter in any way.
4. Never use watch/stream/player/preview/embed links as download entries — only real download/gateway URLs.
5. URL must be absolute with complete hostname. Relative → prepend page domain.
6. Strip blocked site names from TEXT fields only (title, filenames): {_blocked_names_str}. URLs: copy as-is even if they contain a blocked name.
7. One dual/multi-audio file = ONE entry with `l` as array. Do not split same file into separate language entries.
8. If same resolution has both dual-audio and single-language files, keep only dual-audio.
9. Prefer x264 when multiple codec options exist.
10. Never invent resolution keys not shown on the page."""


def seo_block_shared() -> str:
    return """### SEO:
- meta_title: 50-60 chars. meta_description: 140-160 chars, natural CTA. meta_keywords: 10-15 comma-separated."""


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
    ctx_parts = []
    if db_match_candidates:
        ctx_parts.append(
            f"### DB Candidates ({len(db_match_candidates)}):\n```json\n"
            f"{json.dumps(db_match_candidates, separators=(',', ':'), ensure_ascii=False)}\n```"
        )
    if flixbd_results:
        ctx_parts.append(
            f"### {site} search results (top {len(flixbd_results)}):\n"
            f"```json\n{json.dumps(flixbd_results, separators=(',', ':'), ensure_ascii=False)}\n```\n"
            f"(Row `id` → use as `{rk}`, never as `matched_task_id`.)"
        )

    dup_json = json.dumps(duplicate_schema, **_COMPACT)

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


def get_combined_system_prompt(
    locked_content_type: Literal["movie", "tvshow"],
    extra_below: bool = False,
    extra_above: bool = False,
    max_extra: int = 0,
    db_match_candidates: list = None,
    flixbd_results: list = None,
) -> str:
    res_note = _build_resolution_note(extra_below, extra_above, max_extra)
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
