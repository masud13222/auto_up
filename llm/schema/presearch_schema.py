"""
JSON Schema and prompt builders for the presearch step:
infer content_type, title, alternate title, year, and season tag from page markdown.
"""

from __future__ import annotations

import json

_COMPACT = {"separators": (",", ":")}

presearch_response_schema = {
    "type": "object",
    "properties": {
        "content_type": {
            "type": "string",
            "enum": ["movie", "tvshow"],
            "description": "movie = single film; tvshow = series with seasons/episodes",
        },
        "name": {
            "type": "string",
            "description": "Primary title (no quality, no site branding)",
        },
        "alt_name": {
            "type": "string",
            "description": "Alternate title if clearly present in the text, else empty string",
        },
        "year": {
            "type": "string",
            "description": "Release year as four digits, or empty string if unknown",
        },
        "season_tag": {
            "type": "string",
            "description": (
                "For tvshow only: copy the season label exactly as on the page "
                "(e.g. S01, S1, Season 01, Season 1). Empty for movies or unknown."
            ),
        },
    },
    "required": ["content_type", "name", "alt_name", "year", "season_tag"],
    "additionalProperties": False,
}


def _presearch_schema_json() -> str:
    return json.dumps(presearch_response_schema, **_COMPACT)


def get_presearch_system_prompt() -> str:
    """Full system prompt: rules + inlined JSON Schema for the model response."""
    schema_block = _presearch_schema_json()
    return f"""You extract search metadata from entertainment page markdown (HTML converted to Markdown).
Return ONE JSON object only. No markdown fences, no extra text.

All keys in the schema are required. Use empty string for unknown optional values.

Rules:
- content_type: use tvshow if seasons/episodes/Sxx/Season appear; otherwise movie.
- name: main title only; strip release group, quality, resolution, WEB-DL, dubbed tags, site names.
- alt_name: second title only if clearly a different release title (e.g. translated vs original). Else "".
- year: 4-digit release year if explicit; if several years appear, prefer the one next to the main title; else "".
- season_tag: for tvshow, copy the season marker exactly as written (S01 vs Season 01). Else "".

JSON schema:
{schema_block}
"""


def build_presearch_user_prompt(markdown_snippet: str) -> str:
    """User message wrapping the markdown body the model must read."""
    return (
        "MARKDOWN (may be truncated):\n```\n"
        f"{markdown_snippet}\n```\n"
        "Return the JSON object now."
    )


def presearch_system_prompt_block() -> str:
    """
    Compact JSON-schema string only (for debugging or tooling).
    Prefer get_presearch_system_prompt() for LLM calls.
    """
    return _presearch_schema_json()
