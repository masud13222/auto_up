from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME
from .json_encoding import json_compact

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)

# ───────────────────────────────────────────────
# TV show JSON Schema (LLM structured output)
# ───────────────────────────────────────────────

tvshow_schema = {
    "type": "object",
    "properties": {
        "website_tvshow_title": {
            "type": "string",
            "description": f"Formatted title ending with ' - {SITE_NAME}'",
        },
        "title": {"type": "string", "description": "Clean show name only"},
        "year": {"type": "integer"},
        "genre": {"type": "string"},
        "director": {"type": "string"},
        "rating": {"type": "number", "description": "Numeric only"},
        "plot": {"type": "string"},
        "poster_url": {
            "type": "string",
            "description": "Absolute poster/image URL",
        },
        "meta_title": {"type": "string", "description": "SEO title 50-60 chars"},
        "meta_description": {"type": "string", "description": "Meta desc 140-160 chars"},
        "meta_keywords": {"type": "string", "description": "10-15 comma-separated"},
        "total_seasons": {"type": "integer"},
        "cast_info": {"type": "string", "description": "Comma-separated actors"},
        "languages": {"type": "array", "items": {"type": "string"}},
        "countries": {"type": "array", "items": {"type": "string"}},
        "imdb_id": {"type": "string"},
        "tmdb_id": {"type": "string"},
        "is_adult": {
            "type": "boolean",
            "description": "true only for explicit 18+/XXX content",
        },
        "seasons": {
            "type": "array",
            "description": "Array of season objects with download items",
            "items": {
                "type": "object",
                "properties": {
                    "season_number": {
                        "type": "integer",
                        "description": "Season number from page heading",
                    },
                    "download_items": {
                        "type": "array",
                        "description": "Download entries for this season",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["single_episode", "partial_combo", "combo_pack"],
                                    "description": "single_episode=1 ep, partial_combo=range, combo_pack=full season",
                                },
                                "label": {"type": "string"},
                                "episode_range": {
                                    "type": "string",
                                    "description": "Zero-padded: '01', '01-08', or '' for whole-season combo",
                                },
                                "resolutions": {
                                    "type": "object",
                                    "patternProperties": {
                                        r"^(?:\d{3,4}p|4[kK])$": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "u": {
                                                        "type": "string",
                                                        "description": "Absolute download URL exactly as in Markdown",
                                                    },
                                                    "l": {
                                                        "oneOf": [
                                                            {"type": "string"},
                                                            {
                                                                "type": "array",
                                                                "items": {"type": "string"},
                                                                "minItems": 1,
                                                            },
                                                        ],
                                                        "description": "Language string or array for dual/multi audio",
                                                    },
                                                    "f": {"type": "string", "description": "Basename only"},
                                                },
                                                "required": ["u", "l", "f"],
                                                "additionalProperties": False,
                                            },
                                        },
                                    },
                                    "additionalProperties": False,
                                    "description": "Resolution keys (480p, 720p, 1080p, etc.) -> file list",
                                },
                            },
                            "required": ["type", "label", "episode_range", "resolutions"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["season_number", "download_items"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["website_tvshow_title", "title", "year", "is_adult", "seasons"],
    "additionalProperties": False,
}


# ───────────────────────────────────────────────
# Standalone TV prompt (non-combined path)
# ───────────────────────────────────────────────

TVSHOW_SYSTEM_PROMPT = f"""You are a TV show data extraction function. Return ONLY valid JSON.

INPUT: Markdown (converted from HTML). Extract from headings, lists, link labels, and URLs.

RULES (in priority order):
1. Use only what is explicit in the Markdown. Never guess or invent.
2. Omit missing optional fields entirely (no null, no empty strings).
3. Strip blocked names from text fields: {_blocked_names_str}
4. Download URLs: copy exactly as written in Markdown link target. Never modify.
5. Never use watch/stream/player/preview/embed links as download entries.
6. Prefer x264 when multiple codec options exist.
7. One dual/multi-audio file = ONE entry with language array. Do not split.

TITLE: `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`
Combo season → `Season NN Complete`. Source = WEB-DL/NF/AMZN etc (not resolution).

DOWNLOAD TYPE DECISION TREE (follow strictly):
- Heading says a RANGE like "Episode 01-08" or "EP41-EP48" → partial_combo, episode_range="01-08" or "41-48"
- Heading says "Complete Season" or full season → combo_pack, episode_range=""
- Heading says exactly ONE episode → single_episode, episode_range="05"
COMMON MISTAKE: "Episode 41-48" = ONE partial_combo, NOT 8 single_episodes!
Priority: combo > partial > single. Never duplicate coverage.

FILE ENTRY: `{{"u":"URL","l":"Hindi","f":"Title.Year.S01E05.720p.WEB-DL.x264.{SITE_NAME}.mkv"}}`
Dual audio: `{{"u":"URL","l":["Hindi","English"],"f":"Title.Year.S01.Complete.Dual.Audio.720p.{SITE_NAME}.mkv"}}`

Schema: {json_compact(tvshow_schema)}"""
