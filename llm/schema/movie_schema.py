from .blocked_names import SITE_NAME, BLOCKED_SITE_NAMES
from .json_encoding import json_compact

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)

# ───────────────────────────────────────────────
# Movie JSON Schema (LLM structured output)
# ───────────────────────────────────────────────

movie_schema = {
    "type": "object",
    "properties": {
        "website_movie_title": {
            "type": "string",
            "description": f"Formatted title ending with ' - {SITE_NAME}'",
        },
        "title": {"type": "string", "description": "Clean movie name only (no year/quality/language)"},
        "year": {"type": "integer"},
        "genre": {"type": "string"},
        "director": {"type": "string"},
        "rating": {"type": "number", "description": "Numeric only (7.5)"},
        "plot": {"type": "string"},
        "poster_url": {
            "type": "string",
            "description": "Absolute poster/image URL",
        },
        "meta_title": {"type": "string", "description": "SEO title 50-60 chars"},
        "meta_description": {"type": "string", "description": "Meta desc 140-160 chars"},
        "meta_keywords": {"type": "string", "description": "10-15 comma-separated"},
        "download_links": {
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
                                    {"type": "array", "items": {"type": "string"}, "minItems": 1},
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
            "description": "Resolution keys (480p, 720p, 1080p) map to arrays of file-entry objects.",
        },
        "cast": {"type": "string", "description": "Comma-separated actors"},
        "languages": {"type": "array", "items": {"type": "string"}},
        "countries": {"type": "array", "items": {"type": "string"}},
        "imdb_id": {"type": "string"},
        "tmdb_id": {"type": "string"},
        "is_adult": {
            "type": "boolean",
            "description": "true if Tagalog in title OR explicit adult (18+/XXX/erotic). false otherwise.",
        },
    },
    "required": ["website_movie_title", "title", "year", "is_adult", "download_links"],
    "additionalProperties": False,
}


# ───────────────────────────────────────────────
# Standalone movie prompt (non-combined path)
# ───────────────────────────────────────────────

MOVIE_SYSTEM_PROMPT = f"""You are a movie data extraction function. Return ONLY valid JSON.

INPUT: Markdown (converted from HTML). Extract from headings, lists, link labels, and URLs.

RULES (in priority order):
1. Use only what is explicit in the Markdown. Never guess or invent.
2. Omit missing optional fields entirely (no null, no empty strings).
3. Strip blocked names from text fields: {_blocked_names_str}
4. Download URLs: copy exactly as written in Markdown link target. Never modify.
5. Never use watch/stream/player/preview/embed links as download entries.
6. Prefer x264 when multiple codec options exist.
7. One dual/multi-audio file = ONE entry with language array. Do not split.

TITLE: `Title Year Source Language - {SITE_NAME}` (Source = WEB-DL/CAMRip/HDRip/BluRay, not resolution).

FILE ENTRY: `{{"u":"URL","l":"Hindi","f":"Title.Year.Hindi.480p.WEB-DL.x264.{SITE_NAME}.mkv"}}`
Dual audio: `{{"u":"URL","l":["Hindi","English"],"f":"Title.Year.Dual.Audio.720p.WEB-DL.x264.{SITE_NAME}.mkv"}}`

Schema: {json_compact(movie_schema)}"""
