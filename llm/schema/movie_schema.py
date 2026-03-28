import json
from .blocked_names import SITE_NAME, BLOCKED_SITE_NAMES

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)

_COMPACT = {"separators": (",", ":")}

# ───────────────────────────────────────────────
# Movie Info Schema
# ───────────────────────────────────────────────

movie_schema = {
    "type": "object",
    "properties": {
        "website_movie_title": {
            "type": "string",
            "description": f"'Title Year Source Language - {SITE_NAME}'. Source=WEB-DL/CAMRip/HDRip/BluRay/WEBRip/HDTS (not resolution). Strip blocked names.",
        },
        "title": {"type": "string", "description": "Clean movie name only (no year/quality/language)"},
        "year": {"type": "integer"},
        "genre": {"type": "string"},
        "director": {"type": "string"},
        "rating": {"type": "number", "description": "Numeric only (7.5)"},
        "plot": {"type": "string"},
        "poster_url": {"type": "string"},
        "meta_title": {"type": "string", "description": "SEO title 50-60 chars"},
        "meta_description": {"type": "string", "description": "Meta desc 140-160 chars with CTA"},
        "meta_keywords": {"type": "string", "description": "10-15 comma-separated keywords"},
        "download_links": {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "u": {"type": "string", "description": "Absolute download URL only; never watch/stream/player/watch-online URL"},
                        "l": {"type": "string", "description": "Language name"},
                        "f": {"type": "string", "description": "Basename only"},
                    },
                    "required": ["u", "l", "f"],
                },
            },
            "description": "Pure resolution keys only (480p, 720p, 1080p, etc.). Each value is a list of compact file objects with u=url, l=language, f=filename.",
        },
        "cast": {"type": "string", "description": "Comma-separated actors"},
        "languages": {"type": "array", "items": {"type": "string"}},
        "countries": {"type": "array", "items": {"type": "string"}},
        "imdb_id": {"type": "string"},
        "tmdb_id": {"type": "string"},
        "is_adult": {
            "type": "boolean",
            "description": "true if Tagalog in title (any case) OR explicit adult (18+/XXX). false otherwise.",
        },
    },
    "required": ["website_movie_title", "title", "year", "is_adult", "download_links"],
}


# Standalone movie prompt — used only when NOT calling combined.
# Combined prompt (get_combined_system_prompt) is the production path.
MOVIE_SYSTEM_PROMPT = f"""Extract movie info from **Markdown** (page converted HTML→Markdown). Return ONLY valid JSON (no markdown fences).

Rules: omit missing fields (no null/empty). Numeric rating/year. Clean title (no year/quality/language). Strip blocked names: {_blocked_names_str}

website_movie_title: `Title Year Source Language - {SITE_NAME}` (Source=WEB-DL/CAMRip/HDRip/BluRay/WEBRip/HDTS, not resolution).
is_adult: true if Tagalog in title/heading (any case). Else true only for explicit adult (18+/XXX/Adults only). false for mainstream.

SEO: meta_title 50-60 chars (vary structure). meta_description 140-160 chars natural CTA. meta_keywords 10-15 relevant.

download_links: keys must be pure resolutions only, for example `480p`, `720p`, `1080p`.
Strict link rule: use only real download/direct-download/gateway URLs. Never use Watch Online, watch link, watch generate link, stream, player, preview, or embed links as `u`.
Each resolution value must be a list like:
`[{{"u":"ABSOLUTE_URL","l":"Hindi","f":"Title.Year.Src.Hindi.480p.WEB-DL.x264.{SITE_NAME}.mkv"}},{{"u":"ABSOLUTE_URL","l":"English","f":"Title.Year.Src.English.480p.WEB-DL.x264.{SITE_NAME}.mkv"}}]`
`u`=url, `l`=language, `f`=filename basename only (no / \\ :). Do not return a separate `download_filenames` object.
Src: NF(Netflix) / AMZN(Amazon) / DSNP(Hotstar) / JC(Jio) / ZEE5 / else omit extra src. Ext .mkv default.

Schema: {json.dumps(movie_schema, **_COMPACT)}"""
