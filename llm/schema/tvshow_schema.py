import json
from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)

_COMPACT = {"separators": (",", ":")}

# ───────────────────────────────────────────────
# TV Show Schema
# ───────────────────────────────────────────────

tvshow_schema = {
    "type": "object",
    "properties": {
        "website_tvshow_title": {
            "type": "string",
            "description": f"'Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}'. Combo → 'Season NN Complete'. Strip blocked names.",
        },
        "title": {"type": "string", "description": "Clean show name only"},
        "year": {"type": "integer"},
        "genre": {"type": "string"},
        "director": {"type": "string"},
        "rating": {"type": "number", "description": "Numeric only"},
        "plot": {"type": "string"},
        "poster_url": {"type": "string"},
        "meta_title": {"type": "string", "description": "SEO title 50-60 chars"},
        "meta_description": {"type": "string", "description": "Meta desc 140-160 chars with CTA"},
        "meta_keywords": {"type": "string", "description": "10-15 comma-separated keywords"},
        "total_seasons": {"type": "integer"},
        "cast_info": {"type": "string", "description": "Comma-separated actors"},
        "languages": {"type": "array", "items": {"type": "string"}},
        "countries": {"type": "array", "items": {"type": "string"}},
        "imdb_id": {"type": "string"},
        "tmdb_id": {"type": "string"},
        "is_adult": {
            "type": "boolean",
            "description": "true only for explicit adult (18+/XXX). false for mainstream. If unsure false.",
        },
        "seasons": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "season_number": {"type": "integer"},
                    "download_items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["combo_pack", "partial_combo", "single_episode"],
                                },
                                "label": {"type": "string"},
                                "episode_range": {
                                    "type": "string",
                                    "description": "Required for single_episode/partial_combo. Zero-padded: '01', '01-08'. Omit for combo_pack.",
                                },
                                "resolutions": {
                                    "type": "object",
                                    "additionalProperties": {"type": "string"},
                                    "description": "Download URLs per resolution. No watch/stream URLs.",
                                },
                                "download_filenames": {
                                    "type": "object",
                                    "additionalProperties": {"type": "string"},
                                    "description": "Basenames per resolution; keys=resolutions keys. No path separators.",
                                },
                            },
                            "required": ["type", "label", "resolutions", "download_filenames"],
                        },
                    },
                },
                "required": ["season_number", "download_items"],
            },
        },
    },
    "required": ["website_tvshow_title", "title", "year", "is_adult", "seasons"],
}

# ───────────────────────────────────────────────
# TV Show System Prompt (standalone — not used in combined)
# ───────────────────────────────────────────────

TVSHOW_SYSTEM_PROMPT = f"""Extract TV show data from **Markdown** (page converted HTML→Markdown). Return ONLY valid JSON (no markdown fences).

Rules: omit missing fields. Numeric rating/year. Clean title. Strip blocked names: {_blocked_names_str}

website_tvshow_title: `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`. Combo → `Season NN Complete`. Source=WEB-DL/etc (not resolution).
is_adult: true only for explicit adult (18+/XXX). false for mainstream.

SEO: meta_title 50-60 chars. meta_description 140-160 chars CTA. meta_keywords 10-15.

Download item types (classify by Markdown section structure — headings, labels, episode blocks):
- combo_pack: heading covers entire season, no episode breakdown
- partial_combo: heading has episode RANGE (Ep X-Y). Set episode_range.
- single_episode: heading = exactly one episode. Set episode_range (zero-padded).
Priority: combo > partial > single (never duplicate coverage).

download_filenames (per item): keys=resolutions keys. Basename only (no / \\ :).
- combo: `Title.Year.S01.Complete.Res.Src.WEB-DL.x264.{SITE_NAME}.mkv`
- partial: `Title.Year.S01E01-E08.Res.Src.WEB-DL.x264.{SITE_NAME}.mkv`
- single: `Title.Year.S01E05.Res.Src.WEB-DL.x264.{SITE_NAME}.mkv`
Src: NF/AMZN/DSNP/JC/ZEE5 from title, else omit. Archives → match ext. Default .mkv.

Schema: {json.dumps(tvshow_schema, **_COMPACT)}"""