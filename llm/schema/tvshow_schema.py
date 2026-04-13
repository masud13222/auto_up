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
        "poster_url": {
            "type": "string",
            "description": "Absolute poster/image URL; third-party image hosts/CDNs are allowed.",
        },
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
            "description": (
                "Process/replace: full extracted seasons. Update/replace_items: only the season/item/resolution "
                "scope that should change now."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "season_number": {
                        "type": "integer",
                        "description": "Actual season number from the page block/heading.",
                    },
                    "download_items": {
                        "type": "array",
                        "description": (
                            "For TV update, include only the new/missing episode ranges or missing resolutions. "
                            "Do not repeat unchanged old items."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["single_episode" , "partial_combo", "combo_pack"],
                                    "description": (
                                        "single_episode → one episode; episode_range is a single zero-padded number e.g. '01'. "
                                        "partial_combo → a subset of episodes in the season; episode_range is a zero-padded span e.g. '01-04'."
                                        "combo_pack → full season bundle (all episodes); if no explicit range exists, set episode_range to empty string ''."
                                    ),
                                },
                                "label": {"type": "string"},
                                "episode_range": {
                                    "type": "string",
                                    "description": "Always include. Use '01' or '01-08' when explicit. For true whole-season combo_pack with no explicit range, use empty string ''.",
                                },
                                "resolutions": {
                                    "type": "object",
                                    "patternProperties": {
                                        r"^\d{3,4}p$": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "u": {"type": "string", "description": "Absolute download URL only; never watch/stream/player/watch-online URL"},
                                                "l": {
                                                    "oneOf": [
                                                        {"type": "string"},
                                                        {"type": "array", "items": {"type": "string"}, "minItems": 1},
                                                    ],
                                                    "description": "Language string for single-audio files, or an array like ['Hindi','English'] when one file is dual/multi audio",
                                                },
                                                "f": {"type": "string", "description": "Basename only"},
                                            },
                                            "required": ["u", "l", "f"],
                                            "additionalProperties": False,
                                        },
                                        },
                                    },
                                    "additionalProperties": False,
                                    "description": (
                                        "Pure resolution keys only (480p, 720p, 1080p, etc.). Each value is a list "
                                        "of compact file objects with u=url, l=language-or-language-array, f=filename. "
                                        "If only one resolution under an existing item is missing, include only that "
                                        "resolution."
                                    ),
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
# TV Show System Prompt (standalone — not used in combined)
# ───────────────────────────────────────────────

TVSHOW_SYSTEM_PROMPT = f"""Extract TV show data from Markdown. Return ONLY valid JSON matching the schema.

Rules:
- Use only what is explicit in the Markdown. Never guess or invent values.
- Omit missing optional fields entirely.
- rating/year must be numeric.
- Strip blocked names from all titles: {_blocked_names_str}

website_tvshow_title: `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`
Combo season → `Season NN Complete`. Source = WEB-DL/NF/AMZN etc (not resolution).

poster_url: any absolute direct image URL is valid, including third-party hosts/CDNs.

Download type priority (never duplicate coverage): combo_pack > partial_combo > single_episode (never duplicate coverage).
Classify each download section by scope:
- Full season bundled → combo_pack
- Multiple episodes (range) → partial_combo  
- Exactly one episode → single_episode
Never emit the same episode in more than one download_item.
- Always include `episode_range` in every download_item.
- For a true whole-season combo with no explicit range, set `episode_range` to empty string `""`.

Never invent a season number, episode range, or resolution key that is not clearly shown by the page.
Strict link rule: use only real download/direct-download/gateway URLs. Never use Watch Online, watch link, watch generate link, stream, player, preview, or embed links as `u`.

Example multi-season shape:
`"seasons":[{{"season_number":1,"download_items":[...] }},{{"season_number":2,"download_items":[...]}}]`

Each `resolutions` value must be a list like:
`[{{"u":"ABSOLUTE_URL","l":"Hindi","f":"BASENAME_ONLY"}}]`
If one downloadable file contains multiple audio tracks, return ONE file object only:
`[{{"u":"ABSOLUTE_URL","l":["Hindi","English"],"f":"Title.Year.S01E05.Dual.Audio.720p.WEB-DL.x264.{SITE_NAME}.mkv"}}]`
Do not split one dual/multi-audio file into separate Hindi/English entries when the URL/file is the same.
Only create separate entries when the page clearly provides separate downloadable files per language.
Do not return a separate `download_filenames` object.
- combo: `Title.Year.Hindi.S01.Complete.Res.Src.WEB-DL.x264.{SITE_NAME}.mkv`
- partial: `Title.Year.Hindi.S01E01-E08.Res.Src.WEB-DL.x264.{SITE_NAME}.mkv`
- single: `Title.Year.Hindi.S01E05.Res.Src.WEB-DL.x264.{SITE_NAME}.mkv`
Src: NF/AMZN/DSNP/JC/ZEE5 from title, else omit. Archives → match ext. Default .mkv.

Schema: {json.dumps(tvshow_schema, **_COMPACT)}"""