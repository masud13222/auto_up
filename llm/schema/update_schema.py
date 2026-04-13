import json

from .blocked_names import SITE_NAME

_COMPACT = {"separators": (",", ":")}

# ───────────────────────────────────────────────
# Pass-2 Update Schema: Delta-only filtering
# ───────────────────────────────────────────────
# Called ONLY when Pass-1 returns action="update".
# LLM receives: full extracted data + existing coverage
# LLM returns:  ONLY the missing parts (delta).

_movie_update_output = {
    "type": "object",
    "properties": {
        "download_links": {
            "type": "object",
            "description": "Only missing resolution keys -> file list",
            "patternProperties": {
                r"^\d{3,4}p$": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "u": {"type": "string"},
                            "l": {
                                "oneOf": [
                                    {"type": "string"},
                                    {"type": "array", "items": {"type": "string"}, "minItems": 1},
                                ],
                            },
                            "f": {"type": "string"},
                        },
                        "required": ["u", "l", "f"],
                    },
                },
            },
            "additionalProperties": False,
        },
    },
    "required": ["download_links"],
    "additionalProperties": False,
}

_tv_update_output = {
    "type": "object",
    "properties": {
        "seasons": {
            "type": "array",
            "description": "Only seasons/items/resolutions that are missing from existing",
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
                                    "enum": ["single_episode", "partial_combo", "combo_pack"],
                                },
                                "label": {"type": "string"},
                                "episode_range": {"type": "string"},
                                "resolutions": {
                                    "type": "object",
                                    "patternProperties": {
                                        r"^\d{3,4}p$": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "u": {"type": "string"},
                                                    "l": {
                                                        "oneOf": [
                                                            {"type": "string"},
                                                            {"type": "array", "items": {"type": "string"}, "minItems": 1},
                                                        ],
                                                    },
                                                    "f": {"type": "string"},
                                                },
                                                "required": ["u", "l", "f"],
                                            },
                                        },
                                    },
                                    "additionalProperties": False,
                                },
                            },
                            "required": ["type", "label", "episode_range", "resolutions"],
                        },
                    },
                },
                "required": ["season_number", "download_items"],
            },
        },
    },
    "required": ["seasons"],
    "additionalProperties": False,
}

update_output_schema = {
    "type": "object",
    "properties": {
        "content_type": {
            "type": "string",
            "enum": ["movie", "tvshow"],
        },
        "delta": {
            "type": "object",
            "description": "Only the missing data (movie: download_links, tvshow: seasons)",
        },
    },
    "required": ["content_type", "delta"],
    "additionalProperties": False,
    "allOf": [
        {
            "if": {"properties": {"content_type": {"const": "movie"}}},
            "then": {"properties": {"delta": _movie_update_output}},
        },
        {
            "if": {"properties": {"content_type": {"const": "tvshow"}}},
            "then": {"properties": {"delta": _tv_update_output}},
        },
    ],
}


def get_update_system_prompt(content_type: str) -> str:
    """Build the Pass-2 delta-filter system prompt.

    This prompt is intentionally short and single-purpose:
    one task = compare existing vs extracted, return only missing.
    """
    if content_type == "movie":
        schema_json = json.dumps(_movie_update_output, **_COMPACT)
        type_rules = """MOVIE COMPARISON:
- Compare by resolution key (480p, 720p, 1080p, etc.).
- If a resolution exists in EXISTING → omit it entirely from output.
- Only return resolutions that are NOT in EXISTING."""
        example = f"""EXAMPLE:
EXISTING: {{"download_links":{{"720p":[...],"1080p":[...]}}}}
EXTRACTED: {{"download_links":{{"480p":[{{"u":"https://x.com/480","l":"Hindi","f":"Movie.480p.{SITE_NAME}.mkv"}}],"720p":[...],"1080p":[...]}}}}
CORRECT OUTPUT: {{"content_type":"movie","delta":{{"download_links":{{"480p":[{{"u":"https://x.com/480","l":"Hindi","f":"Movie.480p.{SITE_NAME}.mkv"}}]}}}}}}
720p and 1080p exist → omitted. Only 480p returned.
WRONG: returning 720p or 1080p that already exist."""
    else:
        schema_json = json.dumps(_tv_update_output, **_COMPACT)
        type_rules = """TV COMPARISON:
- Compare by season_number + episode_range + resolution key.
- Same season + same episode_range + same resolution in EXISTING → omit that resolution.
- Same season + same episode_range but some resolutions missing → keep ONLY missing resolutions under that item.
- Entirely new episode_range (not in EXISTING at all) → include with all its resolutions.
- If a whole season is fully covered → omit entire season from output.
- If nothing is missing → return empty seasons array."""
        example = f"""EXAMPLE 1 — missing resolution:
EXISTING seasons: [{{"season_number":5,"download_items":[{{"type":"partial_combo","episode_range":"41-48","resolutions":{{"720p":[...],"1080p":[...]}}}}]}}]
EXTRACTED seasons: [{{"season_number":5,"download_items":[{{"type":"partial_combo","label":"Episode 41-48","episode_range":"41-48","resolutions":{{"480p":[{{"u":"https://x.com/480","l":"Hindi","f":"S05E41-E48.480p.{SITE_NAME}.mkv"}}],"720p":[...],"1080p":[...]}}}}]}}]
CORRECT OUTPUT: {{"content_type":"tvshow","delta":{{"seasons":[{{"season_number":5,"download_items":[{{"type":"partial_combo","label":"Episode 41-48","episode_range":"41-48","resolutions":{{"480p":[{{"u":"https://x.com/480","l":"Hindi","f":"S05E41-E48.480p.{SITE_NAME}.mkv"}}]}}}}]}}]}}}}
720p, 1080p exist → omitted. Only 480p returned.

EXAMPLE 2 — new episode range:
EXISTING seasons: [{{"season_number":2,"download_items":[{{"type":"partial_combo","episode_range":"01-06","resolutions":{{"720p":[...],"1080p":[...]}}}}]}}]
EXTRACTED seasons: [{{"season_number":2,"download_items":[{{"type":"partial_combo","label":"Episode 01-06","episode_range":"01-06","resolutions":{{"480p":[...],"720p":[...],"1080p":[...]}}}},{{"type":"partial_combo","label":"Episode 07-12","episode_range":"07-12","resolutions":{{"720p":[...],"1080p":[...]}}}}]}}]
CORRECT OUTPUT: delta.seasons includes EP01-06 with ONLY 480p (720p/1080p exist) + EP07-12 with ALL resolutions (entirely new range).

EXAMPLE 3 — nothing missing:
EXISTING has all resolutions for all episode ranges in extracted.
CORRECT OUTPUT: {{"content_type":"tvshow","delta":{{"seasons":[]}}}}"""

    return f"""You are a delta filter. Your ONLY job: compare EXISTING data with EXTRACTED data and return ONLY what is missing.

RULES:
1. Return ONLY valid JSON. No markdown fences, no extra text.
2. Never modify URLs, filenames, or language values from EXTRACTED. Copy exactly.
3. If something exists in EXISTING, it MUST NOT appear in your output.
4. If nothing is missing, return empty (movie: empty download_links, tvshow: empty seasons array).
5. If you determine the UPDATE HINT is wrong and actually nothing needs updating, return empty delta. This overrides the hint — you are the final judge.

{type_rules}

{example}

Output schema (`delta` field):
```json
{schema_json}
```

Return ONLY: {{"content_type":"{content_type}","delta":{{...}}}}"""
