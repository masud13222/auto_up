import json

from .blocked_names import SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY

# ───────────────────────────────────────────────
# Duplicate Detection Schema
# ───────────────────────────────────────────────

_UPDATED_WEBSITE_TITLE_DESC = (
    f"Better title ending ' - {SITE_NAME}', or false if stored title is fine"
)

_dup_props = {
        "is_duplicate": {
            "type": "boolean",
        "description": "True if same media as existing",
        },
        "matched_task_id": {
            "type": ["integer", "null"],
            "description": "Integer id from DB Candidates, or null",
        },
        "action": {
            "type": "string",
        "enum": ["skip", "update", "replace", "replace_items", "process"],
        "description": "skip|update|replace|replace_items|process",
        },
        "reason": {
            "type": "string",
        "description": "Single-line reasoning with TitleCheck, YearCheck, Extracted, Existing, Missing, Action",
        },
        "detected_new_type": {
            "type": "string",
            "enum": ["movie", "tvshow"],
        "description": "Detected type of new content",
        },
        "missing_resolutions": {
            "type": "array",
            "items": {"type": "string"},
        "description": "Movie update only: list of missing resolution keys",
        },
        "has_new_episodes": {
            "type": "boolean",
        "description": "TV update only: true if new episodes/ranges found",
    },
        "updated_website_title": {
            "oneOf": [
                {"type": "string"},
                {"type": "boolean", "enum": [False]},
            ],
            "description": _UPDATED_WEBSITE_TITLE_DESC,
        },
}

_dup_props[TARGET_SITE_ROW_ID_JSON_KEY] = {
    "type": ["integer", "null"],
    "description": f"Integer id from {SITE_NAME} search results, or null",
}

duplicate_schema = {
    "type": "object",
    "properties": _dup_props,
    "required": [
        "is_duplicate",
        "matched_task_id",
        TARGET_SITE_ROW_ID_JSON_KEY,
        "action",
        "reason",
        "detected_new_type",
        "missing_resolutions",
        "has_new_episodes",
        "updated_website_title",
    ],
    "additionalProperties": False,
}


DUPLICATE_CHECK_PROMPT = f"""You are a media deduplication function. Return ONLY one JSON object.

INPUT: new_website_title, new_name, new_year + candidates (DB rows with id, title, website_title, year, type, resolutions/TV items).

MATCHING RULES:
1. Match requires ALL: same type + exact year + strong title match.
2. Movie ≠ tvshow. Never cross-match.
3. matched_task_id = copy from DB candidate ids only, or null.
4. {TARGET_SITE_ROW_ID_JSON_KEY} = copy from {SITE_NAME} row ids only, or null.
5. If unsure → action=process.

ACTIONS:
- skip: identical content, nothing new.
- update: add only missing parts (delta).
- replace: same coverage but better source.
- replace_items: TV only, overlapping same-season replacement.
- process: no confident match.

RESOLUTION RULES:
- Normalize: 480p, 720p, 1080p, 1440p, 2160p (4K→2160p).
- Ignore codecs: x264, x265, HEVC, AAC.
- Source order: CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX.

REASON FORMAT:
Start with 'Matched candidate id=X.' or 'No candidate matches title+year+type.'
Include: TitleCheck, YearCheck, Extracted:[...], Existing:[...], Missing:[...], Action: <action> because <why>.

Schema: {json.dumps(duplicate_schema, separators=(',',':'))}
"""
