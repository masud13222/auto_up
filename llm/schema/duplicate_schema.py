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
        "update_details": {
            "type": "object",
            "description": "Only when action=update: structured breakdown of what to update",
            "properties": {
                "missing_items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "season_number": {"type": "integer"},
                            "episode_range": {"type": "string"},
                            "missing_resolutions": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "is_new_range": {"type": "boolean"},
                        },
                        "required": ["missing_resolutions"],
                    },
                    "description": "Each entry = one download group with its missing resolution keys",
                },
                "summary": {
                    "type": "string",
                    "description": "One-line human-readable: e.g. 'S05 EP41-48: need 480p; EP49-56: new range (720p,1080p)'",
                },
            },
            "required": ["missing_items", "summary"],
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

INPUT: new_website_title, new_name, new_year + two data sources.

RULES:
1. Action is decided ONLY by {SITE_NAME} search results (target site).
   - {SITE_NAME} match (same type + exact year + strong title) → skip/update/replace/replace_items.
   - No {SITE_NAME} match → process.
2. matched_task_id comes ONLY from DB Candidates. DB match never changes the action.
3. {TARGET_SITE_ROW_ID_JSON_KEY} comes ONLY from {SITE_NAME} results.
4. Movie ≠ tvshow. Never cross-match.

ACTIONS:
- skip: {SITE_NAME} match, nothing new.
- update: {SITE_NAME} match, missing parts.
- replace: {SITE_NAME} match, better source. Order: CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX.
- replace_items: {SITE_NAME} match, TV only, overlapping replacement.
- process: no {SITE_NAME} match.

NORMALIZE: 480p, 720p, 1080p, 1440p, 2160p (4K→2160p). Ignore codecs.

REASON: Start with 'Matched {SITE_NAME} row id=X.' or 'No {SITE_NAME} match.'
Then: TitleCheck, YearCheck, Extracted:[...], Existing:[...], Missing:[...], Action: <action> because <why>.
If DB matched, append: 'DB matched_task_id=Y.'

Schema: {json.dumps(duplicate_schema, separators=(',',':'))}
"""
