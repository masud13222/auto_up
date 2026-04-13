import json

from .blocked_names import SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY

# ───────────────────────────────────────────────
# Duplicate Detection Schema
# ───────────────────────────────────────────────

_UPDATED_WEBSITE_TITLE_DESC = (
    f"Full website title ending ` - {SITE_NAME}`, or `false` if the stored `website_title` is already correct. "
    "Rewrite only when needed; TV season merge -> `Season NN-MM`."
)

_dup_props = {
        "is_duplicate": {
            "type": "boolean",
        "description": "True if new content is the same media as existing",
        },
        "matched_task_id": {
            "type": ["integer", "null"],
            "description": (
            "null unless you copy one integer `id` from the ### DB Candidates JSON in this message. "
            "If that JSON block is missing, empty, or no row matches → null. "
            "Never infer a MediaTask pk from titles, seasons, or memory. Never use site row ids here."
            ),
        },
        "action": {
            "type": "string",
        "enum": ["skip", "update", "replace", "replace_items", "process"],
        "description": (
            "skip=identical, update=delta add only, replace=full replacement, "
            "replace_items=TV overlapping-scope replacement only, process=new content"
        ),
        },
        "reason": {
            "type": "string",
        "description": (
            "Single line. MUST start with 'Matched candidate id=X.' or 'No candidate matches title+year+type.' "
            "Then include 'TitleCheck: ... YearCheck: ... Extracted: [list]. Existing: [list]. Missing: [list]. "
            "Action: <action> because <why>.' Always include all three lists. "
            "Rejected candidates must not contribute to Existing."
        ),
        },
        "detected_new_type": {
            "type": "string",
            "enum": ["movie", "tvshow"],
        "description": "What you detect the NEW content to be (movie or tvshow) from the website title",
        },
        "missing_resolutions": {
            "type": "array",
            "items": {"type": "string"},
        "description": (
            "Movie update only. These are the only missing/new resolutions; "
            "`data.download_links` should contain only these files."
        ),
        },
        "has_new_episodes": {
            "type": "boolean",
        "description": (
            "TV update only. True if the new URL has new episode labels/ranges. "
            "When true, `data.seasons` should include only the new items to append."
        ),
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
    "description": (
        f"null unless you copy one integer `id` from the ### {SITE_NAME} search results JSON in this message. "
        "If that block is missing, empty, or no row matches → null. "
        "Never infer a site row id from URLs or memory. Never put a MediaTask pk here."
    ),
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


DUPLICATE_CHECK_PROMPT = f"""You are a media deduplication assistant. Return ONLY one JSON object matching the schema.

Input:
- `new_website_title`, `new_name`, `new_year`
- `candidates`: DB rows with `id`, `title`, `website_title`, `year`, `type`, and optional resolution / TV item info

Core rules:
- `matched_task_id` must be copied from the DB candidate ids shown in the prompt, or null.
- `{TARGET_SITE_ROW_ID_JSON_KEY}` must be copied from the {SITE_NAME} row ids shown in the prompt, or null.
- Never invent ids.
- A real match needs all three: same type, exact year, and strong title match.
- Movie and TV are different. Never match movie with tvshow.
- Strong title match means only small formatting cleanup is needed. If meaningful words differ, do not match.
- If unsure, use `process`.

Type:
- TV signs: Season, Episode, S01, E01, Complete Season, Web Series, Series.
- Otherwise movie.

Coverage rules:
- `skip` = same content, nothing new.
- `update` = add only the new part.
- `replace` = same coverage, but clearly better source.
- `replace_items` = TV only; only the overlapping same-season range should be replaced.
- `process` = no confident match.

Resolution rules:
- Normalize to `480p`, `720p`, `1080p`, `1440p`, `2160p`.
- Convert `4K` to `2160p`.
- Ignore codec-only words like `x264`, `x265`, `HEVC`, `AAC`, `AVC`, `10bit`.
- `Extracted` = normalized new tiers.
- `Existing` = matched candidate tiers only.
- `Missing` = resolutions that are in `Extracted` but not in `Existing`.

Source rules:
- Use source quality only after title/year/type already match.
- Higher source for the same coverage can be `replace`.
- Never replace from codec alone.

TV rules:
- `has_new_episodes=true` only when explicit new episode labels or ranges are visible.
- New later range or new season -> `update`.
- Same range with better pack/source -> `replace` or `replace_items`.
- Different seasons are additive; do not replace another season.
- If a whole-season combo pack is involved, prefer `replace` over `replace_items`.

Output rules:
- Movie `update`: set `missing_resolutions` to the exact missing movie resolutions only.
- TV `update`: set `has_new_episodes=true` when the new season/item/range should be appended.
- `updated_website_title` should be a better final title ending with ` - {SITE_NAME}`, or `false`.

Reason rules:
- Single line only.
- Start with `Matched candidate id=` or `No candidate matches title+year+type.`
- Include `TitleCheck`, `YearCheck: new_year <N> vs candidate <M> -> ...`, `Extracted`, `Existing`, `Missing`, and `Action: ... because ...`.

JSON Schema:
{json.dumps(duplicate_schema, separators=(',',':'))}
"""
