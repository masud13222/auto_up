import json

from .blocked_names import SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY

# ───────────────────────────────────────────────
# Duplicate Detection Schema
# ───────────────────────────────────────────────

_dup_props = {
        "is_duplicate": {
            "type": "boolean",
        "description": "True if new content is the same media as existing",
        },
        "matched_task_id": {
            "type": ["integer", "null"],
            "description": (
            "ONLY our upload DB (MediaTask) primary key from ### DB Candidates `id`. "
            "Never use target-site row ids here. If no DB Candidates block or no matching DB row → null."
            ),
        },
        "action": {
            "type": "string",
        "enum": ["skip", "update", "replace", "replace_items", "process"],
        "description": "skip=identical, update=add missing parts/episodes, replace=full replacement, replace_items=replace only overlapping TV items/ranges, process=new content",
        },
        "reason": {
            "type": "string",
        "description": (
            "Single-line string. MUST start with 'Matched candidate id=X.' or 'No candidate matches title+year+type.' "
            "then 'Extracted: [list]. Existing: [list]. Missing: [list]. Action: <action> because <why>.' "
            "Always include all three lists (use [] if empty). No other format."
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
        "description": "List of resolutions the new version has that existing is missing (e.g. ['480p']). Only for 'update' action.",
        },
        "has_new_episodes": {
            "type": "boolean",
        "description": "True if the new URL has episode labels NOT present in existing_episodes. When true, new episodes will be APPENDED (not replaced).",
    },
        "updated_title": {
            "oneOf": [
                {"type": "string"},
                {"type": "boolean", "enum": [False]},
            ],
            "description": (
                "For duplicate update/replace flows only. Return a new website title string ONLY when the matched "
                "existing row/title should be renamed to reflect broader merged coverage "
                "(example: old `Season 01 Complete`, incoming `Season 02 Complete` -> `Season 01-02 Complete`). "
                "If no title change is needed, return false."
            ),
        },
}

_dup_props[TARGET_SITE_ROW_ID_JSON_KEY] = {
    "type": ["integer", "null"],
    "description": (
        f"{SITE_NAME} site content row id from ### {SITE_NAME} search results (`id`) when that row matches "
        "title+year+type and you skip/update/replace that site row. "
        "Must be null when no matching site row. Never put a MediaTask pk here. "
        "The pipeline does not guess this id — you must return it or null."
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
        "updated_title",
    ],
}


DUPLICATE_CHECK_PROMPT = f"""You are a strict media deduplication assistant. Return ONLY one JSON object matching the schema.

Input:
- `new_website_title`, `new_name`, `new_year`
- `candidates`: DB rows with `id`, `title`, `year`, `resolutions`, `type`, optional TV episode info

Hard rules:
- `matched_task_id` = ONLY a DB candidate `id`
- `{TARGET_SITE_ROW_ID_JSON_KEY}` = ONLY a {SITE_NAME} site row `id` when surrounding instructions provide site rows
- Never invent ids
- Year mismatch means different content
- Movie and TV show are DIFFERENT content types. Never match movie <-> tvshow.
- Exact title match is strongest, but NOT required when the title is clearly an alternate / shortened / variant form of the same media.
- If the title looks unfamiliar, think carefully before answering: compare core title words, year, type, source clues, episode/season clues, and whether the difference looks like an alias vs a genuinely different subtitle.
- If the new title has meaningful extra words/subtitle tokens not present in the candidate title
  (examples: `Members Only`, `Returns`, `Part 2`, `Season 1`, `Episode 5`), treat as different content.
- If unsure, avoid `replace`

Step 1: detect type
- TV signs: Season, Episode, S01, E01, Complete Season, Web Series, Series
- Otherwise movie

Step 2: pick candidate
- Match by normalized title + exact year + same detected type
- Ignore punctuation-only differences, but DO NOT ignore meaningful subtitle words
- If titles are not exact, you may still match when they look like the same media under an alternate / shortened / romanized title AND there is no conflicting subtitle/sequel/season signal.
- For non-exact title matches, require strong evidence: same year, same type, and close core-title meaning. Otherwise use `process`.
- If multiple exact-year same-type matches, choose the closest full title
- If no candidate matches title+year+type -> `action="process"`, `matched_task_id=null`

Step 3: type mismatch
- If detected type differs from candidate type, that candidate is NOT a match
- Set `matched_task_id=null` and use `process`

Step 4: resolution comparison
- Normalize extracted resolution tiers from `new_website_title`
- Canonical labels: `480p`, `720p`, `1080p`, `1440p`, `2160p`; convert `4K` -> `2160p`
- If a clear resolution number appears without `p` (e.g. `720`), convert to `720p`
- Ignore codec tags: `x264`, `x265`, `HEVC`, `AAC`, `AVC`, `10bit`
- Ignore codec alone for `replace`
- `Extracted` = normalized new tiers
- `Existing` = matched candidate `resolutions`; if surrounding instructions include target-site rows, also use that row's `resolution_keys`
- `Missing` = tiers in `Extracted` but not in `Existing`
- If no resolution is found -> `Extracted=[]` and default to `process` unless duplicate evidence is overwhelming
- If `Missing` is non-empty, do NOT auto-pick `update` yet when the matched site row/title clearly shows a lower source tier
- If site title/source tags show old low quality (e.g. `CAM`, `HDTC`, `HDTS`, `HDRip`) and new title clearly shows higher source (e.g. `WEB-DL`, `BluRay`, `REMUX`), prefer `replace`
- Use `update` for Missing only when this is genuinely an add-missing-resolutions case, not a clear low-source -> high-source replacement

Step 5: source upgrade check
- Run this whenever title+year+type match and source tiers are visible, even if `Missing` is non-empty
- Source order: `CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX`
- If new source is clearly higher for the same content -> `replace`
- If same/lower/unclear -> `skip`
- Unknown codec/tag: use your judgment, but NEVER replace from codec alone; if source superiority is unclear, do not replace

Step 6: TV episodes
- Set `has_new_episodes=true` ONLY when explicit higher episode numbers are visible
- If episode numbers are unclear, set `has_new_episodes=false`
- Never use `replace` for new episode batches
- For TV, compare explicit `season_number` first. Different seasons are the same show but DIFFERENT coverage.
- If the incoming season does not overlap any existing candidate season, NEVER use `replace` or `replace_items` against another season.
- If the show matches but the incoming season is new/missing in the existing row, prefer `update` so the new season is appended.
- When the matched title should expand to reflect merged TV coverage (for example old title only shows `Season 01` but incoming data adds `Season 02`), set `updated_title` to the better combined website title. Otherwise set `updated_title=false`.
- Show-wide resolution lists are only a weak signal for TV. Do NOT replace based on resolution/source alone when the incoming season differs from the existing season.
- Use explicit `episode_range` logic:
  - genuinely NEW higher range/batch -> `update`
  - same range covered in a better pack form (e.g. old singles -> new partial combo, old partial -> new combo, old combo reissued better) -> `replace`
  - same or overlapping range without clear upgrade -> avoid guessing; prefer `skip`

TV pack upgrade rules:
- single_episode -> partial_combo for the SAME covered episode range = usually `replace`
- single_episode/partial_combo -> combo_pack for the SAME season coverage = usually `replace`
- same episode coverage with clearly better source = `replace`
- only additional later episodes = `update`
- same show but different explicit season_number = `update`, not `replace` / `replace_items`
- do NOT invent episode math from labels if explicit `episode_range` is missing; rely on explicit range when available
- If only the incoming overlapping TV items should be replaced (for example old singles `09`,`10`,`11` replaced by new partial `09-11` while `01-08` stays untouched), use `action="replace_items"` instead of full `replace`
- Use `replace_items` only when the replace scope is explicit and NOT a whole-season combo pack on either side; if a combo/complete-season pack is involved, prefer full `replace`

Action table:
- `skip`: same title+year+type, nothing new, no clear upgrade
- `update`: same title+year+type, missing resolutions, explicit new episodes, or a new/missing season, without a clear overlapping same-season replacement
- `replace`: same title+year+type, same coverage, clearly better source
- `replace_items`: TV only; same title+year+type, but only the overlapping incoming episode range/pack should replace existing items instead of wiping the whole show
- `process`: no confident match, ambiguous title, or unfamiliar title without strong evidence
- `updated_title`: false when old title can stay as-is; otherwise the exact replacement website title string

Reason format:
- Single line only
- MUST start with `Matched candidate id=` or `No candidate matches title+year+type.`
- MUST include all three lists even when empty: `Extracted: [...] . Existing: [...] . Missing: [...]`
- Pattern:
  `Matched candidate id=X. Extracted: [...]. Existing: [...]. Missing: [...]. Action: <action> because <why>.`
  or
  `No candidate matches title+year+type. Extracted: [...]. Existing: []. Missing: [...]. Action: process because <why>.`

JSON Schema:
{json.dumps(duplicate_schema, separators=(',',':'))}
"""
