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
        "description": "skip=identical, update=add missing parts/episodes, replace=full replacement, replace_items=replace only overlapping TV items/ranges, process=new content",
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
        "description": "List of resolutions the new version has that existing is missing (e.g. ['480p']). Only for 'update' action.",
        },
        "has_new_episodes": {
            "type": "boolean",
        "description": "True if the new URL has episode labels NOT present in existing_episodes. When true, new episodes will be APPENDED (not replaced).",
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
        "updated_website_title",
    ],
    "additionalProperties": False,
}


DUPLICATE_CHECK_PROMPT = f"""You are a media deduplication assistant. Return ONLY one JSON object matching the schema.

Input:
- `new_website_title`, `new_name`, `new_year`
- `candidates`: DB rows with `id`, `title`, `website_title`, `year`, `resolutions`, `type`, optional TV episode info

Hard rules:
- `matched_task_id` = ONLY an `id` that appears verbatim in the ### DB Candidates JSON block in this message
- If that block is missing or your chosen id is not listed there → `matched_task_id` MUST be null
- `{TARGET_SITE_ROW_ID_JSON_KEY}` = ONLY an `id` from the ### {SITE_NAME} search results JSON in this message, or null
- Never invent, guess, or reuse ids from memory; ids not printed in those JSON blocks are forbidden
- Do not output a non-null id because it feels right — non-null ONLY when you are copying a listed `id`
- Zero DB candidate rows → `matched_task_id` = null. Zero site search rows / no block → `{TARGET_SITE_ROW_ID_JSON_KEY}` = null
- A valid match requires ALL 3: same type, exact year, strong title match
- Year mismatch means NO match
- Movie and TV show are DIFFERENT. Never match movie <-> tvshow.
- Strong title match means normalized titles are the same after trivial formatting cleanup only:
  punctuation, spacing, case, apostrophes, `&` vs `and`, roman numeral vs digit, or obvious transliteration/alias explicitly supported by context
- NOT a valid title match: single shared word, prefix-only match, substring-only match, or "only candidate" pressure
- Examples of NO match: `The Witch` != `The Kitchen`; `Hum` != `Hum Hain Kamaal Ke`; `Nagin` != `Nache Nagin Gali Gali`
- If either side has extra meaningful title words not explained by formatting, sequel numbering, or explicit alias evidence -> NO match
- Never use resolutions/source to rescue a failed title/year/type check
- Use candidate `website_title` for season/source/subtitle clues; do not rely only on plain `title`
- If unsure, return `process` with `matched_task_id=null`

Step 1: detect type
- TV signs: Season, Episode, S01, E01, Complete Season, Web Series, Series
- Otherwise movie

Step 2: pick candidate
- Match by normalized title + exact year + same detected type
- Non-exact title matches are allowed ONLY for trivial formatting differences or clear alias evidence
- Reject prefix/subset/shared-word matches
- Never choose a row just because it is the only candidate
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
- Rejected candidates contribute nothing to `Existing`
- `Missing` = tiers in `Extracted` but not in `Existing`
- If no resolution is found -> `Extracted=[]` and default to `process` unless duplicate evidence is overwhelming
- If `Missing` is non-empty, do NOT auto-pick `update` yet when the matched site row/title clearly shows a lower source tier
- If site title/source tags show old low quality (e.g. `CAM`, `HDTC`, `HDTS`, `HDRip`) and new title clearly shows higher source (e.g. `WEB-DL`, `BluRay`, `REMUX`), prefer `replace`
- Use `update` for Missing only when this is genuinely an add-missing-resolutions case, not a clear low-source -> high-source replacement

Step 5: source upgrade check
- Run this whenever title+year+type match and source tiers are visible, even if `Missing` is non-empty
- Source order: `CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX`
- If new source is clearly higher for the same content -> `replace`
- If same/lower/unclear -> do NOT force `replace`; fall back to Missing/coverage rules
- Unknown codec/tag: use your judgment, but NEVER replace from codec alone; if source superiority is unclear, do not replace

Step 6: TV episodes
- Set `has_new_episodes=true` ONLY when explicit higher episode numbers are visible
- If episode numbers are unclear, set `has_new_episodes=false`
- Never use `replace` for new episode batches
- For TV, compare explicit `season_number` first. Different seasons are the same show but DIFFERENT coverage.
- If the incoming season does not overlap any existing candidate season, NEVER use `replace` or `replace_items` against another season.
- If the show matches but the incoming season is new/missing in the existing row, prefer `update` so the new season is appended.
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

**`updated_website_title`:** Full line ending ` - {SITE_NAME}` only if it beats candidate `website_title`; else `false`. TV season merge → `Season NN-MM` (zero-pad); same show → prefer candidate year.

Action table:
- `skip`: same title+year+type, nothing new, no clear upgrade
- `update`: same title+year+type, missing resolutions, explicit new episodes, or a new/missing season, without a clear overlapping same-season replacement
- `replace`: same title+year+type, same coverage, clearly better source
- `replace_items`: TV only; same title+year+type, but only the overlapping incoming episode range/pack should replace existing items instead of wiping the whole show
- `process`: no confident match, ambiguous title, or unfamiliar title without strong evidence
- `updated_website_title`: see one line above

Reason format:
- Single line only
- MUST start with `Matched candidate id=` or `No candidate matches title+year+type.`
- MUST include `TitleCheck: ...` and `YearCheck: ...`
- MUST include all three lists even when empty: `Extracted: [...] . Existing: [...] . Missing: [...]`
- Pattern:
  `Matched candidate id=X. TitleCheck: <why match>. YearCheck: <why match>. Extracted: [...]. Existing: [...]. Missing: [...]. Action: <action> because <why>.`
  or
  `No candidate matches title+year+type. TitleCheck: <why no match>. YearCheck: <why no match>. Extracted: [...]. Existing: [...]. Missing: [...]. Action: process because <why>.`

JSON Schema:
{json.dumps(duplicate_schema, separators=(',',':'))}
"""
