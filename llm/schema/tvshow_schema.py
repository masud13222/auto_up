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
            "description": f"Formatted title ending with ' - {SITE_NAME}'",
        },
        "title": {"type": "string", "description": "Clean show name only"},
        "year": {"type": "integer"},
        "genre": {"type": "string"},
        "director": {"type": "string"},
        "rating": {"type": "number", "description": "Numeric only"},
        "plot": {"type": "string"},
        "poster_url": {
            "type": "string",
            "description": "Absolute poster/image URL",
        },
        "meta_title": {"type": "string", "description": "SEO title 50-60 chars"},
        "meta_description": {"type": "string", "description": "Meta desc 140-160 chars"},
        "meta_keywords": {"type": "string", "description": "10-15 comma-separated"},
        "total_seasons": {"type": "integer"},
        "cast_info": {"type": "string", "description": "Comma-separated actors"},
        "languages": {"type": "array", "items": {"type": "string"}},
        "countries": {"type": "array", "items": {"type": "string"}},
        "imdb_id": {"type": "string"},
        "tmdb_id": {"type": "string"},
        "is_adult": {
            "type": "boolean",
            "description": "true only for explicit 18+/XXX content",
        },
        "seasons": {
            "type": "array",
            "description": "Array of season objects with download items",
            "items": {
                "type": "object",
                "properties": {
                    "season_number": {
                        "type": "integer",
                        "description": "Season number from page heading",
                    },
                    "download_items": {
                        "type": "array",
                        "description": "Download entries for this season",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["single_episode", "partial_combo", "combo_pack"],
                                    "description": "single_episode=1 ep, partial_combo=range, combo_pack=full season",
                                },
                                "label": {"type": "string"},
                                "episode_range": {
                                    "type": "string",
                                    "description": "Zero-padded: '01', '01-08', or '' for whole-season combo",
                                },
                                "resolutions": {
                                    "type": "object",
                                    "patternProperties": {
                                        r"^\d{3,4}p$": {
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
                                    "description": "Resolution keys (480p, 720p, 1080p, etc.) -> file list",
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


def build_combined_tv_extract_body(core_rules_block: str, seo_block: str, res_note: str) -> str:
    site = SITE_NAME
    return f"""INPUT: Markdown (HTML→Markdown). This page is a **TV show** (series). Extract TV data only.

## EXTRACT (rules below, then schema)

{core_rules_block}

### MEDIA (TV)
Never invent season numbers or episode ranges not shown on the page.

### RESOLUTION: {res_note}

### TITLES:
- TV: `Title Year Season NN EPxx[-yy] Source Language - {site}`. Combo → `Season NN Complete`.

{seo_block}

### FILE ENTRIES:
Each resolution value = list of objects: `{{"u":"URL","l":"Hindi","f":"BASENAME"}}`
Filename pattern (dots not spaces): `Title.Year.<segment>.<lang>.<res>.<src>.WEB-DL.x264.{site}.<ext>`
- TV combo: Title.Year.S01.Complete.Lang.Res...
- TV partial: Title.Year.S01E01-E08.Lang.Res...
- TV single: Title.Year.S01E05.Lang.Res...
- Dual audio → use `Dual.Audio` in filename. Src: NF/AMZN/DSNP/JC/ZEE5 if clear, else omit. Default ext: .mkv.
- `f` = basename only (no / \\ :). Do not return separate `download_filenames` object.

### TV DOWNLOAD ITEM CLASSIFICATION (critical — follow this decision tree):

Step 1: Look at the Markdown section heading/label for each download block.
Step 2: Classify STRICTLY by what the heading says:
  - Heading says "Episode 01-08" or "EP01-EP08" or any RANGE of episodes → `partial_combo`, episode_range="01-08"
  - Heading says "Complete Season" or entire season with no episode breakdown → `combo_pack`, episode_range=""
  - Heading says exactly ONE episode like "Episode 05" or "EP05" → `single_episode`, episode_range="05"

COMMON MISTAKE TO AVOID: If the heading says "Episode 41-48" that is a RANGE → `partial_combo`, NOT multiple single_episodes. Do NOT split "Episode 41-48" into 8 separate single_episode items. One heading = one download_item.

Priority rule: If both combo_pack and partial_combo/single exist for same season, keep ONLY combo_pack. If partial_combo covers a range, do NOT also emit singles within that range.

Few-shot classification examples:
- "Episode 01-06 (480p, 720p, 1080p)" → ONE item: type=partial_combo, episode_range="01-06"
- "Season 1 Complete" → ONE item: type=combo_pack, episode_range=""
- "Episode 05" → ONE item: type=single_episode, episode_range="05"
- "EP41-EP48 [720p] [1080p]" → ONE item: type=partial_combo, episode_range="41-48" (NOT 8 singles!)

Other TV rules:
- `episode_range` required in every item. Zero-pad: "01", "01-08".
- Multi-season: separate season objects sorted by season_number. Only include seasons with download blocks.
- If same logical file repeats (mirrors), emit only one entry.
- poster_url: any absolute image URL is valid including third-party CDNs.

---
### TV SCHEMA:
{json.dumps(tvshow_schema, **_COMPACT)}"""


def build_combined_tv_duplicate_pre_schema(site: str, row_id_key: str) -> str:
    return f"""### RULES — TV duplicates
1. **Action** is decided ONLY by {site} search results (target site where content is uploaded).
   - {site} match found (same type + exact year + strong title) → `skip` / `update` / `replace` / `replace_items`.
   - No {site} match → `process`.
2. **`matched_task_id`** comes ONLY from DB Candidates (internal database, metadata only).
   - DB match found (same type + exact year + strong title) → `matched_task_id` = its integer `id`.
   - No DB match → `matched_task_id` = null.
   - DB match never changes the action.
3. **`{row_id_key}`** comes ONLY from {site} search results — never from DB Candidates.
4. Movie ≠ tvshow. Never cross-match types.

### WHEN {site} MATCH EXISTS — TV rows
Compare `Extracted` (from your extracted `data`) vs `Existing` (from matched {site} row):
- `Extracted` = `Existing` → `skip`.
- `Extracted` has items not in `Existing` → `update`. Fill `update_details`.
- Same coverage but higher source → `replace`. Source order: CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX.
- TV only: overlapping same-season replacement → `replace_items`.

### NORMALIZE — TV
Resolution keys: 480p, 720p, 1080p, 1440p, 2160p (4K→2160p). Ignore codecs.
Compare per exact season_number + episode_range + resolution. Never union across ranges.

### TV-SPECIFIC (duplicate behaviour)
- `has_new_episodes`=true only for explicit new episode labels/ranges.
- New range or season → `update`. Same range + missing resolutions → `update`.
- Same range + better source → `replace` or `replace_items`.
- Different seasons are additive; never replace another season.
- `replace_items` only when no combo/full-season pack is involved.

### REASON FORMAT
Single line: `Matched {site} row id=X.` or `No {site} match.`
Then: TitleCheck, YearCheck, Extracted:[...], Existing:[...], Missing:[...], Action: <action> because <why>.
If DB candidate also matched, append: `DB matched_task_id=Y.`

### OTHER FIELDS — TV
- `updated_website_title`: better stored title ending ` - {SITE_NAME}`, or `false`.
- `update_details` (only when action=update): `missing_items` array + `summary` string — one entry per season+episode_range with `season_number`, `episode_range`, `missing_resolutions`, `is_new_range`.

"""


def build_combined_tv_duplicate_examples(site: str, row_id_key: str) -> str:
    _ = row_id_key
    return f"""### EXAMPLES — TV

**EX-1: TV update — new episodes + missing resolution**
{site} match. Existing: S02 EP01-06 [720p,1080p]. Extracted: EP01-06 [480p,720p,1080p] + EP07-12 [720p,1080p].
→ action=`update`, has_new_episodes=true.
  `update_details`: {{"missing_items":[{{"season_number":2,"episode_range":"01-06","missing_resolutions":["480p"],"is_new_range":false}},{{"season_number":2,"episode_range":"07-12","missing_resolutions":["720p","1080p"],"is_new_range":true}}],"summary":"S02 EP01-06: need 480p; EP07-12: new range"}}

For skip / process / resolution-only `update` on one title row (no new episode ranges), apply the movie duplicate reasoning (same title+year coverage; skip if identical resolutions; update if new resolution keys).

"""


# ───────────────────────────────────────────────
# TV Show System Prompt (standalone — not used in combined)
# ───────────────────────────────────────────────

TVSHOW_SYSTEM_PROMPT = f"""You are a TV show data extraction function. Return ONLY valid JSON.

INPUT: Markdown (converted from HTML). Extract from headings, lists, link labels, and URLs.

RULES (in priority order):
1. Use only what is explicit in the Markdown. Never guess or invent.
2. Omit missing optional fields entirely (no null, no empty strings).
3. Strip blocked names from text fields: {_blocked_names_str}
4. Download URLs: copy exactly as written in Markdown link target. Never modify.
5. Never use watch/stream/player/preview/embed links as download entries.
6. Prefer x264 when multiple codec options exist.
7. One dual/multi-audio file = ONE entry with language array. Do not split.

TITLE: `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`
Combo season → `Season NN Complete`. Source = WEB-DL/NF/AMZN etc (not resolution).

DOWNLOAD TYPE DECISION TREE (follow strictly):
- Heading says a RANGE like "Episode 01-08" or "EP41-EP48" → partial_combo, episode_range="01-08" or "41-48"
- Heading says "Complete Season" or full season → combo_pack, episode_range=""
- Heading says exactly ONE episode → single_episode, episode_range="05"
COMMON MISTAKE: "Episode 41-48" = ONE partial_combo, NOT 8 single_episodes!
Priority: combo > partial > single. Never duplicate coverage.

FILE ENTRY: `{{"u":"URL","l":"Hindi","f":"Title.Year.S01E05.720p.WEB-DL.x264.{SITE_NAME}.mkv"}}`
Dual audio: `{{"u":"URL","l":["Hindi","English"],"f":"Title.Year.S01.Complete.Dual.Audio.720p.{SITE_NAME}.mkv"}}`

Schema: {json.dumps(tvshow_schema, **_COMPACT)}"""
