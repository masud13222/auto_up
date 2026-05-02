import json
from typing import Literal

from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY
from .duplicate_schema import duplicate_schema
from .movie_schema import movie_schema
from .tvshow_schema import tvshow_schema

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)
_COMPACT = {"separators": (",", ":")}


def _build_resolution_note(extra_below: bool = False, extra_above: bool = False, max_extra: int = 0) -> str:
    parts = ["Base: 480p, 720p, 1080p always included when present."]
    if extra_below:
        parts.append("Below 720p: ON (include 520p, 360p, 240p etc).")
    else:
        parts.append("Below 720p: OFF.")
    if extra_above:
        parts.append("Above 1080p: ON (include 2160p/4K).")
    else:
        parts.append("Above 1080p: OFF.")
    if max_extra > 0:
        parts.append(f"Max extras beyond base: {max_extra}.")
    return " ".join(parts)


def _build_duplicate_section(db_match_candidates: list = None, flixbd_results: list = None) -> str:
    if not db_match_candidates and not flixbd_results:
        return ""

    site = SITE_NAME
    ctx_parts = []
    if db_match_candidates:
        ctx_parts.append(
            f"### DB Candidates ({len(db_match_candidates)}):\n```json\n"
            f"{json.dumps(db_match_candidates, separators=(',', ':'), ensure_ascii=False)}\n```"
        )
    if flixbd_results:
        ctx_parts.append(
            f"### {site} search results (top {len(flixbd_results)}):\n"
            f"```json\n{json.dumps(flixbd_results, separators=(',', ':'), ensure_ascii=False)}\n```\n"
            f"(Row `id` → use as `{TARGET_SITE_ROW_ID_JSON_KEY}`, never as `matched_task_id`.)"
        )

    return f"""
---
## DUPLICATE CHECK

{chr(10).join(ctx_parts)}

### RULES
1. **Action** is decided ONLY by {site} search results (target site where content is uploaded).
   - {site} match found (same type + exact year + strong title) → `skip` / `update` / `replace` / `replace_items`.
   - No {site} match → `process`.
2. **`matched_task_id`** comes ONLY from DB Candidates (internal database, metadata only).
   - DB match found (same type + exact year + strong title) → `matched_task_id` = its integer `id`.
   - No DB match → `matched_task_id` = null.
   - DB match never changes the action.
3. **`{TARGET_SITE_ROW_ID_JSON_KEY}`** comes ONLY from {site} search results — never from DB Candidates.
4. Movie ≠ tvshow. Never cross-match types.

### WHEN {site} MATCH EXISTS
Compare `Extracted` (from your extracted `data`) vs `Existing` (from matched {site} row):
- `Extracted` = `Existing` → `skip`.
- `Extracted` has items not in `Existing` → `update`. Fill `update_details`.
- Same coverage but higher source → `replace`. Source order: CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX.
- TV only, overlapping same-season replacement → `replace_items`.

### NORMALIZE
Resolution keys: 480p, 720p, 1080p, 1440p, 2160p (4K→2160p). Ignore codecs.
For TV: compare per exact season_number + episode_range + resolution. Never union across ranges.

### TV-SPECIFIC
- `has_new_episodes`=true only for explicit new episode labels/ranges.
- New range or season → `update`. Same range + missing resolutions → `update`.
- Same range + better source → `replace` or `replace_items`.
- Different seasons are additive; never replace another season.
- `replace_items` only when no combo/full-season pack is involved.

### REASON FORMAT
Single line: `Matched {site} row id=X.` or `No {site} match.`
Then: TitleCheck, YearCheck, Extracted:[...], Existing:[...], Missing:[...], Action: <action> because <why>.
If DB candidate also matched, append: `DB matched_task_id=Y.`

### OTHER FIELDS
- `updated_website_title`: better stored title ending ` - {SITE_NAME}`, or `false`.
- `update_details` (only when action=update): `missing_items` array + `summary` string.
  - Movie: one entry with `missing_resolutions`.
  - TV: one entry per season+episode_range — include `season_number`, `episode_range`, `missing_resolutions`, `is_new_range`.

```json
{json.dumps(duplicate_schema, **_COMPACT)}
```

### EXAMPLES

**EX-1: {site} match, DB empty → skip**
{site}: [{{"id":1540,"title":"Movie X (1991)","download_links":{{"qualities":["480p","720p"]}}}}]. DB: [].
Extracted:[480p,720p]. {site} id=1540 matches. Existing:[480p,720p]. Missing:[].
→ is_duplicate=true, {TARGET_SITE_ROW_ID_JSON_KEY}=1540, matched_task_id=null, action=`skip`.

**EX-2: No {site} match, DB match → process**
{site}: [{{"id":300,"title":"Different Movie (2018)"}}]. DB: [{{"id":77,"title":"New Movie","year":2024,"type":"movie"}}].
Extracted:"New Movie" 2024. No {site} match → action=`process`. DB id=77 matches → matched_task_id=77.
→ is_duplicate=false, {TARGET_SITE_ROW_ID_JSON_KEY}=null, matched_task_id=77, action=`process`.

**EX-3: {site} match, missing resolution → update**
{site}: [{{"id":218,"title":"Show (2023)","download_links":{{"qualities":["720p","1080p"]}}}}]. DB: [].
Extracted:[480p,720p,1080p]. {site} id=218 matches. Missing:[480p].
→ is_duplicate=true, {TARGET_SITE_ROW_ID_JSON_KEY}=218, matched_task_id=null, action=`update`.
  `update_details`: {{"missing_items":[{{"missing_resolutions":["480p"]}}],"summary":"need 480p"}}

**EX-4: TV update — new episodes + missing resolution**
{site} match. Existing: S02 EP01-06 [720p,1080p]. Extracted: EP01-06 [480p,720p,1080p] + EP07-12 [720p,1080p].
→ action=`update`, has_new_episodes=true.
  `update_details`: {{"missing_items":[{{"season_number":2,"episode_range":"01-06","missing_resolutions":["480p"],"is_new_range":false}},{{"season_number":2,"episode_range":"07-12","missing_resolutions":["720p","1080p"],"is_new_range":true}}],"summary":"S02 EP01-06: need 480p; EP07-12: new range"}}

"""


def _core_rules_block() -> str:
    return f"""### CORE RULES (follow strictly, in this order):
1. Return ONLY valid JSON. No markdown fences, no extra text.
2. Use only what is explicit in the Markdown. Never guess. Omit missing fields (no null, no empty strings).
3. Download URLs: copy each URL exactly as written in the Markdown link target. Do not shorten, decode, rebuild, or alter in any way.
4. Never use watch/stream/player/preview/embed links as download entries — only real download/gateway URLs.
5. URL must be absolute with complete hostname. Relative → prepend page domain.
6. Strip blocked site names from TEXT fields only (title, filenames): {_blocked_names_str}. URLs: copy as-is even if they contain a blocked name.
7. One dual/multi-audio file = ONE entry with `l` as array. Do not split same file into separate language entries.
8. If same resolution has both dual-audio and single-language files, keep only dual-audio.
9. Prefer x264 when multiple codec options exist.
10. Never invent season numbers, episode ranges, or resolution keys not shown on page."""


def _seo_block() -> str:
    return """### SEO:
- meta_title: 50-60 chars. meta_description: 140-160 chars, natural CTA. meta_keywords: 10-15 comma-separated."""


def _movie_body(res_note: str, site: str) -> str:
    return f"""INPUT: Markdown (HTML→Markdown). This page is a **movie** (single film). Extract movie data only.

## EXTRACT (rules below, then schema)

{_core_rules_block()}

### RESOLUTION: {res_note}

### TITLES:
- Movie: `Title Year Source Language - {site}` (Source=WEB-DL/CAMRip/HDRip/BluRay/WEBRip/HDTS, not resolution).

{_seo_block()}

### FILE ENTRIES:
Each resolution value = list of objects: `{{"u":"URL","l":"Hindi","f":"BASENAME"}}`
Filename pattern (dots not spaces): `Title.Year.<segment>.<lang>.<res>.<src>.WEB-DL.x264.{site}.<ext>`
- Movie: Title.Year.Lang.Res.Src...
- Dual audio → use `Dual.Audio` in filename. Src: NF/AMZN/DSNP/JC/ZEE5 if clear, else omit. Default ext: .mkv.
- `f` = basename only (no / \\ :). Do not return separate `download_filenames` object.

---
### MOVIE SCHEMA:
{json.dumps(movie_schema, **_COMPACT)}"""


def _tv_body(res_note: str, site: str) -> str:
    return f"""INPUT: Markdown (HTML→Markdown). This page is a **TV show** (series). Extract TV data only.

## EXTRACT (rules below, then schema)

{_core_rules_block()}

### RESOLUTION: {res_note}

### TITLES:
- TV: `Title Year Season NN EPxx[-yy] Source Language - {site}`. Combo → `Season NN Complete`.

{_seo_block()}

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


def get_combined_system_prompt(
    locked_content_type: Literal["movie", "tvshow"],
    extra_below: bool = False,
    extra_above: bool = False,
    max_extra: int = 0,
    db_match_candidates: list = None,
    flixbd_results: list = None,
) -> str:
    res_note = _build_resolution_note(extra_below, extra_above, max_extra)
    has_dup = bool(db_match_candidates or flixbd_results)
    dup_section = _build_duplicate_section(db_match_candidates, flixbd_results) if has_dup else ""

    if locked_content_type == "movie":
        body = _movie_body(res_note, SITE_NAME)
        if has_dup:
            output_line = '{"content_type":"movie","data":{...},"duplicate_check":{...}}'
        else:
            output_line = '{"content_type":"movie","data":{...}}'
        intro = "You are a structured data extraction function. Extract **movie** metadata and download links. Return ONLY valid JSON. `content_type` must be exactly `\"movie\"`."
    else:
        body = _tv_body(res_note, SITE_NAME)
        if has_dup:
            output_line = '{"content_type":"tvshow","data":{...},"duplicate_check":{...}}'
        else:
            output_line = '{"content_type":"tvshow","data":{...}}'
        intro = "You are a structured data extraction function. Extract **TV show** metadata and download links. Return ONLY valid JSON. `content_type` must be exactly `\"tvshow\"`."

    return f"""{intro}

{body}
{dup_section}
## OUTPUT:
{output_line}
Return ONLY the JSON."""


COMBINED_SYSTEM_PROMPT = get_combined_system_prompt(locked_content_type="movie")
