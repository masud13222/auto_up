"""Combined-prompt fragments for locked TV extraction + TV duplicate wording."""

from __future__ import annotations

from ..blocked_names import SITE_NAME
from ..json_encoding import json_compact
from ..tvshow_schema import tvshow_schema


def build_combined_tv_extract_body(core_rules_block: str, seo_block: str, res_note: str) -> str:
    site = SITE_NAME
    schema_json = json_compact(tvshow_schema)
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
{schema_json}"""


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
- `Existing` has empty `episodes_range` ([] or missing) → treat ALL extracted items as missing → `update`.

### REASON FORMAT
Single line: `Matched {site} row id=X.` or `No {site} match.`
Then: TitleCheck, YearCheck, Extracted:[...], Existing:[...], Missing:[...], Action: <action> because <why>.
If DB candidate also matched, append: `DB matched_task_id=Y.`

### OTHER FIELDS — TV
- `updated_website_title`: better stored title ending ` - {SITE_NAME}`, or `false`.
- `update_details` (only when action=update): `missing_items` array + `summary` string — one entry per season+episode_range with `season_number`, `episode_range`, `missing_resolutions`, `is_new_range`.

"""


def build_combined_tv_duplicate_examples(site: str, row_id_key: str) -> str:
    return f"""### EXAMPLES — TV

**EX-1: TV update — new episodes + missing resolution**
{site} match. Existing: S02 EP01-06 [720p,1080p]. Extracted: EP01-06 [480p,720p,1080p] + EP07-12 [720p,1080p].
→ action=`update`, has_new_episodes=true, {row_id_key}=210.
  `update_details`: {{"missing_items":[{{"season_number":2,"episode_range":"01-06","missing_resolutions":["480p"],"is_new_range":false}},{{"season_number":2,"episode_range":"07-12","missing_resolutions":["720p","1080p"],"is_new_range":true}}],"summary":"S02 EP01-06: need 480p; EP07-12: new range"}}

**EX-2: {site} match + DB match, missing resolution on same range → update**
{site}: [{{"id":441,"title":"Series Y 2023 Season 2 ...","download_links":{{"episodes_range":["S02 Episode 01-08: 720p,1080p"]}}}}]. DB: [{{"id":9002,"title":"Series Y","year":2023,"type":"tvshow"}}].
Extracted: S02 EP01-08 [480p,720p,1080p]. Existing:[720p,1080p]. Missing:[480p] for that range.
→ is_duplicate=true, {row_id_key}=441, matched_task_id=9002, action=`update`, has_new_episodes=false.
  `update_details`: {{"missing_items":[{{"season_number":2,"episode_range":"01-08","missing_resolutions":["480p"],"is_new_range":false}}],"summary":"S02 EP01-08: need 480p"}}

**EX-2b: {site} match exists but episodes_range is empty → update**

**EX-3: No {site} match, DB match → update**
{site} search results are unrelated titles only (no row with same tvshow + exact year + strong title match for extracted content). Example:
{site}: [{{"id":1,"title":"Other Show (2019)","download_links":{{"episodes_range":["S01 Episode 01-04: 1080p"]}}}},{{"id":2,"title":"Different Series (2020)","download_links":{{"episodes_range":[]}}}}].
DB: [{{"id":9100,"title":"New Show","year":2025,"type":"tvshow","episode_count":0,"episodes":[]}}].
Extracted: title "New Show", year 2025, seasons with download_items. **Action** follows {site} only → no qualifying {site} match → `update`. DB still records prior internal row → matched_task_id=9100.
→ is_duplicate=true, {row_id_key}=null, matched_task_id=9100, action=`update`, has_new_episodes=false, missing_resolutions=[].

For resolution-only `update` / `skip` on one title row (no new episode ranges beyond EX-1), apply the same comparison as movie duplicates (same title+year; skip if identical coverage; update if new resolution keys).

"""
