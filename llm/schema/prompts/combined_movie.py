"""Combined-prompt fragments for locked movie extraction + movie duplicate wording."""

from __future__ import annotations

from ..blocked_names import SITE_NAME
from ..json_encoding import json_compact
from ..movie_schema import movie_schema


def build_combined_movie_extract_body(core_rules_block: str, seo_block: str, res_note: str) -> str:
    site = SITE_NAME
    schema_json = json_compact(movie_schema)
    return f"""INPUT: Markdown (HTML→Markdown). This page is a **movie** (single film). Extract movie data only.

## EXTRACT (rules below, then schema)

{core_rules_block}

### RESOLUTION: {res_note}

### TITLES:
- Movie: `Title Year Source Language - {site}` (Source=WEB-DL/CAMRip/HDRip/BluRay/WEBRip/HDTS, not resolution).

{seo_block}

### FILE ENTRIES:
Each resolution value = list of objects: `{{"u":"URL","l":"Hindi","f":"BASENAME"}}`
Filename pattern (dots not spaces): `Title.Year.<segment>.<lang>.<res>.<src>.WEB-DL.x264.{site}.<ext>`
- Movie: Title.Year.Lang.Res.Src...
- Dual audio → use `Dual.Audio` in filename. Src: NF/AMZN/DSNP/JC/ZEE5 if clear, else omit. Default ext: .mkv.
- `f` = basename only (no / \\ :). Do not return separate `download_filenames` object.

---
### MOVIE SCHEMA:
{schema_json}"""


def build_combined_movie_duplicate_pre_schema(site: str, row_id_key: str) -> str:
    return f"""### RULES — movie duplicates
1. **Action** is decided ONLY by {site} search results (target site where content is uploaded).
   - {site} match found (same type + exact year + strong title) → `skip` / `update` / `replace`.
   - No {site} match → `process`.
2. **`matched_task_id`** comes ONLY from DB Candidates (internal database, metadata only).
   - DB match found (same type + exact year + strong title) → `matched_task_id` = its integer `id`.
   - No DB match → `matched_task_id` = null.
   - DB match never changes the action.
3. **`{row_id_key}`** comes ONLY from {site} search results — never from DB Candidates.
4. Movie ≠ tvshow. Never cross-match types.

### WHEN {site} MATCH EXISTS — movie rows
Compare `Extracted` (from your extracted `data`) vs `Existing` (from matched {site} row):
- `Extracted` = `Existing` → `skip`.
- `Extracted` has items not in `Existing` → `update`. Fill `update_details`.
- Same coverage but higher source → `replace`. Source order: CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX.

### NORMALIZE — movie
Resolution keys: 480p, 720p, 1080p, 1440p, 2160p (4K→2160p). Ignore codecs.

### REASON FORMAT
Single line: `Matched {site} row id=X.` or `No {site} match.`
Then: TitleCheck, YearCheck, Extracted:[...], Existing:[...], Missing:[...], Action: <action> because <why>.
If DB candidate also matched, append: `DB matched_task_id=Y.`

### OTHER FIELDS — movie
- `updated_website_title`: better stored title ending ` - {SITE_NAME}`, or `false`.
- `update_details` (only when action=update): one `missing_items` entry with `missing_resolutions`; `summary` one line.

"""


def build_combined_movie_duplicate_examples(site: str, row_id_key: str) -> str:
    return f"""### EXAMPLES — movie

**EX-1: {site} match, DB empty → skip**
{site}: [{{"id":1540,"title":"Movie X (1991)","download_links":{{"qualities":["480p","720p"]}}}}]. DB: [].
Extracted:[480p,720p]. {site} id=1540 matches. Existing:[480p,720p]. Missing:[].
→ is_duplicate=true, {row_id_key}=1540, matched_task_id=null, action=`skip`.

**EX-2: {site} match + DB match, missing resolution → update**
{site}: [{{"id":301,"title":"New Movie (2024)","download_links":{{"qualities":["720p","1080p"]}}}}]. DB: [{{"id":77,"title":"New Movie","year":2024,"type":"movie"}}].
Extracted:[480p,720p,1080p]. {site} id=301 matches (same title+year). Existing:[720p,1080p]. Missing:[480p].
→ is_duplicate=true, {row_id_key}=301, matched_task_id=77, action=`update`.
  `update_details`: {{"missing_items":[{{"missing_resolutions":["480p"]}}],"summary":"need 480p"}}

**EX-3: {site} match, missing resolution, DB empty → update**
{site}: [{{"id":218,"title":"Show (2023)","download_links":{{"qualities":["720p","1080p"]}}}}]. DB: [].
Extracted:[480p,720p,1080p]. {site} id=218 matches. Missing:[480p].
→ is_duplicate=true, {row_id_key}=218, matched_task_id=null, action=`update`.
  `update_details`: {{"missing_items":[{{"missing_resolutions":["480p"]}}],"summary":"need 480p"}}

"""
