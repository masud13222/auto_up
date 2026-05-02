import json

from .blocked_names import SITE_NAME, BLOCKED_SITE_NAMES

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)

_COMPACT = {"separators": (",", ":")}

# ───────────────────────────────────────────────
# Movie Info Schema
# ───────────────────────────────────────────────

movie_schema = {
    "type": "object",
    "properties": {
        "website_movie_title": {
            "type": "string",
            "description": f"Formatted title ending with ' - {SITE_NAME}'",
        },
        "title": {"type": "string", "description": "Clean movie name only (no year/quality/language)"},
        "year": {"type": "integer"},
        "genre": {"type": "string"},
        "director": {"type": "string"},
        "rating": {"type": "number", "description": "Numeric only (7.5)"},
        "plot": {"type": "string"},
        "poster_url": {
            "type": "string",
            "description": "Absolute poster/image URL",
        },
        "meta_title": {"type": "string", "description": "SEO title 50-60 chars"},
        "meta_description": {"type": "string", "description": "Meta desc 140-160 chars"},
        "meta_keywords": {"type": "string", "description": "10-15 comma-separated"},
        "download_links": {
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
        "cast": {"type": "string", "description": "Comma-separated actors"},
        "languages": {"type": "array", "items": {"type": "string"}},
        "countries": {"type": "array", "items": {"type": "string"}},
        "imdb_id": {"type": "string"},
        "tmdb_id": {"type": "string"},
        "is_adult": {
            "type": "boolean",
            "description": "true if Tagalog in title OR explicit adult (18+/XXX/erotic). false otherwise.",
        },
    },
    "required": ["website_movie_title", "title", "year", "is_adult", "download_links"],
    "additionalProperties": False,
}


def build_combined_movie_extract_body(core_rules_block: str, seo_block: str, res_note: str) -> str:
    site = SITE_NAME
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
{json.dumps(movie_schema, **_COMPACT)}"""


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

**EX-2: No {site} match, DB match → process**
{site}: [{{"id":300,"title":"Different Movie (2018)"}}]. DB: [{{"id":77,"title":"New Movie","year":2024,"type":"movie"}}].
Extracted:"New Movie" 2024. No {site} match → action=`process`. DB id=77 matches → matched_task_id=77.
→ is_duplicate=false, {row_id_key}=null, matched_task_id=77, action=`process`.

**EX-3: {site} match, missing resolution → update**
{site}: [{{"id":218,"title":"Show (2023)","download_links":{{"qualities":["720p","1080p"]}}}}]. DB: [].
Extracted:[480p,720p,1080p]. {site} id=218 matches. Missing:[480p].
→ is_duplicate=true, {row_id_key}=218, matched_task_id=null, action=`update`.
  `update_details`: {{"missing_items":[{{"missing_resolutions":["480p"]}}],"summary":"need 480p"}}

"""


# Standalone movie prompt — used only when NOT calling combined.
MOVIE_SYSTEM_PROMPT = f"""You are a movie data extraction function. Return ONLY valid JSON.

INPUT: Markdown (converted from HTML). Extract from headings, lists, link labels, and URLs.

RULES (in priority order):
1. Use only what is explicit in the Markdown. Never guess or invent.
2. Omit missing optional fields entirely (no null, no empty strings).
3. Strip blocked names from text fields: {_blocked_names_str}
4. Download URLs: copy exactly as written in Markdown link target. Never modify.
5. Never use watch/stream/player/preview/embed links as download entries.
6. Prefer x264 when multiple codec options exist.
7. One dual/multi-audio file = ONE entry with language array. Do not split.

TITLE: `Title Year Source Language - {SITE_NAME}` (Source = WEB-DL/CAMRip/HDRip/BluRay, not resolution).

FILE ENTRY: `{{"u":"URL","l":"Hindi","f":"Title.Year.Hindi.480p.WEB-DL.x264.{SITE_NAME}.mkv"}}`
Dual audio: `{{"u":"URL","l":["Hindi","English"],"f":"Title.Year.Dual.Audio.720p.WEB-DL.x264.{SITE_NAME}.mkv"}}`

Schema: {json.dumps(movie_schema, **_COMPACT)}"""
