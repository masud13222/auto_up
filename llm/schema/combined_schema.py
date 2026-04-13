import json
from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY
from .movie_schema import movie_schema
from .tvshow_schema import tvshow_schema
from .duplicate_schema import duplicate_schema

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)

_COMPACT = {"separators": (",", ":")}

# ───────────────────────────────────────────────
# Combined Schema: Auto-detect + Extract in ONE call
# ───────────────────────────────────────────────

combined_schema = {
    "type": "object",
    "properties": {
        "content_type": {
            "type": "string",
            "enum": ["movie", "tvshow"],
            "description": "Whether the content is a movie or TV show",
        },
        "data": {
            "type": "object",
            "description": "Extracted data following movie_schema or tvshow_schema",
        },
        "duplicate_check": duplicate_schema,
    },
    "required": ["content_type", "data"],
    "additionalProperties": False,
    "allOf": [
        {
            "if": {"properties": {"content_type": {"const": "movie"}}},
            "then": {"properties": {"data": movie_schema}},
        },
        {
            "if": {"properties": {"content_type": {"const": "tvshow"}}},
            "then": {"properties": {"data": tvshow_schema}},
        },
    ],
}


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
        ctx_parts.append(f"### DB Candidates ({len(db_match_candidates)}):\n```json\n{json.dumps(db_match_candidates, separators=(',',':'), ensure_ascii=False)}\n```")
    if flixbd_results:
        ctx_parts.append(
            f"### {site} search results (top {len(flixbd_results)}):\n"
            f"```json\n{json.dumps(flixbd_results, separators=(',',':'), ensure_ascii=False)}\n```\n"
            f"(Row `id` → use as `{TARGET_SITE_ROW_ID_JSON_KEY}`, never as `matched_task_id`.)"
        )

    return f"""
---
## DUPLICATE CHECK
{chr(10).join(ctx_parts)}

MATCHING:
- Match requires ALL THREE: same type + exact year + strong title match after trivial cleanup.
- Movie ≠ tvshow. Never cross-match.
- `matched_task_id` = copy one integer `id` from DB Candidates only, or null.
- `{TARGET_SITE_ROW_ID_JSON_KEY}` = copy one integer `id` from {site} search results only, or null.
- If title/year/type don't match any candidate → action=`process`.

NORMALIZE:
- Resolution keys: 480p, 720p, 1080p, 1440p, 2160p (4K→2160p). Ignore codecs (x264/x265/HEVC/AAC).
- `Extracted` = resolutions from your extracted `data`.
- `Existing` = resolutions/items from the matched DB candidate.
- `Missing` = in Extracted but not in Existing.
- For TV: compare per exact season_number + episode_range + resolution. Never union resolutions across different ranges.

ACTIONS:
- `process`: no confident match → `data` = full extraction.
- `skip`: same content, nothing new.
- `replace`: same coverage, better source → `data` = full extraction.
- `replace_items`: TV only, overlapping same-season replacement → `data.seasons` = replacement scope only.
- `update` (CRITICAL — delta only):
  * Movie: `data.download_links` = ONLY missing resolutions. Omit existing ones completely.
  * TV: `data.seasons` = ONLY missing items/resolutions. Compare each extracted item against Existing by season_number + episode_range. If fully covered → omit. If partially covered → keep only missing resolutions under that item. Never return full season data.

SOURCE ORDER: CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX.
Higher source for same coverage → `replace`. Never replace from codec alone.

TV-SPECIFIC:
- `has_new_episodes`=true only for explicit new episode labels/ranges.
- New later range or new season → `update` (delta only).
- Same range + missing resolutions only → `update`, return only those missing resolutions.
- Same range + better source → `replace` or `replace_items`.
- Different seasons are additive; never replace another season.
- `replace_items` only when no combo/full-season pack is involved.
- Never treat an aggregated title that summarizes many episodes as proof that every range/resolution is new.

REASON: single line. Start with `Matched candidate id=X.` or `No candidate matches title+year+type.`
Include: TitleCheck, YearCheck: new <N> vs candidate <M>, Extracted:[...], Existing:[...], Missing:[...], Action: <action> because <why>.

`updated_website_title` = better stored title ending ` - {SITE_NAME}`, or `false`.

```json
{json.dumps(duplicate_schema, **_COMPACT)}
```

### FEW-SHOT EXAMPLES (study these carefully):

**EX-1: TV update — missing resolution (delta only)**
Existing: S05, Episode 41-48, resolutions: 1080p, 720p.
Page: S05, Episode 41-48, resolutions: 480p, 720p, 1080p.
Analysis: 720p ✓ exists, 1080p ✓ exists, 480p ✗ missing.
Action: `update`. `data.seasons` = only the missing 480p:
```json
[{{"season_number":5,"download_items":[{{"type":"partial_combo","label":"Episode 41-48","episode_range":"41-48","resolutions":{{"480p":[{{"u":"https://example.com/s05e41-48-480p","l":"Hindi","f":"Show.2024.S05E41-E48.480p.WEB-DL.x264.{SITE_NAME}.mkv"}}]}}}}]}}]
```
WRONG output would include 720p and 1080p. Only 480p is correct.

**EX-2: TV update — new episode range added**
Existing: S02, Episode 01-06, resolutions: 1080p, 720p.
Page: S02, Episode 01-06 (480p, 720p, 1080p) AND Episode 07-12 (720p, 1080p).
Analysis: EP01-06 720p ✓, 1080p ✓, 480p ✗ missing. EP07-12 is entirely new.
Action: `update`. `data.seasons` = missing 480p for 01-06 + full 07-12:
```json
[{{"season_number":2,"download_items":[{{"type":"partial_combo","label":"Episode 01-06","episode_range":"01-06","resolutions":{{"480p":[{{"u":"https://example.com/s02e01-06-480p","l":"Hindi","f":"Show.2024.S02E01-E06.480p.WEB-DL.x264.{SITE_NAME}.mkv"}}]}}}},{{"type":"partial_combo","label":"Episode 07-12","episode_range":"07-12","resolutions":{{"720p":[{{"u":"https://example.com/s02e07-12-720p","l":"Hindi","f":"Show.2024.S02E07-E12.720p.WEB-DL.x264.{SITE_NAME}.mkv"}}],"1080p":[{{"u":"https://example.com/s02e07-12-1080p","l":"Hindi","f":"Show.2024.S02E07-E12.1080p.WEB-DL.x264.{SITE_NAME}.mkv"}}]}}}}]}}]
```
EP01-06 returns ONLY the missing 480p. EP07-12 is new so all resolutions included.

**EX-3: TV skip — everything already exists**
Existing: S01, Episode 01-10, resolutions: 480p, 720p, 1080p.
Page: S01, Episode 01-10, resolutions: 720p, 1080p.
Analysis: 720p ✓, 1080p ✓. Page has nothing new.
Action: `skip`. `data.seasons` = full extraction (skip means no delta needed).

**EX-4: Movie update — missing resolution only**
Existing movie resolutions: 1080p, 720p.
Page: 480p, 720p, 1080p, 2160p.
Analysis: 720p ✓, 1080p ✓, 480p ✗ missing, 2160p ✗ missing (if above-1080p ON).
Action: `update`. `data.download_links` = only 480p (and 2160p if policy allows):
```json
{{"480p":[{{"u":"https://example.com/movie-480p","l":"Hindi","f":"Movie.2024.Hindi.480p.WEB-DL.x264.{SITE_NAME}.mkv"}}]}}
```
WRONG output would include 720p and 1080p. Only missing resolutions are correct.

"""


def get_combined_system_prompt(
    extra_below: bool = False,
    extra_above: bool = False,
    max_extra: int = 0,
    db_match_candidates: list = None,
    flixbd_results: list = None,
) -> str:
    res_note = _build_resolution_note(extra_below, extra_above, max_extra)
    has_dup = bool(db_match_candidates or flixbd_results)
    dup_section = _build_duplicate_section(db_match_candidates, flixbd_results) if has_dup else ""

    return f"""You are a structured data extraction function. Detect content type AND extract data. Return ONLY valid JSON.

INPUT: Markdown (HTML→Markdown). Use headings, lists, link labels, and link URLs.

## STEP 1 — DETECT
TV signs: Season, Episode, S01, E01, Complete Season, Web Series → "tvshow". Otherwise → "movie".

## STEP 2 — EXTRACT (rules below, then schema)

### CORE RULES (follow strictly, in this order):
1. Return ONLY valid JSON. No markdown fences, no extra text.
2. Use only what is explicit in the Markdown. Never guess. Omit missing fields (no null, no empty strings).
3. Download URLs: copy each URL exactly as written in the Markdown link target. Do not shorten, decode, rebuild, or alter in any way.
4. Never use watch/stream/player/preview/embed links as download entries — only real download/gateway URLs.
5. URL must be absolute with complete hostname. Relative → prepend page domain.
6. Strip blocked site names from TEXT fields only (title, filenames): {_blocked_names_str}. URLs: copy as-is even if they contain a blocked name.
7. One dual/multi-audio file = ONE entry with `l` as array. Do not split same file into separate language entries.
8. If same resolution has both dual-audio and single-language files, keep only dual-audio.
9. Prefer x264 when multiple codec options exist.
10. Never invent season numbers, episode ranges, or resolution keys not shown on page.

### RESOLUTION: {res_note}

### TITLES:
- Movie: `Title Year Source Language - {SITE_NAME}` (Source=WEB-DL/CAMRip/HDRip/BluRay/WEBRip/HDTS, not resolution).
- TV: `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`. Combo → `Season NN Complete`.

### SEO:
- meta_title: 50-60 chars. meta_description: 140-160 chars, natural CTA. meta_keywords: 10-15 comma-separated.

### FILE ENTRIES:
Each resolution value = list of objects: `{{"u":"URL","l":"Hindi","f":"BASENAME"}}`
Filename pattern (dots not spaces): `Title.Year.<segment>.<lang>.<res>.<src>.WEB-DL.x264.{SITE_NAME}.<ext>`
- Movie: Title.Year.Lang.Res.Src...
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
### MOVIE SCHEMA:
{json.dumps(movie_schema, **_COMPACT)}

### TV SCHEMA:
{json.dumps(tvshow_schema, **_COMPACT)}
{dup_section}
## OUTPUT:
{{"content_type":"movie" or "tvshow","data":{{...}}{',"duplicate_check":{{...}}' if has_dup else ''}}}
Return ONLY the JSON."""


# Backward compat — default: only standard resolutions
COMBINED_SYSTEM_PROMPT = get_combined_system_prompt()
