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
    parts = ["- Base: 480p, 720p, 1080p always included when present in the Markdown.\n"]
    if extra_below:
        parts.append("- Below 720p: enabled. Include 520p, 360p, 240p and similar if present.\n")
    else:
        parts.append("- Below 720p: disabled. Do not include extra tiers below 480p/720p base set.\n")
    if extra_above:
        parts.append("- Above 1080p: enabled. Include 2160p / 4K and similar if present.\n")
    else:
        parts.append("- Above 1080p: disabled. Do not include tiers above 1080p.\n")
    if max_extra > 0:
        parts.append(f"- Max Extra Resolutions: {max_extra}. This limits extra tiers beyond 480p/720p/1080p.\n")
    else:
        parts.append("- Max Extra Resolutions: 0 means unlimited extras beyond 480p/720p/1080p.\n")
    return "".join(parts)


def _build_duplicate_section(db_match_candidates: list = None, flixbd_results: list = None) -> str:
    if not db_match_candidates and not flixbd_results:
        return ""

    site = SITE_NAME
    ctx_parts = []
    if db_match_candidates:
        ctx_parts.append(f"### DB Candidates ({len(db_match_candidates)}):\n```json\n{json.dumps(db_match_candidates, separators=(',',':'), ensure_ascii=False)}\n```")
    if flixbd_results:
        ctx_parts.append(
            f"### {site} (target site) search results (top {len(flixbd_results)}):\n"
            f"```json\n{json.dumps(flixbd_results, separators=(',',':'), ensure_ascii=False)}\n```\n"
            f"(Each row `id` → use as **`{TARGET_SITE_ROW_ID_JSON_KEY}`** when that row matches; never as **matched_task_id**.)"
        )

    return f"""## Duplicate Check
{chr(10).join(ctx_parts)}
Rules:
- Non-null `matched_task_id` = copy one id from DB Candidates only; otherwise null.
- Non-null `{TARGET_SITE_ROW_ID_JSON_KEY}` = copy one id from {site} search JSON only; otherwise null.
- Valid match requires all three: same detected type, exact year, and strong title match after trivial cleanup only.
- Never match movie vs tvshow.
- Use candidate `website_title` / matched {site} row title for season and source clues.
- If title/year/type do not match, action=`process`.

Normalize:
- Resolution keys: `480p`, `720p`, `1080p`, `1440p`, `2160p` (`4K` -> `2160p`).
- Ignore codec-only tokens such as `x264`, `x265`, `HEVC`, `AAC`, `AVC`, `10bit`.
- `Extracted` = keys from final `data`.
- `Existing` = matched DB movie `resolutions`, matched DB TV `tv_items` / `episodes`, matched {site} row `download_links`.
- `Missing` = resolutions present in `Extracted` but absent from `Existing`.

Action:
- `skip`: same coverage, nothing new, no clear upgrade.
- `update`: only new/missing part should be added.
- `replace`: same coverage, clearly better source.
- `replace_items`: TV only; explicit overlapping same-season replacement scope.
- `process`: no confident match.
- Source order: `CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX`.
- Higher source for same coverage -> `replace`. Never replace from codec alone.

Output shaping:
- `process` or `replace`: `data` = full extracted page content.
- Movie `update`: `data.download_links` must contain only missing/new files. Omit already-existing resolutions completely.
- TV `update`: `data.seasons` must contain only the season/item/range/resolution that needs appending. Omit unchanged old items.
- TV `replace_items`: `data.seasons` must contain only the overlapping replacement scope. Omit untouched items.
- If an existing TV item already exists and only one resolution is missing, return only that missing resolution under that item.
- `updated_website_title` = better stored title only; otherwise `false`.
- When the Duplicate Check section is present, include `duplicate_check` in the final JSON.

TV rules:
- `has_new_episodes=true` only for explicit later/new episode labels or ranges.
- New later range or new season -> `update`.
- Same range with better pack/source -> `replace` or `replace_items`.
- Different seasons are additive; do not replace another season.
- Use `replace_items` only when no combo/full-season pack is involved; otherwise use `replace`.

Reason:
- Single line only.
- Must start with `Matched candidate id=` or `No candidate matches title+year+type.`
- Must include `TitleCheck`, `YearCheck: new_year <N> vs candidate <M> -> ...`, `Extracted`, `Existing`, `Missing`, and `Action: ... because ...`.

```json
{json.dumps(duplicate_schema, **_COMPACT)}
```

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

    return f"""You are an expert web scraping assistant. Detect content type AND extract structured data in one step.

**Input format:** The user message is **Markdown** (the article page was converted HTML→Markdown). Use headings, lists, link labels, and link URLs from that Markdown — not raw HTML.

## Step 1: Detect (from the Markdown)
- TV show signs: Season, Episode, S01, E01, Complete Season, Web Series, episode listings → "tvshow"
- Otherwise → "movie"

## Step 2: Extract (schema below)

---

## Resolution Rules (applies to BOTH movie and tvshow):
{res_note}
---

## Common Rules:
- Return ONLY valid JSON. No markdown, no extra text.
- Use only what is explicit in the Markdown. If missing, omit. Never guess.
- Omit missing fields (no null, no empty strings).
- Remove blocked site names from every field: {_blocked_names_str}
- Prefer x264 encodes when multiple options exist.
- languages: array (e.g. ["Hindi","English"]). countries: array. cast / cast_info: comma-separated. Omit if absent.
- Absolute URLs only; relative links → prepend the page domain.
- `poster_url`: any absolute direct image URL is valid, including third-party hosts/CDNs.
- Download URLs only (generate.php gateways, real Download links). Never watch/stream/player/.m3u8/Watch Resulation/Online Stream — omit that resolution.
- Strict link rule: never use Watch Online, Watch Resolution, watch link, watch generate link, stream, player, preview, embed, or similar watch-only URLs as download entries.
- Return only absolute direct download URLs. use each URL exactly as written in the Markdown link target (inside parentheses) without any modification.
- Blocked site name rule applies to TEXT FIELDS ONLY (title, filenames, etc.). Download URLs must be copied exactly as-is — even if the URL contains a blocked domain name.
- **Download / gateway URLs (strict):** Every movie `download_links.<resolution>[i].u` and TV `resolutions.<resolution>[i].u` value MUST be a valid absolute URL with a **complete hostname**.

## Title Format:
- Movie: `Title Year Source Language - {SITE_NAME}` (no Season/EP). Source = WEB-DL/CAMRip/HDRip/BluRay/WEBRip/HDTS (not resolution).
- TV: `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`. Combo → `Season NN Complete`. If one page contains multiple seasons, `website_tvshow_title` may summarize them as `Season 01-02 Complete`.
Example movie: `Inception 2010 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`
Example TV: `Single Papa 2025 Season 01 EP01-06 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`

## SEO (required):
- meta_title: 50-60 chars, main keyword early, vary structure
- meta_description: 140-160 chars, natural CTA
- meta_keywords: 10-15 comma-separated

## File Download Entries (required):
Movie `download_links` and TV item `resolutions` must use pure resolution keys only: `480p`, `720p`, `1080p`.
Never invent season numbers, episode ranges, or resolution keys not clearly shown by the page.
Each resolution value must be a list of per-file objects:
`[{{"u":"ABSOLUTE_URL","l":"Hindi","f":"BASENAME_ONLY"}}]`
If one downloadable file contains multiple audio tracks, return ONE file object only:
`[{{"u":"ABSOLUTE_URL","l":["Hindi","English"],"f":"Title.Year.Dual.Audio.720p.WEB-DL.x264.{SITE_NAME}.mkv"}}]`
Do not split one dual/multi-audio file into separate Hindi/English entries when the URL/file is the same.
If the same resolution shows both a dual/multi-audio file and a separate single-language file, keep only the dual/multi-audio file.
Only create separate entries when the page clearly provides separate downloadable files per language.
Do not return a separate `download_filenames` object for movie or TV when these fields are already inside each file entry.
`u`=url, `l`=language string or language array, `f`=filename. `f` is basename only — no `/` `\\` `:`.
Pattern (dots not spaces): `Title.Year.<segment>.<language_or_audio_tag>.<res>.<src>.WEB-DL.x264.{SITE_NAME}.<ext>`
- Movie segment: (none — just Title.Year.Language.Res...)
- TV combo: S01.Complete | partial: S01E01-E08 | single: S01E05
- Use `Dual.Audio` / `Multi.Audio` in filename when one file contains multiple audio tracks.
- src: NF(Netflix) / AMZN(Amazon) / DSNP(Hotstar) / JC(Jio) / ZEE5 — if clearly in title; else omit extra src token
- ext: .mkv default; archives → match ext
Example movie:
`"download_links":{{"720p":[{{"u":"https://...","l":["Hindi","English"],"f":"War.Machine.2026.Dual.Audio.720p.NF.WEB-DL.x264.{SITE_NAME}.mkv"}}]}}`

---

## IF movie — schema:
{json.dumps(movie_schema, **_COMPACT)}

---

## IF tvshow — schema:
{json.dumps(tvshow_schema, **_COMPACT)}

### TV Download Item Types (classify by Markdown structure):
- combo_pack: one section covers the entire season (no per-episode breakdown in that block)
- partial_combo: section label shows an episode NUMBER RANGE (Ep X-Y). Set episode_range (zero-padded).
- single_episode: section = exactly one episode. Set episode_range (zero-padded).
Decision: whole season→combo | range→partial | one ep→single.
Button count does NOT affect type. Never merge separate episodes into range. Never split range.
Priority: combo present→only combo. Partial covers range→no singles in that range.
- `episode_range` is required in every TV item.
- Use `episode_range=""` only for a true whole-season `combo_pack` with no explicit range.
Multi-season extraction rules:
- If the page shows multiple explicit season headings/labels, output multiple objects in `data.seasons` with the real `season_number` for each one.
- Create a season object only when that season has its own explicit download block/label/heading in the Markdown. Do not infer seasons from title text, metadata, or `total_seasons` alone.
- Group each link under the nearest matching season block and use the real `season_number` for that block.
- If only some seasons have downloadable blocks, return only those seasons.
- Never mix links from different seasons in one season object. Keep `data.seasons` sorted by `season_number`.
- `total_seasons` may reflect the show's real total only when clearly stated by the page/metadata. Omit if unclear.
- If the same logical file repeats (same season, item/range, language, quality, and filename), treat it as mirror links for one file and emit only one entry with one preferred URL.
{dup_section}
## Output:
{{"content_type":"movie" or "tvshow","data":{{...}}{',"duplicate_check":{{...}}' if has_dup else ''}}}
Return ONLY the JSON. Nothing else."""


# Backward compat — default: only standard resolutions
COMBINED_SYSTEM_PROMPT = get_combined_system_prompt()
