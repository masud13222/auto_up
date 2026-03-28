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
    },
    "required": ["content_type", "data"],
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

    has_db = bool(db_match_candidates)
    no_db_rules = ""
    if not has_db and flixbd_results:
        no_db_rules = f"""
**No DB Candidates (only {site} rows above):**
- `matched_task_id` = **null** (no MediaTask row).
- `{TARGET_SITE_ROW_ID_JSON_KEY}` = the matching row's `id` from the JSON when title+year match; else null.
- Extracted = pure resolution keys from movie `data.download_links` or TV `resolutions`. Existing = that row's `resolution_keys`.
- Also inspect the matched row `title` for source tier.
- If the matched row clearly shows lower source (example: old `HDTC`, new `WEB-DL`), prefer **replace** even when `Existing` is empty or `Missing` is non-empty.
- Use **update** only for genuine add-missing-resolutions cases, not for clear low-source -> high-source upgrades.
- No title+year match → **process**.

"""
    db_rules = ""
    if has_db:
        db_rules = f"""
**DB Candidates present:**
- `matched_task_id` = DB candidate `id` only when you attach to that row; else null.
- `{TARGET_SITE_ROW_ID_JSON_KEY}` = matching {site} row `id` when you also identify a site row (same title+year); else null.
- Never swap the two id spaces.

"""

    return f"""## Duplicate Check
{chr(10).join(ctx_parts)}
{no_db_rules}{db_rules}
**Resolutions (strict):**
- Extract only normalized resolution tiers: `480p`, `720p`, `1080p`, `1440p`, `2160p` (`4K` -> `2160p`).
- If a clear tier appears without `p` (e.g. `720`), normalize to `720p`.
- Ignore codec tags: `x264`, `x265`, `HEVC`, `AAC`, `AVC`, `10bit`.
- Extracted = normalized keys from extracted `data` links.
- Existing = matched DB `resolutions` and/or matched {site} `resolution_keys`.
- If Extracted is empty -> default `action=process` unless duplicate evidence is overwhelming.
- Unknown resolution/quality token: treat as distinct; if unsure, include it in Missing.

**Source upgrade (replace only):**
- Run whenever title+year match and source tiers are visible, even if Missing is non-empty.
- Use source order: `CAM < HDCAM < HDTC < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX`
- Same resolutions but clearly higher source -> `replace`
- Lower old source in matched site row title -> higher new source also -> `replace`
- Same/lower/unclear source -> `skip`
- Never replace from codec alone.

Steps:
1. Match title+year to DB candidate (if any) and/or {site} row (if any).
2. Set **matched_task_id** and **`{TARGET_SITE_ROW_ID_JSON_KEY}`** per the two-field rules above.
3. If matched site row/title clearly shows lower source and new source is clearly higher -> **replace**.
4. Otherwise Missing non-empty -> **update**. Missing empty -> source-upgrade rule for **skip/replace**. No valid match -> **process**.

TV shows:
- `has_new_episodes=true` only when explicit higher episode numbers are visible.
- If episode numbers are unclear, set `has_new_episodes=false`.
- Use explicit `episode_range` when available:
  - genuinely NEW later episode range -> **update**
  - same covered range in a better pack form (single -> partial, partial -> combo, same range better source) -> **replace**
  - do not guess missing ranges from loose label text
 - if only the overlapping incoming TV items/range should replace old ones while the rest of the show stays untouched, use **`replace_items`** (TV only) instead of full **`replace`**
 - use **`replace_items`** only when the replace scope is explicit and no whole-season combo pack is involved; otherwise prefer full **`replace`**

Reason format:
- Single line only.
- Must start with `Matched candidate id=` or `No candidate matches title+year.`
- Must include `Extracted`, `Existing`, `Missing` lists even when empty, then `Action: ... because ...`

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
- Omit missing fields (no null, no empty strings).
- Remove blocked site names from every field: {_blocked_names_str}
- Prefer x264 encodes when multiple options exist.
- languages: array (e.g. ["Hindi","English"]). countries: array. cast / cast_info: comma-separated. Omit if absent.
- - Absolute URLs only; relative links → prepend the page domain.
- Download URLs only (generate.php gateways, real Download links). Never watch/stream/player/.m3u8 — omit that resolution.
- Strict link rule: never use Watch Online, Watch Resolution, watch link, watch generate link, stream, player, preview, embed, or similar watch-only URLs as download entries.
- Do not decode, resolve, or alter query strings or paths; keep gateway links intact.
- Blocked site name rule applies to TEXT FIELDS ONLY (title, filenames, etc.). Download URLs must be copied exactly as-is — even if the URL contains a blocked domain name.
- **Download / gateway URLs (strict):** Every movie `download_links.<resolution>[i].u` and TV `resolutions.<resolution>[i].u` value MUST be a valid absolute URL with a **complete hostname**.

## Title Format:
- Movie: `Title Year Source Language - {SITE_NAME}` (no Season/EP). Source = WEB-DL/CAMRip/HDRip/BluRay/WEBRip/HDTS (not resolution).
- TV: `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`. Combo → `Season NN Complete`. If one page contains multiple seasons, `website_tvshow_title` may summarize them as `Season 01-02 Complete`.
Example movie: `Inception 2010 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`
Example TV: `Single Papa 2025 Season 01 EP01-06 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`

## is_adult:
Movie: true if Tagalog in title/heading (any case) OR explicit adult (18+/XXX/Adults only). false otherwise.
TV: true only for explicit adult. false for mainstream.

## SEO (required):
- meta_title: 50-60 chars, main keyword early, vary structure
- meta_description: 140-160 chars, natural CTA
- meta_keywords: 10-15 comma-separated

## File Download Entries (required):
Movie `download_links` and TV item `resolutions` must use pure resolution keys only: `480p`, `720p`, `1080p`.
Each resolution value must be a list of per-file objects:
`[{{"u":"ABSOLUTE_URL","l":"Hindi","f":"BASENAME_ONLY"}},{{"u":"ABSOLUTE_URL","l":"English","f":"BASENAME_ONLY"}}]`
Do not return a separate `download_filenames` object for movie or TV when these fields are already inside each file entry.
`u`=url, `l`=language, `f`=filename. `f` is basename only — no `/` `\\` `:`.
Pattern (dots not spaces): `Title.Year.<segment>.<language>.<res>.<src>.WEB-DL.x264.{SITE_NAME}.<ext>`
- Movie segment: (none — just Title.Year.Language.Res...)
- TV combo: S01.Complete | partial: S01E01-E08 | single: S01E05
- src: NF(Netflix) / AMZN(Amazon) / DSNP(Hotstar) / JC(Jio) / ZEE5 — if clearly in title; else omit extra src token
- ext: .mkv default; archives → match ext
Example movie:
`"download_links":{{"720p":[{{"u":"https://...","l":"Hindi","f":"War.Machine.2026.Hindi.720p.NF.WEB-DL.x264.{SITE_NAME}.mkv"}},{{"u":"https://...","l":"English","f":"War.Machine.2026.English.720p.NF.WEB-DL.x264.{SITE_NAME}.mkv"}}]}}`

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
