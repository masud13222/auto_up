import json
from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME
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
    base = "- Base: always include 480p, 720p, 1080p if present in the Markdown.\n"
    if not extra_below and not extra_above:
        return base + "- ONLY 480p/720p/1080p. No others.\n"
    parts = [base]
    if extra_below:
        parts.append("- Also include sub-720p non-standard (360p, 520p, etc.).\n")
    else:
        parts.append("- No sub-720p extras (only 480p).\n")
    if extra_above:
        parts.append("- Also include above 1080p (2160p, 4K).\n")
    else:
        parts.append("- No above-1080p.\n")
    if max_extra > 0:
        parts.append(f"- Max {max_extra} extra beyond base.\n")
    return "".join(parts)


def _build_duplicate_section(db_match_info: dict, flixbd_results: list = None) -> str:
    if not db_match_info and not flixbd_results:
        return ""

    ctx_parts = []
    if db_match_info:
        ctx_parts.append(f"### DB Match:\n```json\n{json.dumps(db_match_info, separators=(',',':'), ensure_ascii=False)}\n```")
    if flixbd_results:
        ctx_parts.append(
            f"### FlixBD Match (top {len(flixbd_results)}):\n"
            f"```json\n{json.dumps(flixbd_results, separators=(',',':'), ensure_ascii=False)}\n```\n"
            "(If no DB Match, compare only to FlixBD `resolution_keys`.)"
        )

    return f"""## Duplicate Check
{chr(10).join(ctx_parts)}

Compare sources: DB `existing_resolutions` and/or FlixBD `resolution_keys`. Both must be covered for "skip".
Do NOT use the page title line in the Markdown alone to determine what already exists on FlixBD/DB.

Steps:
1. Extracted = resolutions from your download_links with real URLs
2. Existing = from DB/FlixBD above
3. Missing = Extracted − Existing (check each one)
4. Decision: Missing empty → skip | Missing non-empty → update (set missing_resolutions) | Quality upgrade (CAM→WEB-DL) → replace | Different content → process

TV shows: also check new episodes. New eps → update (has_new_episodes=true).

reason format: "Extracted: [...]. Existing: [...]. Missing: [...]. Action: ... because ..."

```json
{json.dumps(duplicate_schema, **_COMPACT)}
```

"""


def get_combined_system_prompt(
    extra_below: bool = False,
    extra_above: bool = False,
    max_extra: int = 0,
    db_match_info: dict = None,
    flixbd_results: list = None,
) -> str:
    res_note = _build_resolution_note(extra_below, extra_above, max_extra)
    has_dup = bool(db_match_info or flixbd_results)
    dup_section = _build_duplicate_section(db_match_info, flixbd_results) if has_dup else ""

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
- Absolute URLs only; relative links → prepend the page domain.
- Download URLs only (generate.php gateways, real Download links). Never watch/stream/player/.m3u8 — omit that resolution.
- Do not decode, resolve, or alter URLs; keep gateway links intact.

## Title Format:
- Movie: `Title Year Source Language - {SITE_NAME}` (no Season/EP). Source = WEB-DL/CAMRip/HDRip/BluRay/WEBRip/HDTS (not resolution).
- TV: `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`. Combo → `Season NN Complete`.
Example movie: `Inception 2010 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`
Example TV: `Single Papa 2025 Season 01 EP01-06 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`

## is_adult:
Movie: true if Tagalog in title/heading (any case) OR explicit adult (18+/XXX/Adults only). false otherwise.
TV: true only for explicit adult. false for mainstream.

## SEO (required):
- meta_title: 50-60 chars, main keyword early, vary structure
- meta_description: 140-160 chars, natural CTA
- meta_keywords: 10-15 comma-separated

## download_filenames (required):
Keys MUST exactly match download_links (movie) or resolutions (TV item). Basename only — no `/` `\\` `:`.
Pattern (dots not spaces): `Title.Year.<segment>.<res>.<src>.WEB-DL.x264.{SITE_NAME}.<ext>`
- Movie segment: (none — just Title.Year.Res...)
- TV combo: S01.Complete | partial: S01E01-E08 | single: S01E05
- src: NF(Netflix) / AMZN(Amazon) / DSNP(Hotstar) / JC(Jio) / ZEE5 — if clearly in title; else omit extra src token
- ext: .mkv default; archives → match ext
Example: `War.Machine.2026.720p.NF.WEB-DL.x264.{SITE_NAME}.mkv`

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
{dup_section}
## Output:
{{"content_type":"movie" or "tvshow","data":{{...}}{',"duplicate_check":{{...}}' if has_dup else ''}}}
Return ONLY the JSON. Nothing else."""


# Backward compat — default: only standard resolutions
COMBINED_SYSTEM_PROMPT = get_combined_system_prompt()
