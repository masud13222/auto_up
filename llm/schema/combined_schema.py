import json
from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME
from .movie_schema import movie_schema
from .tvshow_schema import tvshow_schema
from .duplicate_schema import duplicate_schema

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)


# ───────────────────────────────────────────────
# Combined Schema: Auto-detect + Extract in ONE call
# ───────────────────────────────────────────────

combined_schema = {
    "type": "object",
    "properties": {
        "content_type": {
            "type": "string",
            "enum": ["movie", "tvshow"],
            "description": "Whether the content is a movie or TV show"
        },
        "data": {
            "type": "object",
            "description": "Extracted content data — follows movie_schema if content_type='movie', tvshow_schema if content_type='tvshow'"
        }
    },
    "required": ["content_type", "data"]
}


def _build_resolution_note(extra_below: bool = False, extra_above: bool = False, max_extra: int = 0) -> str:
    """Build the resolution constraint text for the LLM prompt."""
    base = "- **Base resolutions**: Always include 480p, 720p, and 1080p if available on the page.\n"

    if not extra_below and not extra_above:
        return base + "- **ONLY** include 480p, 720p, 1080p. Do NOT include any other resolutions.\n"

    parts = [base]

    if extra_below:
        parts.append("- Also include any NON-STANDARD resolutions below 720p if available (e.g. 520p, 360p, 240p). Note: 480p is already in the base.\n")
    else:
        parts.append("- Do NOT include non-standard resolutions below 720p (no 520p, 360p, 240p, etc.). Only 480p from sub-720p range.\n")

    if extra_above:
        parts.append("- Also include resolutions ABOVE 1080p if available (e.g. 2160p, 4K).\n")
    else:
        parts.append("- Do NOT include resolutions above 1080p (no 2160p, 4K, etc.).\n")

    if max_extra > 0:
        parts.append(f"- Include at most {max_extra} extra resolution(s) beyond the base 480p/720p/1080p.\n")

    return "".join(parts)


def _build_duplicate_section(db_match_info: dict, flixbd_results: list = None) -> str:
    """Build the duplicate check section for combined prompt.
    Includes both DB match info and FlixBD search results as context.
    """
    if not db_match_info and not flixbd_results:
        return ""

    flixbd_section = ""
    if flixbd_results:
        flixbd_section = f"""
### FlixBD Site Match (top {len(flixbd_results)} results from target site):
```json
{json.dumps(flixbd_results, indent=2, ensure_ascii=False)}
```
(FlixBD match data. Use this to understand if the content already exists on the target site.
If the Existing DB Match section is missing, you should treat FlixBD download_links as the only available
"existing" source of resolutions/episode ranges.)
"""

    db_section = ""
    if db_match_info:
        db_section = f"""
### Existing DB Match (our database):
```json
{json.dumps(db_match_info, indent=2, ensure_ascii=False)}
```"""

    return f"""## ADDITIONAL TASK: Duplicate Check
Decide if this URL is already fully covered by existing content.
{db_section}{flixbd_section}
### Which existing source to compare against:
- If **only** FlixBD Site Match is present (no Existing DB Match block): compare ONLY to FlixBD `resolution_keys`.
- If **only** Existing DB Match is present: compare to `existing_resolutions`.
- If **both** are present: both must be fully covered before you can say "skip".
- Do NOT look at the scraped page title to determine what the site already has. The page title lists what the SOURCE offers, not what is already on FlixBD/DB.
- Do NOT write "database" in `reason` unless the Existing DB Match section is actually present above.

### MOVIE — Step-by-step resolution comparison (YOU MUST FOLLOW ALL 4 STEPS):

**Step 1 — List your EXTRACTED resolutions:**
Look at the `download_links` you extracted. Write down every resolution key that has a real, non-empty download URL.
Example: Extracted = [480p, 720p, 1080p]

**Step 2 — List the EXISTING resolutions on the site/DB:**
- From FlixBD: copy the `resolution_keys` array from the JSON above.
- From DB: copy `existing_resolutions`.
Example: Existing = [720p]

**Step 3 — Check each extracted resolution one by one:**
Go through your Extracted list. For each one, ask: "Is this resolution already in the Existing list?"
- 480p in Existing? NO — this is MISSING
- 720p in Existing? YES — already there
- 1080p in Existing? NO — this is MISSING
Write down the missing ones. Example: Missing = [480p, 1080p]

**Step 4 — Make your decision:**
- If Missing list is EMPTY (every extracted resolution is already in Existing) → action = **"skip"**
- If Missing list has ANY items → action = **"update"**, and set `missing_resolutions` to the Missing list
- NEVER return "skip" when the Missing list is not empty. Even one missing resolution means "update".

### COMMON MISTAKES (DO NOT MAKE THESE):

WRONG: "FlixBD has 720p, page also has 720p, so resolutions are the same → skip"
WHY WRONG: This ignores 480p and 1080p that were also extracted with URLs. You must check ALL extracted resolutions, not just the ones that match.

WRONG: "Content already exists on the site with the same resolutions → skip"
WHY WRONG: FlixBD only has 720p but you extracted 480p, 720p, and 1080p. Two resolutions are missing. This is "update", not "skip".

### CORRECT EXAMPLE:
FlixBD `resolution_keys`: ["720p"]. You extracted download_links with URLs for 480p, 720p, 1080p.
Step 1: Extracted = [480p, 720p, 1080p]
Step 2: Existing = [720p]
Step 3: 480p missing? YES. 720p missing? NO. 1080p missing? YES. Missing = [480p, 1080p]
Step 4: Missing is NOT empty → action = "update", missing_resolutions = ["480p", "1080p"]
Reason: "Extracted: [480p, 720p, 1080p]. Existing: [720p]. Missing: [480p, 1080p]. Action: update because 480p and 1080p are not on the site yet."

### TV SHOW duplicate rules:
Same title+year, no new episodes, and no missing per-episode resolutions → "skip".
New episodes or missing resolutions on any episode → "update".

### Action definitions:

**"skip"** — Nothing new to upload. Same movie with same title+year, AND every single extracted resolution already exists on the site/DB. Even ONE missing resolution means this is NOT a skip.

**"update"** — Same movie/show, but at least one extracted resolution (with a real URL) is not yet on the site/DB. Set `missing_resolutions` to the list of resolutions that are missing. Only count resolutions that have actual download URLs in your extraction, not just text in the page title.

**"replace"** — Same content but quality upgrade: existing is low quality (CAM/HDCAM/HDTS/DVDRip) and new source is better quality (WEB-DL/BluRay). Full re-download needed.

**"process"** — Different content entirely (different title, different year, different season). NOT for "same movie but more resolutions" — that is always "update".

### The `reason` field MUST follow this format:
"Extracted: [list]. Existing: [list]. Missing: [list]. Action: [action] because [explanation]."
This is mandatory. Do not write a vague sentence. Always show the three lists first.

### Duplicate Check Output:
Add a `duplicate_check` field to your response:
```json
{json.dumps(duplicate_schema, indent=2)}
```

"""



def get_combined_system_prompt(
    extra_below: bool = False,
    extra_above: bool = False,
    max_extra: int = 0,
    db_match_info: dict = None,
    flixbd_results: list = None,
) -> str:
    """
    Generate the combined system prompt based on resolution settings.
    If db_match_info or flixbd_results provided, adds duplicate check section.
    """
    res_note = _build_resolution_note(extra_below, extra_above, max_extra)
    has_dup = bool(db_match_info or flixbd_results)
    dup_section = _build_duplicate_section(db_match_info, flixbd_results) if has_dup else ""

    return f"""You are an expert web scraping assistant that can detect content type AND extract structured data in a single step.

## STEP 1: Detect Content Type
First, determine if the HTML content is about a **movie** or a **TV show**.

- **TV Show indicators**: "Season", "Episode", "S01", "E01", "Complete Season", "All Episodes", "Series", "Web Series", episode listings
- **Movie indicators**: Single download links without season/episode references, "Movie", "Film"
- If you see ANY season or episode references → classify as "tvshow"

## STEP 2: Extract Data
Based on your detection, extract the full structured data.

---

## IF content_type = "movie", extract data following this schema:
{json.dumps(movie_schema, indent=2)}

### Movie Rules:
- website_movie_title: Full title from site, with blocked site names removed (`Title Year Source Language - {SITE_NAME}`)
- title: CLEAN movie name only (no year, quality, language)
- rating: numeric only (e.g. 7.5)
- year: integer only

### ⚠️ RESOLUTION RULES:
{res_note}
---

## IF content_type = "tvshow", extract data following this schema:
{json.dumps(tvshow_schema, indent=2)}

### TV Show Rules:
- website_tvshow_title: Series format with `Season NN`, episode scope `EPxx` or `EPxx-yy`, or `Season NN Complete` for full-season combo — then Source, Language, ` - {SITE_NAME}` (see tvshow schema)

### TV Show Download Structure:
Each download item belongs to exactly ONE of these types. Classification is based ENTIRELY on the HTML/page structure — not the episode number or resolution count.

**`combo_pack`** — the download section covers the ENTIRE season with no episode breakdown in the heading.

**`partial_combo`** — the heading contains a NUMBER RANGE indicating multiple episodes (e.g., "Ep X-Y", "Episode N to M", "Part 1-8"). The section may have any number of resolution buttons (even just one). The defining signal is the RANGE in the label, not the button count.

**`single_episode`** — each individual episode has its OWN heading/section. There is no range — each heading refers to ONE episode only.

### ✅ Classification decision — ask these questions in order:
1. Does the heading cover the WHOLE season (no episode details)? → `combo_pack`
2. Does the heading contain a NUMBER RANGE (any two episode numbers joined by a hyphen, dash, or "to")? → `partial_combo`. Set `episode_range` to that range as-is.
3. Does the heading refer to exactly ONE episode? → `single_episode`

### ⚠️ Critical:
- The number of resolution buttons (1, 2, or 3) does NOT affect the type
- A range heading is ALWAYS `partial_combo` regardless of how few or many resolutions are available
- Do NOT infer or merge: if separate headings exist for each episode → keep as separate `single_episode` items

### ⚠️ NO DUPLICATE EPISODES — Priority Rules:
1. **combo_pack** present for the season → include ONLY the combo_pack, no partials or singles
2. **partial_combo** covers a range → do NOT add singles for any episode within that range
3. **single_episode** only for episodes not covered by any combo or partial

### ⚠️ RESOLUTION RULES:
{res_note}
---

## COMMON RULES (both movie and tvshow):
- Return ONLY a valid JSON object — NO markdown, NO backticks, NO extra text
- For missing fields, omit them entirely (do not return null or empty strings)
- Remove ALL references to these site names from ALL fields (including website_movie_title and website_tvshow_title): {_blocked_names_str}
- Always prefer x264 encodes when available
- **languages**: extract as array of strings e.g. ["Hindi", "English"]. Omit if not found.
- **countries**: extract as array of strings e.g. ["USA"]. Omit if not found.
- **cast** (movie) / **cast_info** (tvshow): comma-separated actors list. Omit if not found.

## IMPORTANT - website_movie_title vs website_tvshow_title:
- **Movie** → `Title Year Source Language - {SITE_NAME}` (no Season/EP). Example: `Inception 2010 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`
- **TV show** → `Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}` or `... Season NN Complete ...` for full-season pack. Example: `Single Papa 2025 Season 01 EP01-06 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`
- **Source** (both): WEB-DL, CAMRip, HDRip, BluRay, WEBRip, HDTS — not resolution (1080p/720p). **Language** from page. Always end with ` - {SITE_NAME}`.

- **meta_title**: Natural, human-like SEO title (50-60 chars). Place main keyword early. Vary structure — no repetitive patterns.
- **meta_description**: Compelling meta description (140-160 chars). Natural language with a CTA. Include content name, year, quality, language (and season for TV when relevant).
- **meta_keywords**: 10-15 comma-separated relevant keywords. Include name variations, year, quality variants, language, "download", "watch online", genre.

- ALL download URLs MUST be ABSOLUTE (start with https://).
- Relative URL → prepend source domain only. Nothing else changes.

### ⚠️ URL COPY RULE — NEVER decode, resolve, or alter any URL.
Ask: "Does this URL stay on the source site with an encoded parameter?"
- YES (gateway) → copy intact. Never decode the parameter value.
  ✅ `/generate.php?id=aHR0cHM6...` → `https://source.com/generate.php?id=aHR0cHM6...`
  ❌ WRONG: `https://actualdomain.com/file/abc123` (this is the decoded destination)
- NO (direct link) → copy exactly as-is.
Encoded parameters are server-side tokens — decoding them breaks the link.
If no gateway URL exists: use the best direct download link found on the page.

{dup_section}
## Response Format:
{{
  "content_type": "movie" or "tvshow",
  "data": {{ ... extracted data ... }}{',"duplicate_check": {{ ... }}' if has_dup else ''}
}}


## Output:
Return ONLY the JSON object. Nothing else."""


# Backward compat — default: only standard resolutions
COMBINED_SYSTEM_PROMPT = get_combined_system_prompt()
