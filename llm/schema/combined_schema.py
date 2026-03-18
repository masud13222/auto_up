import json
from .blocked_names import BLOCKED_SITE_NAMES
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


def _build_duplicate_section(db_match_info: dict) -> str:
    """Build the duplicate check section for combined prompt."""
    if not db_match_info:
        return ""

    return f"""## ADDITIONAL TASK: Duplicate Check
You ALSO need to decide if this content is a duplicate of an existing database entry.

### Existing DB Match:
```json
{json.dumps(db_match_info, indent=2, ensure_ascii=False)}
```

### Duplicate Check Rules:
Compare the EXTRACTED data (not just the title) against the existing DB entry.

**"skip"** — SAME content, SAME or fewer resolutions/episodes. Nothing new to add.
  - For movies: same title+year AND extracted download_links has NO resolutions missing from existing
  - For TV shows: same title+year AND no new episodes AND no missing per-episode resolutions

**"update"** — SAME content BUT has NEW data to add:
  - Missing resolutions: your extracted data has resolutions the existing entry lacks
  - New episodes: your extracted data has episodes not in existing
  - IMPORTANT: Only report resolutions that ACTUALLY have download links in your extraction, NOT just mentioned in the title

**"replace"** — SAME content BUT quality upgrade needed:
  - Existing is low quality (CAM/HDCAM/HDTS/DVDRip) and new is better (WEB-DL/BluRay)

**"process"** — DIFFERENT content entirely (different title, year, or season)

### Duplicate Check Output:
Add a `duplicate_check` field to your response:
```json
{json.dumps(duplicate_schema, indent=2)}
```

"""


def get_combined_system_prompt(extra_below: bool = False, extra_above: bool = False, max_extra: int = 0, db_match_info: dict = None) -> str:
    """
    Generate the combined system prompt based on resolution settings.
    If db_match_info is provided, adds duplicate check section.
    """
    res_note = _build_resolution_note(extra_below, extra_above, max_extra)
    dup_section = _build_duplicate_section(db_match_info) if db_match_info else ""

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
- website_movie_title: Full title from site, with blocked site names removed
- title: CLEAN movie name only (no year, quality, language)
- rating: numeric only (e.g. 7.5)
- year: integer only

### ⚠️ RESOLUTION RULES:
{res_note}
---

## IF content_type = "tvshow", extract data following this schema:
{json.dumps(tvshow_schema, indent=2)}

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
- Extract ALL image URLs for screenshots
- Remove ALL references to these site names from ALL fields (including website_movie_title and website_tvshow_title): {_blocked_names_str}
- Always prefer x264 encodes when available

- ALL download URLs MUST be ABSOLUTE (start with https://).
- **HOW to make a relative URL absolute**: ONLY prepend the site domain. NOTHING else changes.
- NEVER decode, transform, resolve, shorten, or alter any URL in any way
- NEVER decode base64, URL-encoding, or any encoded parameters — even if `?id=` value looks like base64, leave it **exactly as-is**
- NEVER replace a URL with its decoded/resolved/redirected destination
- ⚠️ ENCODED RELATIVE URL — follow this exactly:
  - HTML has: `/generate.php?id=aHR0cHM6Ly9uZXc1...`
  - ✅ CORRECT: `https://siteurl.com/generate.php?id=aHR0cHM6Ly9uZXc1...`
  - ❌ WRONG: `https://new5.cinecloud.site/f/abc123` ← decoded destination, NEVER do this
- The ONLY allowed modification: prepend site domain to relative URLs. Everything else stays byte-for-byte identical.
- VIOLATION OF THIS RULE = ALL DOWNLOADS FAIL. This is the single most important rule.


{dup_section}
## Response Format:
{{
  "content_type": "movie" or "tvshow",
  "data": {{ ... extracted data ... }}{',"duplicate_check": {{ ... }}' if db_match_info else ''}
}}


## Output:
Return ONLY the JSON object. Nothing else."""


# Backward compat — default: only standard resolutions
COMBINED_SYSTEM_PROMPT = get_combined_system_prompt()
