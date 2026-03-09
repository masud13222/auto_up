import json
from .blocked_names import BLOCKED_SITE_NAMES
from .movie_schema import movie_schema
from .tvshow_schema import tvshow_schema

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


def get_combined_system_prompt(extra_below: bool = False, extra_above: bool = False, max_extra: int = 0) -> str:
    """
    Generate the combined system prompt based on resolution settings.
    """
    res_note = _build_resolution_note(extra_below, extra_above, max_extra)

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
- website_movie_title: FULL raw title as shown on site
- title: CLEAN movie name only (no year, quality, language)
- rating: numeric only (e.g. 7.5)
- year: integer only

### ⚠️ RESOLUTION RULES:
{res_note}
---

## IF content_type = "tvshow", extract data following this schema:
{json.dumps(tvshow_schema, indent=2)}

### TV Show Download Structure:
TV shows have complex download structures organized SEASON-WISE.

Each season can have these download item types:
1. **combo_pack** — Entire season as a single download
2. **partial_combo** — Range of episodes bundled together (e.g., Ep 01-08)
3. **single_episode** — Individual episode download

### ⚠️ NO DUPLICATE EPISODES — Priority Rules:
1. **combo_pack** covers ALL episodes → do NOT add partial combos or singles for that season
2. **partial_combo** covers a range → do NOT add singles for episodes in that range
3. **single_episode** ONLY for episodes NOT covered by a combo or partial combo

### Classifying download types:
- **combo_pack**: ONE download section for the WHOLE season
- **partial_combo**: ONE download section for a RANGE of episodes (e.g., "Ep 01-08")
- **single_episode**: Each episode has its OWN separate download section
- DO NOT group consecutive single episodes into combos

### ⚠️ RESOLUTION RULES:
{res_note}
---

## COMMON RULES (both movie and tvshow):
- Return ONLY a valid JSON object — NO markdown, NO backticks, NO extra text
- For missing fields, omit them entirely (do not return null or empty strings)
- Extract ALL image URLs for screenshots
- Remove ALL references to these site names: {_blocked_names_str}
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


## Response Format:
{{
  "content_type": "movie" or "tvshow",
  "data": {{ ... extracted data ... }}
}}


## Output:
Return ONLY the JSON object. Nothing else."""


# Backward compat — default: only standard resolutions
COMBINED_SYSTEM_PROMPT = get_combined_system_prompt()
