import json
from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)

# ───────────────────────────────────────────────
# TV Show Schema
# ───────────────────────────────────────────────

tvshow_schema = {
    "type": "object",
    "properties": {
        "website_tvshow_title": {
            "type": "string",
            "description": "Full title from site with blocked site names removed. E.g., 'Money Heist (Season 1-5) [Hindi] 1080p 720p 480p Netflix WEBRip'"
        },
        "title": {
            "type": "string",
            "description": "Clean show name only — no year, quality, or language tags"
        },
        "year":        {"type": "integer", "description": "Release year (integer only)"},
        "genre":       {"type": "string"},
        "director":    {"type": "string"},
        "rating":      {"type": "number", "description": "Numeric only, e.g. 7.5"},
        "plot":        {"type": "string"},
        "poster_url":  {"type": "string", "description": "Main poster image URL"},
        "screen_shots_url": {
            "type": "array",
            "items": {"type": "string"},
            "description": "All screenshot image URLs found on page"
        },
        "total_seasons": {"type": "integer"},
        "seasons": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "season_number": {"type": "integer"},
                    "download_items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["combo_pack", "partial_combo", "single_episode"]
                                },
                                "label":         {"type": "string", "description": "E.g. 'Season 1 Combo Pack', 'Season 2 Episode 01-08', 'Season 3 Episode 05'"},
                                "episode_range": {"type": "string", "description": "E.g. '01-08' or '05'. Omit for combo_pack."},
                                "resolutions": {
                                    "type": "object",
                                    "additionalProperties": {"type": "string"},
                                    "description": "Download URLs keyed by resolution: '480p', '720p', '1080p', etc."
                                }
                            },
                            "required": ["type", "label", "resolutions"]
                        }
                    }
                },
                "required": ["season_number", "download_items"]
            }
        }
    },
    "required": ["website_tvshow_title", "title", "year"]
}

# ───────────────────────────────────────────────
# TV Show System Prompt
# ───────────────────────────────────────────────

TVSHOW_SYSTEM_PROMPT = f"""You are a web scraping assistant. Extract TV show data from HTML and return a single valid JSON object. No markdown, no backticks, no extra text.

## GENERAL RULES:
- Omit missing fields entirely (no null, no empty strings)
- Extract ALL screenshot image URLs from the page
- Strip these site names from ALL fields (including website_tvshow_title): {_blocked_names_str}
- Prefer x264 encodes when multiple encode options exist
- rating: numeric only (e.g. 7.5) | year: integer only (e.g. 2024)

## URL RULES (CRITICAL — violations break all downloads):
- All URLs must be ABSOLUTE (start with https://)
- To convert a relative URL: ONLY prepend the site domain — change NOTHING else
- NEVER decode base64, URL params, or any encoding — if `?id=` looks like base64, leave it exactly as-is
- ✅ CORRECT: `/generate.php?id=aHR0cHM6...` → `https://site.com/generate.php?id=aHR0cHM6...`
- ❌ WRONG:   `/generate.php?id=aHR0cHM6...` → `https://new5.cinecloud.site/f/abc123` (decoded — NEVER do this)

## DOWNLOAD STRUCTURE:
Organize all downloads season-wise. Each download item has one of three types:

| Type | When to use | episode_range |
|------|-------------|---------------|
| `combo_pack` | ONE download covers the ENTIRE season | omit |
| `partial_combo` | ONE download covers a RANGE of episodes (e.g. Ep 01-08) | "01-08" |
| `single_episode` | Each episode has its OWN separate download section | "05" |

⚠️ DO NOT group consecutive single episodes into a partial_combo. If each episode has its own links → always `single_episode`.

## NO DUPLICATE EPISODES (strict priority):
1. If `combo_pack` exists for a season → include ONLY the combo_pack. No partials, no singles.
2. If `partial_combo` covers Ep 01-08 → do NOT add single episodes for Ep 01-08. Only add singles for episodes outside that range.
3. `single_episode` only for episodes not covered by any combo or partial.

## CORRECT EXAMPLE:
Page has: Season 1 full combo | Season 2 Ep 01-08 bundle | Season 2 Ep 09-16 bundle | Season 2 Ep 17,18,19 individually

Output:
- Season 1 → combo_pack only
- Season 2 → partial_combo "01-08", partial_combo "09-16", single_episode 17, single_episode 18, single_episode 19

## JSON SCHEMA:
{json.dumps(tvshow_schema, indent=2)}

Return only the JSON object. Nothing else."""


# ───────────────────────────────────────────────
# TV Show Filename Schema
# ───────────────────────────────────────────────

tvshow_filename_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "season_number": {"type": "integer"},
            "type": {"type": "string", "enum": ["combo_pack", "partial_combo", "single_episode"]},
            "label": {"type": "string", "description": "Exact label from the download item"},
            "resolutions": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": "Filenames keyed by resolution. Only include resolutions present in the download item."
            }
        },
        "required": ["season_number", "type", "label", "resolutions"]
    }
}

TVSHOW_FILENAME_SYSTEM_PROMPT = f"""You are a filename generator for TV show downloads.

Given TV show JSON, generate standardized filenames for every download item. Return a JSON array. No markdown, no backticks.

## FORMAT BY TYPE:
- combo_pack:    Title.Year.S01.Complete.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- partial_combo: Title.Year.S01E01-E08.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- single_episode: Title.Year.S01E05.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- combo_pack archives (.zip/.rar): use matching extension instead of .mkv

## SOURCE DETECTION (from website_tvshow_title):
Netflix/NF → NF | Amazon/AMZN → AMZN | Hotstar/DSNP → DSNP | Jio/JC → JC | ZEE5/Zee5 → ZEE5 | none matched → WEB-DL

## RULES:
- Use dots instead of spaces
- Last part before extension MUST be "{SITE_NAME}"
- Only generate filenames for resolutions that exist in the item
- NEVER include site names: {_blocked_names_str}
- One entry per download_item (match by season_number + type + label)

## JSON SCHEMA:
{json.dumps(tvshow_filename_schema, indent=2)}

Return only the JSON array. Nothing else."""