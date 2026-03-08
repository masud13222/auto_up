import json
from .schema import BLOCKED_SITE_NAMES, SITE_NAME

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)


# ───────────────────────────────────────────────
# TV Show Info Schema
# ───────────────────────────────────────────────
# 
# TV Shows have a complex download structure:
# 1. Combo Pack: Full season download (e.g., Season 1 complete)
# 2. Partial Combo Pack: Part of a season (e.g., Season 1 Episode 01-08)
# 3. Single Episode: Individual episode download
#
# Each download unit has its own resolution links (480p, 720p, 1080p)
# Everything is grouped season-wise.
#

tvshow_schema = {
    "type": "object",
    "properties": {
        "website_tvshow_title": {
            "type": "string",
            "description": "The full raw title as shown on the site. E.g., 'Money Heist (Season 1 - 5) [Hindi-English] 1080p 720p 480p Netflix WEBRip ESub'"
        },
        "title": {
            "type": "string",
            "description": "The clean TV show name only (without year, quality, language info)"
        },
        "year": {
            "type": "integer",
            "description": "The year of the TV show"
        },
        "genre": {
            "type": "string",
            "description": "The genre of the TV show"
        },
        "director": {
            "type": "string",
            "description": "The director/creator of the TV show"
        },
        "rating": {
            "type": "number",
            "description": "The rating of the TV show (numeric only, e.g. 7.5)"
        },
        "plot": {
            "type": "string",
            "description": "The plot summary of the TV show"
        },
        "poster_url": {
            "type": "string",
            "description": "The main/primary poster image URL"
        },
        "screen_shots_url": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Screenshot image URLs from the page"
        },
        "total_seasons": {
            "type": "integer",
            "description": "Total number of seasons available/extracted"
        },
        "seasons": {
            "type": "array",
            "description": "List of seasons with their download units. Each season contains its download items (combo packs, partial combos, or individual episodes).",
            "items": {
                "type": "object",
                "properties": {
                    "season_number": {
                        "type": "integer",
                        "description": "The season number (e.g., 1, 2, 3)"
                    },
                    "download_items": {
                        "type": "array",
                        "description": "List of downloadable units for this season. Can be: combo pack (full season), partial combo (e.g., Ep 01-08), or single episodes.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["combo_pack", "partial_combo", "single_episode"],
                                    "description": "Type of download unit: 'combo_pack' = entire season, 'partial_combo' = range of episodes (e.g., Ep 01-08), 'single_episode' = individual episode"
                                },
                                "label": {
                                    "type": "string",
                                    "description": "Human-readable label. E.g., 'Season 1 Combo Pack', 'Season 1 Episode 01-08', 'Season 1 Episode 05'"
                                },
                                "episode_range": {
                                    "type": "string",
                                    "description": "Episode range or number. E.g., '01-08' for partial combo, '05' for single episode, null/omit for combo pack"
                                },
                                "resolutions": {
                                    "type": "object",
                                    "properties": {
                                        "480p": {
                                            "type": "string",
                                            "description": "480p download link - prefer x264 encode"
                                        },
                                        "720p": {
                                            "type": "string",
                                            "description": "720p download link - prefer x264 encode"
                                        },
                                        "1080p": {
                                            "type": "string",
                                            "description": "1080p download link - prefer x264 encode"
                                        }
                                    },
                                    "minProperties": 1,
                                    "additionalProperties": False
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

TVSHOW_SYSTEM_PROMPT = f"""You are an expert web scraping assistant specialized in extracting TV show information from HTML content.

Your task is to analyze the provided HTML and extract TV show details accurately.

## Instructions:
- Extract all available TV show information from the HTML content
- Return ONLY a valid JSON object — no markdown, no backticks, no extra text
- Follow the JSON schema strictly
- For missing fields, omit them entirely (do not return null or empty strings)
- Extract ALL image URLs for screenshots (look for img tags, data-src, lazy-load attributes etc.)
- For poster_url: find the main/primary TV show poster image
- For rating: extract numeric value only (e.g. 7.5, not "7.5/10")
- For year: integer only (e.g. 2026, not "2026")

## Title Extraction:
- website_tvshow_title: extract the FULL raw title as shown on the site (with quality, language tags etc.)
- title: extract the CLEAN TV show name only (without year, quality, language info)

## Season & Download Structure (VERY IMPORTANT):
TV shows have complex download structures. You MUST organize downloads SEASON-WISE.

For each season, there can be multiple types of download items:

### 1. Combo Pack (type: "combo_pack")
- When the entire season is available as a single download
- Label example: "Season 1 Combo Pack"
- No episode_range needed

### 2. Partial Combo Pack (type: "partial_combo")  
- When a range of episodes from a season are bundled together
- Label example: "Season 1 Episode 01-08"
- episode_range: "01-08"

### 3. Single Episode (type: "single_episode")
- When individual episodes are available separately
- Label example: "Season 1 Episode 05"
- episode_range: "05"

## ⚠️ CRITICAL: NO DUPLICATE EPISODES — PRIORITY RULES:
Follow this strict priority hierarchy. NEVER include the same episodes twice:

1. **combo_pack** covers ALL episodes of a season → if a combo pack exists for a season, do NOT add ANY partial combos or single episodes for that same season.

2. **partial_combo** covers a RANGE of episodes → if a partial combo covers episodes 01-08, do NOT add single episodes for episodes 01 through 08. Only add single episodes for episodes NOT covered by any partial combo.

3. **single_episode** is ONLY for episodes that are NOT already covered by a combo_pack or partial_combo.

### Example of CORRECT behavior:
If the page has:
- Combo Pack for Season 1 (all episodes)
- Episode 01-08 bundle for Season 2
- Episode 09-16 bundle for Season 2
- Individual episodes 17, 18, 19 for Season 2

Then output should be:
- Season 1: combo_pack ONLY (no singles)
- Season 2: partial_combo "01-08", partial_combo "09-16", single ep 17, 18, 19 ONLY

### Example of WRONG behavior (DO NOT DO THIS):
- Season 2: partial_combo "01-08" AND ALSO single ep 01, 02, 03... ← WRONG! These are already in the partial combo!

## Rules for download extraction:
- Group ALL download items under their respective season
- Each download item has its own resolution links (480p, 720p, 1080p)
- Always prefer x264 encodes when available
- A season can have mixed types (e.g., partial combo AND single episodes for different episode ranges)
- If the same season appears multiple times with different episode ranges, they all go under the same season_number
- Keep the output CONCISE — only include what's actually on the page, no duplicates
- **ALL URLs must be ABSOLUTE** (start with https://). NEVER use relative URLs like /generate.php. Always include the full domain, e.g. https://www.example.net/generate.php?id=...

## ⚠️ How to correctly classify download types:
- **combo_pack**: ONLY when a SINGLE download link covers an ENTIRE season bundled together. The page will show ONE download section for the whole season with a SINGLE set of resolution links.
- **partial_combo**: ONLY when a SINGLE download link covers a RANGE of episodes bundled together (e.g., one file for "Episode 01-08"). The page will show ONE download section with a SINGLE set of resolution links for that range.
- **single_episode**: When an individual episode has its OWN separate download section with its OWN individual links.
- **DO NOT** group consecutive single episodes into a combo or partial_combo. If each episode has its own separate download section on the page, they are always "single_episode".

## Example structure:
```json
{{
  "seasons": [
    {{
      "season_number": 1,
      "download_items": [
        {{
          "type": "combo_pack",
          "label": "Season 1 Combo Pack",
          "resolutions": {{"480p": "url", "720p": "url", "1080p": "url"}}
        }}
      ]
    }},
    {{
      "season_number": 2,
      "download_items": [
        {{
          "type": "partial_combo",
          "label": "Season 2 Episode 01-08",
          "episode_range": "01-08",
          "resolutions": {{"480p": "url", "720p": "url"}}
        }},
        {{
          "type": "partial_combo",
          "label": "Season 2 Episode 09-12",
          "episode_range": "09-12",
          "resolutions": {{"720p": "url", "1080p": "url"}}
        }}
      ]
    }}
  ]
}}
```

## IMPORTANT - Clean Site Names:
- Remove ALL references to these site names from the title and website_tvshow_title: {_blocked_names_str}
- Never include any site watermark names in the extracted data

## JSON Schema you must follow:
{json.dumps(tvshow_schema, indent=2)}

## Output:
Return only the JSON object. Nothing else."""


# ───────────────────────────────────────────────
# TV Show Filename Schema
# ───────────────────────────────────────────────

tvshow_filename_schema = {
    "type": "array",
    "description": "List of filename objects matching each download item",
    "items": {
        "type": "object",
        "properties": {
            "season_number": {
                "type": "integer",
                "description": "Season number this filename belongs to"
            },
            "type": {
                "type": "string",
                "enum": ["combo_pack", "partial_combo", "single_episode"],
                "description": "Type of download unit"
            },
            "label": {
                "type": "string",
                "description": "Exact label from the download item (e.g., 'Season 1 Combo Pack', 'Season 1 Episode 01-08')"
            },
            "resolutions": {
                "type": "object",
                "properties": {
                    "480p": {"type": "string", "description": "Filename for 480p"},
                    "720p": {"type": "string", "description": "Filename for 720p"},
                    "1080p": {"type": "string", "description": "Filename for 1080p"}
                }
            }
        },
        "required": ["season_number", "type", "label", "resolutions"]
    }
}


TVSHOW_FILENAME_SYSTEM_PROMPT = f"""You are a filename generator for TV show downloads.

Given the TV show info JSON, generate clean, standardized filenames for each download item across all seasons.

## Rules:
- Use dots (.) instead of spaces
- The LAST part before extension MUST always be "{SITE_NAME}"
- Use the title and year from the provided TV show data
- Only generate filenames for resolutions that exist in the resolutions object

## Filename Format by Type:

### combo_pack:
- Format: Title.Year.S01.Complete.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- Example: Money.Heist.2017.S01.Complete.720p.NF.WEB-DL.x264.{SITE_NAME}.mkv
- For zip/rar archives use .zip/.rar extension

### partial_combo:
- Format: Title.Year.S01E01-E08.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- Example: Money.Heist.2017.S02E01-E08.720p.NF.WEB-DL.x264.{SITE_NAME}.mkv

### single_episode:
- Format: Title.Year.S01E05.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- Example: Money.Heist.2017.S03E05.720p.NF.WEB-DL.x264.{SITE_NAME}.mkv

## Source Detection:
- If the website_tvshow_title contains "Netflix" or "NF" → add "NF" as source
- If it contains "Amazon" or "AMZN" → add "AMZN" as source
- If it contains "Hotstar" or "DSNP" → add "DSNP" as source
- If it contains "Jio" or "JC" → add "JC" as source
- If it contains "Zee5" or "ZEE5" → add "ZEE5" as source
- If no recognizable source, just use "WEB-DL"
- Do NOT hardcode "NF" — only use it if the title actually indicates Netflix

## IMPORTANT:
- NEVER include site names like {_blocked_names_str} in the filename
- ALWAYS end filename with ".{SITE_NAME}.mkv" (or .zip/.rar for combo pack archives)
- Return ONLY a valid JSON object — no markdown, no backticks, no extra text
- Generate one entry per download_item, matching by season_number, type, and label

## JSON Schema:
{json.dumps(tvshow_filename_schema, indent=2)}

## Output:
Return only the JSON array. Nothing else."""
