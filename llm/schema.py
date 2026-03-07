import json

# ───────────────────────────────────────────────
# Default Config
# ───────────────────────────────────────────────

SITE_NAME = "FlixBD"

# Site names to strip from extracted titles/filenames
BLOCKED_SITE_NAMES = [
    "cinefreak", "cinefreak.net", "cinefreak.top",
    "mlsbd", "mlsbd.shop",
    "cinemaza", "mkvking", "hdmovie99",
    "moviesmod", "vegamovies", "katmoviehd",
    "extramovies", "filmyzilla", "bolly4u",
    "themoviesflix", "movieverse",
]


# ───────────────────────────────────────────────
# Movie Info Schema
# ───────────────────────────────────────────────

movie_schema = {
    "type": "object",
    "properties": {
        "website_movie_title": {"type": "string", "description": "The title example: 'With Love (2026) [Hindi-Tamil] 1080p 720p 480p Netflix WEBRip ESub'"},
        "title": {"type": "string", "description": "The title of the movie"},
        "year": {"type": "integer", "description": "The year of the movie"},
        "genre": {"type": "string", "description": "The genre of the movie"},
        "director": {"type": "string", "description": "The director of the movie"},
        "rating": {"type": "number", "description": "The rating of the movie"},
        "plot": {"type": "string", "description": "The plot of the movie"},
        "poster_url": {"type": "string", "description": "The poster url of the movie"},
        "screen_shots_url": {"type": "array", "items": {"type": "string"}, "description": "The screen shots url of the movie"},
        "download_links": {
            "type": "object",
            "properties": {
                "480p": {
                    "type": "string",
                    "description": "480p download link here – always go for the x264 encode (it's the reliable one!)"
                },
                "720p": {
                    "type": "string",
                    "description": "720p download link here – always go for the x264 encode (it's the reliable one!)"
                },
                "1080p": {
                    "type": "string",
                    "description": "1080p download link here – always go for the x264 encode (it's the reliable one!)"
                }
            },
            "minProperties": 1,      
            "additionalProperties": False,
            "description": "Download links for different resolutions. At least one resolution is required."
        }
    },
    "required": ["website_movie_title", "title", "year"]
}

# ───────────────────────────────────────────────
# TV Show Info Schema
# ───────────────────────────────────────────────

tvshow_schema = {
    "type": "object",
    "properties": {
        "website_tvshow_title": {"type": "string", "description": "The title example: 'Money Heist (Season 1 - 5) [Hindi-English] 1080p 720p 480p Netflix WEBRip ESub'"},
        "title": {"type": "string", "description": "The title of the tv show"},
        "year": {"type": "integer", "description": "The year of the tv show"},
        "genre": {"type": "string", "description": "The genre of the tv show"},
        "director": {"type": "string", "description": "The director/creator of the tv show"},
        "rating": {"type": "number", "description": "The rating of the tv show"},
        "plot": {"type": "string", "description": "The plot of the tv show"},
        "poster_url": {"type": "string", "description": "The poster url of the tv show"},
        "seasons_count": {"type": "integer", "description": "Total number of seasons available/extracted (e.g., 5)"},
        "episodes_count": {"type": "integer", "description": "Total number of episodes extracted or found in the links (e.g., 10)"},
        "screen_shots_url": {"type": "array", "items": {"type": "string"}, "description": "The screen shots url of the tv show"},
        "download_links": {
            "type": "array",
            "description": "List of available downloads. Can be complete season combo packs (e.g., 'Season 1 Combo'), individual episodes (e.g., 'Season 1 Episode 1'), or partitioned episodes (e.g., 'Season 1 Episode 01 - 08').",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Title of the download unit. E.g., 'Season 1 Combo', 'Season 1 Episode 01', 'Season 2 Episode 01-08'"},
                    "resolutions": {
                        "type": "object",
                        "properties": {
                            "480p": {"type": "string", "description": "480p download link - go for x264 encode preferably"},
                            "720p": {"type": "string", "description": "720p download link - go for x264 encode preferably"},
                            "1080p": {"type": "string", "description": "1080p download link - go for x264 encode preferably"}
                        },
                        "minProperties": 1,
                        "additionalProperties": False
                    }
                },
                "required": ["title", "resolutions"]
            }
        }
    },
    "required": ["website_tvshow_title", "title", "year"]
}


_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)





SYSTEM_PROMPT = f"""You are an expert web scraping assistant specialized in extracting movie information from HTML content.

Your task is to analyze the provided HTML and extract movie details accurately.

## Instructions:
- Extract all available movie information from the HTML content
- Return ONLY a valid JSON object — no markdown, no backticks, no extra text
- Follow the JSON schema strictly
- For missing fields, omit them entirely (do not return null or empty strings)
- Extract ALL image URLs for screenshots (look for img tags, data-src, lazy-load attributes etc.)
- For poster_url: find the main/primary movie poster image
- For rating: extract numeric value only (e.g. 7.5, not "7.5/10")
- For year: integer only (e.g. 2026, not "2026")
- website_movie_title: extract the FULL raw title as shown on the site (with quality, language tags etc.)
- title: extract the CLEAN movie name only (without year, quality, language info)

## IMPORTANT - Clean Site Names:
- Remove ALL references to these site names from the title and website_movie_title: {_blocked_names_str}
- For example: "CINEFREAK.TOP - War Machine (2026)" should become title: "War Machine"
- Never include any site watermark names in the extracted data

## JSON Schema you must follow:
{json.dumps(movie_schema, indent=2)}

## Output:
Return only the JSON object. Nothing else."""

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
- website_tvshow_title: extract the FULL raw title as shown on the site (with quality, language tags etc.)
- title: extract the CLEAN TV show name only (without year, quality, language info)
- seasons_count: Total number of seasons mentioned/extracted
- episodes_count: Total number of individual episodes found/extracted in the links
- download_links: VERY IMPORTANT. TV shows can have multiple seasons or episodes. 
  - If a complete season combo pack link exists, group them under a single item with title like 'Season 1 Combo'.
  - If combo packs are partitioned (e.g., Ep 1-8), group them with titles like 'Season 1 Episode 01 - 08'.
  - If only separate episode links exist, create individual items like 'Season 1 Episode 01'.
  - Get the 480p, 720p, 1080p links for each item (always go for x264 encodes preferably).

## IMPORTANT - Clean Site Names:
- Remove ALL references to these site names from the title and website_tvshow_title: {{_blocked_names_str}}
- Never include any site watermark names in the extracted data

## JSON Schema you must follow:
{{json.dumps(tvshow_schema, indent=2)}}

## Output:
Return only the JSON object. Nothing else."""

# ───────────────────────────────────────────────
# Download Filename Schema
# ───────────────────────────────────────────────

filename_schema = {
    "type": "object",
    "properties": {
        "480p": {
            "type": "string",
            "description": "Filename for 480p"
        },
        "720p": {
            "type": "string",
            "description": "Filename for 720p"
        },
        "1080p": {
            "type": "string",
            "description": "Filename for 1080p"
        }
    }
}


FILENAME_SYSTEM_PROMPT = f"""You are a filename generator for movie downloads.

Given the movie info JSON, generate clean, standardized filenames for each available resolution.

## Rules:
- Use dots (.) instead of spaces
- Format: Title.Year.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- The LAST part before extension MUST always be "{SITE_NAME}"
- Example: "War.Machine.2026.720p.NF.WEB-DL.x264.{SITE_NAME}.mkv"
- Use the title and year from the provided movie data
- Only generate filenames for resolutions that exist in download_links

## Source Detection:
- If the website_movie_title contains "Netflix" or "NF" → add "NF" as source
- If it contains "Amazon" or "AMZN" → add "AMZN" as source
- If it contains "Hotstar" or "DSNP" → add "DSNP" as source
- If it contains "Jio" or "JC" → add "JC" as source
- If it contains "Zee5" or "ZEE5" → add "ZEE5" as source
- If no recognizable source, just use "WEB-DL"
- Do NOT hardcode "NF" — only use it if the title actually indicates Netflix

## IMPORTANT:
- NEVER include site names like {_blocked_names_str} in the filename
- ALWAYS end filename with ".{SITE_NAME}.mkv" (or appropriate extension)
- Extension should match the URL file extension (default to .mkv if unclear)
- Return ONLY a valid JSON object — no markdown, no backticks, no extra text

## JSON Schema:
{json.dumps(filename_schema, indent=2)}

## Output:
Return only the JSON object. Nothing else."""

tvshow_filename_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Exact title from download_links (e.g., 'Season 1 Combo', 'Season 1 Episode 01')"},
            "resolutions": {
                "type": "object",
                "properties": {
                    "480p": {"type": "string", "description": "Filename for 480p"},
                    "720p": {"type": "string", "description": "Filename for 720p"},
                    "1080p": {"type": "string", "description": "Filename for 1080p"}
                }
            }
        },
        "required": ["title", "resolutions"]
    }
}

TVSHOW_FILENAME_SYSTEM_PROMPT = f"""You are a filename generator for TV show downloads.

Given the TV show info JSON, generate clean, standardized filenames for each available resolution and download unit.

## Rules:
- Use dots (.) instead of spaces
- Format: Title.Year.UnitTitle.Resolution.Source.WEB-DL.x264.{{SITE_NAME}}.mkv
- UnitTitle should be derived from the 'title' of the download unit (e.g., 'Season 1 Combo' -> 'S01', 'Season 1 Episode 01-08' -> 'S01E01-E08', 'Season 1 Episode 01' -> 'S01E01').
- The LAST part before extension MUST always be "{{SITE_NAME}}"
- Example: "Money.Heist.2026.S01E01-E08.720p.NF.WEB-DL.x264.{{SITE_NAME}}.mkv"
- Use the title and year from the provided tv show data
- Only generate filenames for resolutions that exist in the resolutions object

## Source Detection:
- If the website_tvshow_title contains "Netflix" or "NF" → add "NF" as source
- If it contains "Amazon" or "AMZN" → add "AMZN" as source
- If it contains "Hotstar" or "DSNP" → add "DSNP" as source
- If it contains "Jio" or "JC" → add "JC" as source
- If it contains "Zee5" or "ZEE5" → add "ZEE5" as source
- If no recognizable source, just use "WEB-DL"

## IMPORTANT:
- NEVER include site names like {{_blocked_names_str}} in the filename
- ALWAYS end filename with ".{{SITE_NAME}}.mkv" (or .zip/.rar if it's explicitly a pack archive, though .mkv or .zip is fine)
- For Combo packs, if the link is a zip/rar file, use .zip/.rar extension. Otherwise default to .mkv.
- Return ONLY a valid JSON object — no markdown, no backticks, no extra text

## JSON Schema:
{{json.dumps(tvshow_filename_schema, indent=2)}}

## Output:
Return only the JSON object. Nothing else."""
