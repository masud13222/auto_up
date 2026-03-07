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
