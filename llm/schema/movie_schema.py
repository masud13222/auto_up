import json
from .blocked_names import SITE_NAME, BLOCKED_SITE_NAMES

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)


# ───────────────────────────────────────────────
# Movie Info Schema
# ───────────────────────────────────────────────

movie_schema = {
    "type": "object",
    "properties": {
        "website_movie_title": {
            "type": "string",
            "description": (
                f"Formatted display title in this exact format: "
                f"'Title Year Source Language - {SITE_NAME}'. "
                f"Source = WEB-DL / CAMRip / HDRip / BluRay / WEBRip / HDTS (NOT resolution). "
                f"Language like 'Dual Audio [Hindi ORG. + English]' or 'Bengali'. "
                f"Remove ALL blocked site names. "
                f"Example: 'Inception 2010 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}'"
            )
        },
        "title": {"type": "string", "description": "The clean movie name only (no year, quality, language)"},
        "year": {"type": "integer", "description": "The year of the movie"},
        "genre": {"type": "string", "description": "The genre of the movie"},
        "director": {"type": "string", "description": "The director of the movie"},
        "rating": {"type": "number", "description": "Numeric rating only (e.g. 7.5)"},
        "plot": {"type": "string", "description": "The plot of the movie"},
        "poster_url": {"type": "string", "description": "The poster url of the movie"},
        "meta_title": {"type": "string", "description": "Natural SEO title (50-60 chars). Place main keyword early. Vary structure — avoid repeating the same pattern across pages."},
        "meta_description": {"type": "string", "description": "Compelling meta description (140-160 chars). Natural language with a click-worthy CTA. Include movie name, year, quality, language naturally."},
        "meta_keywords": {"type": "string", "description": "10-15 comma-separated SEO keywords. Include name variations, year, quality variants, language, dubbed, genre, 'download', 'watch online'."},
        "download_links": {
            "type": "object",
            "additionalProperties": {"type": "string"},
            "description": "Download links keyed by resolution (e.g. '480p', '720p', '1080p'). Keys are dynamic — any resolution is allowed. Values are download URLs. Prefer x264 encodes."
        },
        "cast": {
            "type": "string",
            "description": "Comma-separated cast/actors list. E.g. 'Leonardo DiCaprio, Joseph Gordon-Levitt'"
        },
        "languages": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of audio languages available. E.g. ['Hindi', 'English'] or ['Bengali']"
        },
        "countries": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of production countries. E.g. ['USA', 'UK']"
        },
        "imdb_id": {
            "type": "string",
            "description": "IMDb ID if found on the page (e.g. 'tt1375666'). Omit if not present."
        },
        "tmdb_id": {
            "type": "string",
            "description": "TMDB ID if found on the page (e.g. '27205'). Omit if not present."
        }
    },
    "required": ["website_movie_title", "title", "year"]
}


MOVIE_SYSTEM_PROMPT = f"""You are an expert web scraping assistant specialized in extracting movie information from HTML content.

Your task is to analyze the provided HTML and extract movie details accurately.

## Instructions:
- Extract all available movie information from the HTML content
- Return ONLY a valid JSON object — no markdown, no backticks, no extra text
- Follow the JSON schema strictly
- For missing fields, omit them entirely (do not return null or empty strings)
- For poster_url: find the main/primary movie poster image
- For rating: extract numeric value only (e.g. 7.5, not "7.5/10")
- For year: integer only (e.g. 2026, not "2026")
- title: extract the CLEAN movie name only (without year, quality, language info)
- cast: comma-separated actors if listed on page
- languages: array of audio languages found on page (e.g. ["Hindi", "English"])
- countries: array of production countries from the page (e.g. ["USA"])

## IMPORTANT - website_movie_title field (MUST generate in this exact format):
`Title Year Source Language - {SITE_NAME}`
- **Title**: clean movie title
- **Year**: 4-digit year
- **Source**: WEB-DL, CAMRip, HDRip, BluRay, WEBRip, HDTS, or similar — detect from page content. Do NOT use resolution (1080p/720p) here.
- **Language**: e.g. `Dual Audio [Hindi ORG. + English]` or `Bengali` or `Multi Audio [Hindi + Bengali + Tamil]`. Extract from the page.
- **{SITE_NAME}**: always append ` - {SITE_NAME}` at the end
Example: `Inception 2010 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`

## IMPORTANT - Clean Site Names:
- Remove ALL references to these site names from ALL fields (title, website_movie_title, etc.): {_blocked_names_str}
- For example: "CINEFREAK.TOP - War Machine (2026)" should become title: "War Machine"
- Never include any site watermark names in the extracted data

## SEO Meta Fields (MUST generate — do NOT skip):
- **meta_title**: Create a natural, human-like SEO title (50-60 chars). Place the main keyword (movie name) early. Avoid repetitive patterns across pages — vary the structure. Do NOT always use the same format.
  Good examples:
  - "Ali (2025) Bengali WEB-DL 1080p 720p Full Movie"
  - "Download Kaliyugam (2025) Hindi Dubbed 480p 720p 1080p"
  - "War Machine 2026 Hindi Full Movie WEB-DL Download"
- **meta_description**: Write a compelling, natural meta description (140-160 chars). Use human language — NOT a keyword list. Include a CTA (call-to-action) that makes users want to click. Mention movie name, year, quality, and language naturally.
  Good examples:
  - "Download Ali (2025) Bengali full movie in multiple qualities. Fast direct links, English subs included. Stream or download now."
  - "Watch Kaliyugam (2025) Hindi Dubbed in 480p to 1080p. High-speed GDrive download with subtitles. Grab your copy today."
- **meta_keywords**: Generate 10-15 relevant, comma-separated keywords. Include name variations (with/without year), quality variants (480p, 720p, 1080p), language, "download", "watch online", "full movie", genre terms.
  Example: "Ali, Ali 2025, Ali Bengali movie, Ali download, Ali 480p, Ali 720p, Ali 1080p, Bengali movie download, WEB-DL, full movie, watch online, GDrive, drama"

## JSON Schema you must follow:
{json.dumps(movie_schema, indent=2)}

## Output:
Return only the JSON object. Nothing else."""

# ───────────────────────────────────────────────
# Download Filename Schema
# ───────────────────────────────────────────────

filename_schema = {
    "type": "object",
    "additionalProperties": {"type": "string"},
    "description": "Filenames keyed by resolution (e.g. '480p', '720p', ....). Only include resolutions that exist in download_links."
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
