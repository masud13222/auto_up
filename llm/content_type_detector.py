import json
import logging

logger = logging.getLogger(__name__)


# ───────────────────────────────────────────────
# Content Type Detection Schema & Prompt
# ───────────────────────────────────────────────

content_type_schema = {
    "type": "object",
    "properties": {
        "content_type": {
            "type": "string",
            "enum": ["movie", "tvshow"],
            "description": "Whether the content is a movie or a TV show"
        },
        "confidence": {
            "type": "number",
            "description": "Confidence score between 0 and 1"
        },
        "reason": {
            "type": "string",
            "description": "Brief reason for the classification"
        }
    },
    "required": ["content_type", "confidence"]
}


CONTENT_TYPE_DETECTION_PROMPT = f"""You are an expert content classifier. Your task is to analyze the provided HTML content and determine whether it is about a MOVIE or a TV SHOW.

## How to Detect:
- **TV Show indicators**: 
  - Contains words like "Season", "Episode", "S01", "E01", "Complete Season", "All Episodes"
  - Multiple seasons mentioned (Season 1, Season 2, etc.)
  - Episode listings or episode-wise download links
  - Combo packs with season references
  - Words like "Series", "Web Series", "TV Series", "Mini Series"
  
- **Movie indicators**:
  - Single download links (480p, 720p, 1080p) without season/episode references
  - No mention of seasons or episodes
  - Words like "Movie", "Film"
  - Typically a single title with year, no season numbers

## Rules:
- Return ONLY a valid JSON object — no markdown, no backticks, no extra text
- If you see ANY season or episode references, classify as "tvshow"
- If it is a single standalone content with no season/episode info, classify as "movie"

## JSON Schema:
{json.dumps(content_type_schema, indent=2)}

## Output:
Return only the JSON object. Nothing else."""
