import json

from .blocked_names import SITE_NAME
from .movie_schema import movie_schema
from .tvshow_schema import tvshow_schema

_COMPACT = {"separators": (",", ":")}


def get_update_system_prompt(content_type: str) -> str:
    """Build the Pass-2 delta-filter system prompt.

    Uses the same movie/tvshow schema as Pass-1 — output structure is identical,
    but only the missing/new data should be returned.
    """
    if content_type == "movie":
        schema_json = json.dumps(movie_schema, **_COMPACT)
        type_rules = """MOVIE COMPARISON:
- Compare by resolution key (480p, 720p, 1080p, etc.).
- If a resolution exists in SEARCH CONTEXT → omit it entirely from output.
- Only return resolutions that are NOT in SEARCH CONTEXT."""
        example = f"""EXAMPLE:
SEARCH CONTEXT has: 720p, 1080p
PASS-1 RESPONSE has: 480p, 720p, 1080p
CORRECT OUTPUT: {{"content_type":"movie","data":{{"download_links":{{"480p":[{{"u":"https://x.com/480","l":"Hindi","f":"Movie.480p.{SITE_NAME}.mkv"}}]}}}}}}
720p and 1080p exist → omitted. Only 480p returned.
WRONG: returning 720p or 1080p that already exist."""
    else:
        schema_json = json.dumps(tvshow_schema, **_COMPACT)
        type_rules = """TV COMPARISON:
- Compare by season_number + episode_range + resolution key.
- Same season + same episode_range + same resolution in SEARCH CONTEXT → omit that resolution.
- Same season + same episode_range but some resolutions missing → keep ONLY missing resolutions under that item.
- Entirely new episode_range (not in SEARCH CONTEXT at all) → include with all its resolutions.
- If a whole season is fully covered → omit entire season from output.
- If nothing is missing → return empty seasons array."""
        example = f"""EXAMPLE 1 — missing resolution:
SEARCH CONTEXT: S05 Episode 41-48 has 720p, 1080p
PASS-1 RESPONSE: S05 Episode 41-48 has 480p, 720p, 1080p
CORRECT OUTPUT: data.seasons has S05 EP41-48 with ONLY 480p (720p/1080p exist → omitted).

EXAMPLE 2 — new episode range:
SEARCH CONTEXT: S02 Episode 01-06 has 720p, 1080p
PASS-1 RESPONSE: S02 EP01-06 (480p, 720p, 1080p) + EP07-12 (720p, 1080p)
CORRECT OUTPUT: data.seasons has EP01-06 with ONLY 480p + EP07-12 with ALL resolutions (entirely new range).

EXAMPLE 3 — nothing missing:
SEARCH CONTEXT has all resolutions for all episode ranges in PASS-1 RESPONSE.
CORRECT OUTPUT: {{"content_type":"tvshow","data":{{"seasons":[]}}}}"""

    return f"""You are a delta filter. Your ONLY job: compare PASS-1 RESPONSE with SEARCH CONTEXT and return ONLY what is missing.

The output uses the SAME schema as PASS-1 — but include ONLY the missing/new items.

RULES:
1. Return ONLY valid JSON. No markdown fences, no extra text.
2. Never modify URLs, filenames, or language values from PASS-1 RESPONSE. Copy exactly.
3. If something exists in SEARCH CONTEXT, it MUST NOT appear in your output.
4. If nothing is missing, return empty (movie: empty download_links, tvshow: empty seasons array).
5. If you determine the UPDATE HINT is wrong and actually nothing needs updating, return empty. This overrides the hint — you are the final judge.
6. Keep all metadata fields (title, year, genre, poster_url, etc.) from PASS-1 RESPONSE as-is. Only filter download_links (movie) or seasons (tvshow).

{type_rules}

{example}

Output schema (same as extraction):
```json
{schema_json}
```

Return ONLY: {{"content_type":"{content_type}","data":{{...}}}}"""
