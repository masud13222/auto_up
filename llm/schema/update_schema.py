import json

from .blocked_names import SITE_NAME
from .movie_schema import movie_schema
from .tvshow_schema import tvshow_schema

_COMPACT = {"separators": (",", ":")}


def get_update_system_prompt(content_type: str) -> str:
    """Build the Pass-2 delta-filter system prompt.

    Follows research best practices:
    - Role first (primacy bias)
    - Max ~10 core rules
    - Concrete few-shot examples
    - Single concern (delta filtering only)
    """
    site = SITE_NAME
    if content_type == "movie":
        schema_json = json.dumps(movie_schema, **_COMPACT)
        comparison = f"""COMPARE BY: resolution key (480p, 720p, 1080p, etc.).
- Resolution in {site} SEARCH CONTEXT → remove from output.
- Resolution NOT in {site} SEARCH CONTEXT → keep in output."""
        examples = f"""### EXAMPLE 1 — missing resolution:
INPUT: PASS-1 DATA has 480p, 720p, 1080p. {site} SEARCH CONTEXT has 720p, 1080p.
OUTPUT: {{"action":"update","reason":"Missing 480p resolution","data":{{"download_links":{{"480p":[{{"u":"https://x.com/480","l":"Hindi","f":"Movie.480p.{SITE_NAME}.mkv"}}]}}}}}}

### EXAMPLE 2 — nothing missing:
INPUT: PASS-1 DATA has 720p, 1080p. {site} SEARCH CONTEXT has 720p, 1080p.
OUTPUT: {{"action":"skip","reason":"All resolutions already exist on {site} (SEARCH CONTEXT)","data":{{"download_links":{{}}}}}}"""
    else:
        schema_json = json.dumps(tvshow_schema, **_COMPACT)
        comparison = f"""COMPARE BY: season_number + episode_range + resolution key.
- Same season + same range + same resolution in {site} SEARCH CONTEXT → remove.
- Same season + same range + missing resolution → keep ONLY missing resolutions.
- Entirely new episode_range → keep with ALL its resolutions.
- Whole season fully covered → omit entire season."""
        examples = f"""### EXAMPLE 1 — missing resolution:
INPUT: PASS-1 DATA S05 EP41-48 has 480p, 720p, 1080p. {site} SEARCH CONTEXT S05 EP41-48 has 720p, 1080p.
OUTPUT: {{"action":"update","reason":"S05 EP41-48: missing 480p","data":{{"seasons":[{{"season_number":5,"download_items":[{{"type":"partial_combo","label":"Episode 41-48","episode_range":"41-48","resolutions":{{"480p":[{{"u":"https://x.com/480","l":"Bengali","f":"S05.EP41-48.480p.{SITE_NAME}.mkv"}}]}}}}]}}]}}}}
720p/1080p exist → removed. Only 480p kept.

### EXAMPLE 2 — new episode range:
INPUT: PASS-1 DATA has S02 EP01-06 (480p,720p,1080p) + EP07-12 (720p,1080p). {site} SEARCH CONTEXT has S02 EP01-06 (720p,1080p).
OUTPUT: {{"action":"update","reason":"S02 EP01-06: missing 480p; EP07-12: entirely new range","data":{{"seasons":[{{"season_number":2,"download_items":[{{"type":"partial_combo","label":"Episode 01-06","episode_range":"01-06","resolutions":{{"480p":[...]}}}},{{"type":"partial_combo","label":"Episode 07-12","episode_range":"07-12","resolutions":{{"720p":[...],"1080p":[...]}}}}]}}]}}}}
EP01-06: only 480p (720p/1080p exist). EP07-12: all resolutions (new range).

### EXAMPLE 3 — nothing missing:
INPUT: {site} SEARCH CONTEXT covers everything in PASS-1 DATA.
OUTPUT: {{"action":"skip","reason":"All episode ranges and resolutions already covered","data":{{"seasons":[]}}}}"""

    return f"""You are a delta filter. One job: compare PASS-1 DATA against {site} SEARCH CONTEXT (target upload site), return only what is missing.

RULES:
1. Return ONLY valid JSON. No markdown, no extra text.
2. Copy URLs, filenames, language values EXACTLY from PASS-1 DATA. Never modify.
3. Remove anything that already exists in {site} SEARCH CONTEXT.
4. Keep all metadata (title, year, genre, poster_url, etc.) from PASS-1 DATA as-is.
5. Only modify download_links (movie) or seasons (tvshow).
6. Set "action": "update" if there IS missing data.
7. Set "action": "skip" if NOTHING is missing — you are the final judge, override any hints.
8. Always include "reason": a single line explaining what is missing or why skipping.
9. NEVER use DB Candidates `episodes` field for comparison — it is ignored entirely.
10. A DB Candidates match never changes `action`; decide `update` vs `skip` only from PASS-1 DATA vs {site} SEARCH CONTEXT.

{comparison}

{examples}

Data schema:
```json
{schema_json}
```

Return: {{"action":"update" or "skip","reason":"...","data":{{...}}}}"""
