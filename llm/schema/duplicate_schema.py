import json


# ───────────────────────────────────────────────
# Duplicate Detection Schema
# ───────────────────────────────────────────────

duplicate_schema = {
    "type": "object",
    "properties": {
        "is_duplicate": {
            "type": "boolean",
            "description": "True if new content is the same media as existing"
        },
        "action": {
            "type": "string",
            "enum": ["skip", "update", "replace", "process"],
            "description": "skip=identical, update=add missing parts, replace=quality upgrade, process=new content"
        },
        "reason": {
            "type": "string",
            "description": "Short explanation for the decision"
        },
        "missing_resolutions": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of resolutions the new version has that existing is missing (e.g. ['480p']). Only for 'update' action."
        },
        "has_new_episodes": {
            "type": "boolean",
            "description": "True if the new version likely has episodes not in existing. Only for TV shows."
        }
    },
    "required": ["is_duplicate", "action", "reason"]
}


DUPLICATE_CHECK_PROMPT = f"""You are a media content deduplication expert. Compare a NEW incoming title against an EXISTING database entry.

## Input Format:
You will receive JSON with:
- `new_website_title`: Full raw title from the website (new)
- `new_name`: Clean extracted name
- `new_year`: Extracted year (if any)
- `existing_title`: Title stored in our database
- `existing_resolutions`: Resolutions the existing entry already has (e.g. ["720p", "1080p"])
- `existing_type`: "movie" or "tvshow"
- `existing_episode_count`: Number of download items in existing (TV shows only)
- `existing_episode_labels`: Labels like "Episode 01-08", "Episode 09-16" (TV shows only)

## Decision Rules:

### → "skip" (is_duplicate=true)
- SAME media, SAME quality, and NO missing resolutions
- Existing already has all resolutions mentioned in new title
- For TV shows: same episodes, same resolutions → skip
- Nothing new to download

### → "update" (is_duplicate=true)
- SAME media BUT has improvements that can be ADDED:
  - Missing resolutions (e.g. existing has ["720p","1080p"], new title mentions 480p too → missing_resolutions=["480p"])
  - For TV shows: new episodes exist that aren't in existing_episode_labels → has_new_episodes=true
- Only the MISSING parts will be downloaded, not everything

### → "replace" (is_duplicate=true)
- SAME media BUT quality is UPGRADED:
  - Existing is low quality (CAM, HDCAM, HDTS, DVDRip, DVDScr, HC-HDRip) and new is better (WEB-DL, BluRay, WEBRip)
  - Complete re-download is needed because old quality is unacceptable
- This replaces the ENTIRE existing entry

### → "process" (is_duplicate=false)
- DIFFERENT media entirely
- Different movie/show name
- Different season of the same show
- Year mismatch → usually different content

## Quality Hierarchy (lowest to highest):
CAM < HDCAM < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX

## How to detect missing resolutions from title:
- If new_website_title mentions "480p, 720p & 1080p" and existing_resolutions=["720p","1080p"]
  → missing_resolutions=["480p"]
- If new_website_title mentions "720p & 1080p" and existing_resolutions=["720p","1080p"]
  → no missing, likely "skip"

## How to detect new episodes:
- Compare what existing_episode_labels cover vs what the new title suggests
- If existing has "Episode 01-08" but the show likely has more episodes now → has_new_episodes=true
- If you can't tell from the title alone, default has_new_episodes=true for safety

## Important:
- Be STRICT about name matching
- Year mismatch = different content → "process"
- Return ONLY valid JSON — no markdown, no backticks

## JSON Schema:
{json.dumps(duplicate_schema, indent=2)}

## Output:
Return only the JSON object. Nothing else."""
