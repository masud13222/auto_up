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
            "description": "skip=identical, update=add missing parts/episodes, replace=quality upgrade, process=new content"
        },
        "reason": {
            "type": "string",
            "description": "Short explanation for the decision"
        },
        "detected_new_type": {
            "type": "string",
            "enum": ["movie", "tvshow"],
            "description": "What you detect the NEW content to be (movie or tvshow) from the website title"
        },
        "missing_resolutions": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of resolutions the new version has that existing is missing (e.g. ['480p']). Only for 'update' action."
        },
        "has_new_episodes": {
            "type": "boolean",
            "description": "True if the new URL has episode labels NOT present in existing_episodes. When true, new episodes will be APPENDED (not replaced)."
        }
    },
    "required": ["is_duplicate", "action", "reason", "detected_new_type"]
}


DUPLICATE_CHECK_PROMPT = f"""You are a media content deduplication expert. Compare a NEW incoming title against an EXISTING database entry.

## Input Format:
You will receive JSON with:
- `new_website_title`: Full raw title from the website (new)
- `new_name`: Clean extracted name
- `new_year`: Extracted year (if any)
- `existing_title`: Title stored in our database
- `existing_resolutions`: Resolutions the existing entry already has (e.g. ["720p", "1080p"]) — null values are already filtered out
- `existing_type`: "movie" or "tvshow"
- `existing_episode_count`: Number of download items in existing (TV shows only)
- `existing_episodes`: Per-episode resolution info like ["Episode 01: 480p,720p,1080p", "Season 5 Episode 73-80: 720p,1080p"] (TV shows only)
  → This tells you EXACTLY which episodes have which resolutions. Null/empty resolution values are already filtered.

## STEP 1: Detect New Content Type
First, from `new_website_title`, detect if the new content is a **movie** or **tvshow**.
- TV show signs: "Season", "Episode", "S01", "E01", "Complete Season", "Web Series", "Series"
- Movie signs: "Full Movie", "Movie", "Film", no season/episode references
- Set `detected_new_type` accordingly

## STEP 2: Type Mismatch Check
If `detected_new_type` ≠ `existing_type`:
- If SAME title/name → the existing entry was MISCLASSIFIED → action="replace"
  - Example: existing="Sa Re Ga Ma Pa Legends" (movie) vs new="Sa Re Ga Ma Pa Legends Season 22" (tvshow) → REPLACE (same content, wrong classification)
- If DIFFERENT title/name → genuinely different content → action="process"

## STEP 3: Same-Type Comparison (only if types match OR already decided)

### → "skip" (is_duplicate=true)
- SAME media, SAME quality, and NO missing resolutions AND NO new episodes
- Existing already has all resolutions AND all episode labels mentioned in new title
- Nothing new to download

### → "update" (is_duplicate=true)
- SAME media BUT has improvements that can be ADDED WITHOUT replacing existing data:
  - Missing resolutions (e.g. existing has ["720p","1080p"], new title mentions 480p too → missing_resolutions=["480p"])
  - NEW episode labels NOT found in existing_episodes → has_new_episodes=true
- **IMPORTANT for TV shows**: When a new URL contains a DIFFERENT episode batch (e.g. existing has ep 1-72, new URL has ep 73-80), this is ALWAYS "update" with has_new_episodes=true.
  - The new episodes will be APPENDED to the existing ones — existing Drive links are NEVER touched.
  - NEVER use "replace" for this case.

### → "replace" (is_duplicate=true)
- SAME media BUT quality is UPGRADED:
  - Existing is low quality (CAM, HDCAM, HDTS, DVDRip, DVDScr, HC-HDRip) and new is better (WEB-DL, BluRay, WEBRip)
  - Complete re-download is needed because old quality is unacceptable
- This replaces the ENTIRE existing entry
- ALSO use "replace" when existing_type ≠ detected_new_type BUT same title/year (misclassification)

### → "process" (is_duplicate=false)
- DIFFERENT media entirely
- Different movie/show name
- Different season of the same show (e.g. Season 4 vs Season 5)
- Year mismatch → usually different content

## Quality Hierarchy (lowest to highest):
CAM < HDCAM < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX

## How to detect missing resolutions from title:
- If new_website_title mentions "480p, 720p & 1080p" and existing_resolutions=["720p","1080p"]
  → missing_resolutions=["480p"]
- If new_website_title mentions "720p & 1080p" and existing_resolutions=["720p","1080p"]
  → no missing, likely "skip"

## How to detect new episodes:
- Compare episode labels in existing_episodes vs what the new title suggests.
- If existing has "Season 5 Episode 1-72" but new title mentions "Season 5 Episode 73-80" → has_new_episodes=true, action="update"
- If existing episodes and new title describe the SAME episode range → has_new_episodes=false
- If you can't tell from the title alone, default has_new_episodes=true for safety.
- NEVER use "replace" when the only difference is a new episode batch.

## Important:
- Be STRICT about name matching
- Year mismatch = different content → "process"
- Same show, same season, new episode batch = "update" with has_new_episodes=true (NEVER "replace")
- Return ONLY valid JSON — no markdown, no backticks

## JSON Schema:
{json.dumps(duplicate_schema, indent=2)}

## Output:
Return only the JSON object. Nothing else."""


