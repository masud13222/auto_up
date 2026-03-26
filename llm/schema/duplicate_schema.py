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
            "description": "MUST follow this format exactly: 'Extracted: [480p, 720p, 1080p]. Existing: [720p]. Missing: [480p, 1080p]. Action: update because 480p and 1080p are not on the site yet.' Always list all three sets before stating your action."
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

## STEP 1: Detect New Content Type
From `new_website_title`, detect if the new content is a **movie** or **tvshow**.
- TV show signs: "Season", "Episode", "S01", "E01", "Complete Season", "Web Series", "Series"
- Movie signs: "Full Movie", "Movie", "Film", no season/episode references
- Set `detected_new_type` accordingly

## STEP 2: Type Mismatch Check
If `detected_new_type` is different from `existing_type`:
- If SAME title/name → the existing entry was MISCLASSIFIED → action="replace"
- If DIFFERENT title/name → genuinely different content → action="process"

## STEP 3: Resolution Comparison — FOLLOW ALL 4 SUB-STEPS EXACTLY

**Step 3a — List EXTRACTED resolutions:**
From `new_website_title`, identify all resolutions mentioned (e.g. "480p, 720p & 1080p").
Write them out explicitly: Extracted = [480p, 720p, 1080p]

**Step 3b — List EXISTING resolutions:**
Copy `existing_resolutions` from the input JSON exactly.
Write them out explicitly: Existing = [720p]

**Step 3c — Check EACH extracted resolution ONE BY ONE:**
For every item in Extracted, ask: "Is this in Existing?"
Write out each check explicitly:
- 480p in Existing? NO → MISSING
- 720p in Existing? YES → already there
- 1080p in Existing? NO → MISSING
Missing = [480p, 1080p]

**Step 3d — Decision:**
- If Missing is EMPTY → action = "skip"
- If Missing has ANY items → action = "update", missing_resolutions = Missing list
- NEVER return "skip" if Missing is not empty

## STEP 4: Episode Comparison (TV shows only)
- Compare episode labels in existing_episodes vs what the new title suggests
- New episode batch (e.g. existing ep 1-72, new ep 73-80) → has_new_episodes=true, action="update"
- Same episode range → has_new_episodes=false
- If you can't tell, default has_new_episodes=true for safety
- NEVER use "replace" for new episode batches

## Action Definitions:

**"skip"** — Nothing new. Same title+year AND every resolution in Extracted already exists in Existing. Even ONE missing resolution = NOT a skip.

**"update"** — Same title+year but at least one resolution is missing OR new episodes found. Set missing_resolutions to the missing list.

**"replace"** — Existing is low quality (CAM/HDCAM/HDTS/DVDRip) and new is better (WEB-DL/BluRay). OR type mismatch with same title.

**"process"** — Completely different content (different title, year, or season).

## Quality Hierarchy (lowest to highest):
CAM < HDCAM < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX

## COMMON WRONG REASONING (never do this):
WRONG: "Existing has 720p, new title also has 720p → resolutions match → skip"
WHY WRONG: Ignores 480p and 1080p that are also in the new title. You MUST check ALL extracted resolutions individually.

WRONG: "Content already exists with the same resolutions → skip"
WHY WRONG: Database may only have 720p while the new title lists 480p+720p+1080p. Any resolution in the new title that is not in existing_resolutions is missing → "update".

## CORRECT EXAMPLE:
Input: new_website_title has "480p, 720p & 1080p", existing_resolutions = ["720p"]
Step 3a: Extracted = [480p, 720p, 1080p]
Step 3b: Existing = [720p]
Step 3c: 480p in Existing? NO → MISSING. 720p in Existing? YES. 1080p in Existing? NO → MISSING. Missing = [480p, 1080p]
Step 3d: Missing is NOT empty → action = "update", missing_resolutions = ["480p", "1080p"]
Reason: "Extracted: [480p, 720p, 1080p]. Existing: [720p]. Missing: [480p, 1080p]. Action: update because 480p and 1080p are not on the site yet."

## The `reason` field MUST follow this format exactly:
"Extracted: [list]. Existing: [list]. Missing: [list]. Action: [action] because [explanation]."
Always show all three lists. Never write a vague sentence.

## Important:
- Be STRICT about name matching
- Year mismatch = different content → "process"
- SAME movie with more resolutions than the database = "update", NOT "process", NOT "skip"
- Return ONLY valid JSON — no markdown, no backticks

## JSON Schema:
{json.dumps(duplicate_schema, separators=(',',':'))}

## Output:
Return only the JSON object. Nothing else."""


