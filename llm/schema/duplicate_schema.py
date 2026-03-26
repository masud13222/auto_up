import json

from .blocked_names import SITE_NAME

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
        "matched_task_id": {
            "type": ["integer", "null"],
            "description": (
                "Our upload DB (MediaTask) primary key: ONLY from ### DB Candidates `id` when that block exists. "
                f"If there is NO DB Candidates block, MUST always be null — never invent an id; never use {SITE_NAME} Match `id` "
                f"({SITE_NAME} uses a different id space). "
                f"When DB candidates exist AND you skip/update/replace that specific DB row, set its `id`. "
                "When action is process, or only the target site matches (no DB row), null."
            ),
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
    "required": ["is_duplicate", "matched_task_id", "action", "reason", "detected_new_type"]
}


DUPLICATE_CHECK_PROMPT = f"""You are a media content deduplication expert. Compare a NEW incoming title against MULTIPLE existing database candidates.

## Input Format:
You will receive JSON with:
- `new_website_title`: Full raw title from the website (new)
- `new_name`: Clean extracted name
- `new_year`: Extracted year (if any)
- `candidates`: Array of existing DB entries, each with:
  - `id`: Database primary key (you MUST return this in `matched_task_id`)
  - `title`: Title stored in our database
  - `year`: Year stored in our database (from result JSON)
  - `resolutions`: Resolutions the existing entry already has (e.g. ["720p", "1080p"]) — null values are already filtered out
  - `type`: "movie" or "tvshow"
  - `episode_count`: Number of download items in existing (TV shows only)
  - `episodes`: Per-episode resolution info (TV shows only)

## STEP 0: Pick the Correct Candidate — YEAR IS CRITICAL
Look at ALL candidates. Find the one that matches BOTH title AND year.
- YEAR MUST MATCH EXACTLY. "Love 2008" ≠ "Love Express 2016". Different year = different content.
- If `new_year` is present, ONLY consider candidates whose `year` matches `new_year`.
- If NO candidate matches both title and year → action="process", matched_task_id=null.
- If exactly one matches → use that candidate for the remaining steps.
- If multiple match title+year → prefer the one with the closest title match.

## STEP 1: Detect New Content Type
From `new_website_title`, detect if the new content is a **movie** or **tvshow**.
- TV show signs: "Season", "Episode", "S01", "E01", "Complete Season", "Web Series", "Series"
- Movie signs: "Full Movie", "Movie", "Film", no season/episode references
- Set `detected_new_type` accordingly

## STEP 2: Type Mismatch Check
If `detected_new_type` is different from the matched candidate's `type`:
- If SAME title/name → the existing entry was MISCLASSIFIED → action="replace"
- If DIFFERENT title/name → genuinely different content → action="process"

## STEP 3: Resolution Comparison — FOLLOW ALL 4 SUB-STEPS EXACTLY

**Step 3a — List EXTRACTED resolutions:**
From `new_website_title`, identify all resolutions mentioned (e.g. "480p, 720p & 1080p").
Write them out explicitly: Extracted = [480p, 720p, 1080p]

**Step 3b — List EXISTING resolutions:**
Copy `resolutions` from the matched candidate exactly.
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
- Compare episode labels in candidate's `episodes` vs what the new title suggests
- New episode batch (e.g. existing ep 1-72, new ep 73-80) → has_new_episodes=true, action="update"
- Same episode range → has_new_episodes=false
- If you can't tell, default has_new_episodes=true for safety
- NEVER use "replace" for new episode batches

## Action Definitions:

**"skip"** — Nothing new on the target. Same title+year AND every resolution in Extracted already exists in Existing. Even ONE missing resolution = NOT a skip. If you matched a **DB candidate**, set matched_task_id to that candidate's id; if you matched **only** a target-site row (no DB candidates in input), matched_task_id=null.

**"update"** — Same title+year but missing resolutions OR new episodes. Set missing_resolutions. If a **DB candidate** is the match, set matched_task_id to its id; if only target-site context (no DB list), matched_task_id=null.

**"replace"** — Quality upgrade or type fix against a **DB** row; matched_task_id = that DB candidate's id. If no DB candidates in input, use action process with matched_task_id=null unless instructions say otherwise.

**"process"** — Different content or no confident match. matched_task_id=null.

## matched_task_id Rules:
- NEVER use **{SITE_NAME} Match** `id` values (e.g. 10407) — those are NOT MediaTask primary keys.
- **DB Candidates present in prompt:** skip/update/replace against a DB row → set that row's `id`; process → null.
- **No DB Candidates in prompt:** matched_task_id MUST always be null; decide skip/update/process using target-site rows + resolutions only.

## Quality Hierarchy (lowest to highest):
CAM < HDCAM < HDTS < DVDScr < DVDRip < HC-HDRip < HDRip < WEBRip < WEB-DL < BluRay < REMUX

## COMMON WRONG REASONING (never do this):
WRONG: "Title contains 'Love' and candidate also has 'Love' → same content"
WHY WRONG: "Love 2008" and "Love Express 2016" are COMPLETELY different movies. ALWAYS check year.

WRONG: "Existing has 720p, new title also has 720p → resolutions match → skip"
WHY WRONG: Ignores 480p and 1080p that are also in the new title. You MUST check ALL extracted resolutions individually.

WRONG: Picking a candidate without verifying year match.
WHY WRONG: You MUST verify title AND year match before choosing a candidate.

## CORRECT EXAMPLE:
Input: new_name="Love", new_year="2008", candidates=[{{"id":10,"title":"Love Express","year":2016,...}},{{"id":20,"title":"Love","year":2008,...}}]
Step 0: Candidate id=10 year=2016 ≠ 2008 → SKIP. Candidate id=20 year=2008 = 2008, title matches → USE id=20.
matched_task_id = 20

## The `reason` field MUST follow this format exactly:
"Matched candidate id=X. Extracted: [list]. Existing: [list]. Missing: [list]. Action: [action] because [explanation]."
Always show matched id and all three lists. Never write a vague sentence.
If no match: "No candidate matches title+year. Action: process because this is new content."

## Important:
- YEAR MISMATCH = DIFFERENT CONTENT → DO NOT MATCH
- Be STRICT about name+year matching
- SAME movie with more resolutions than the database = "update", NOT "process", NOT "skip"
- Return ONLY valid JSON — no markdown, no backticks

## JSON Schema:
{json.dumps(duplicate_schema, separators=(',',':'))}

## Output:
Return only the JSON object. Nothing else."""


