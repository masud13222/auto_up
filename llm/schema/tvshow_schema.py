import json
from .blocked_names import BLOCKED_SITE_NAMES, SITE_NAME

_blocked_names_str = ", ".join(BLOCKED_SITE_NAMES)

# ───────────────────────────────────────────────
# TV Show Schema
# ───────────────────────────────────────────────

tvshow_schema = {
    "type": "object",
    "properties": {
        "website_tvshow_title": {
            "type": "string",
            "description": (
                f"Series display title — must include Season and episode scope (movie titles do not use Season/EP). "
                f"Exact format: 'Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}'. "
                f"NN = zero-padded season (01, 02, …). "
                f"EP: single episode → 'EP05'; range → 'EP01-03' (always zero-pad). "
                f"Whole season one pack (combo_pack) with no episode split → use 'Season NN Complete' instead of EP… "
                f"Source = WEB-DL / CAMRip / HDRip / BluRay / WEBRip / HDTS (NOT 480p/720p/1080p). "
                f"Strip blocked site names. "
                f"Example: 'Single Papa 2025 Season 01 EP01-06 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}'"
            )
        },
        "title": {
            "type": "string",
            "description": "Clean show name only — no year, quality, or language tags"
        },
        "year":        {"type": "integer", "description": "Release year (integer only)"},
        "genre":       {"type": "string"},
        "director":    {"type": "string"},
        "rating":      {"type": "number", "description": "Numeric only, e.g. 7.5"},
        "plot":        {"type": "string"},
        "poster_url":  {"type": "string", "description": "Main poster image URL"},
        "meta_title": {"type": "string", "description": "Natural SEO title (50-60 chars). Place main keyword early. Vary structure — avoid repeating the same pattern."},
        "meta_description": {"type": "string", "description": "Compelling meta description (140-160 chars). Natural language with CTA. Include show name, year/season, quality, language."},
        "meta_keywords": {"type": "string", "description": "10-15 comma-separated SEO keywords. Include name variations, season info, quality variants, language, 'download', 'watch online'."},
        "total_seasons": {"type": "integer"},
        "cast_info": {
            "type": "string",
            "description": "Comma-separated cast/actors list from the page."
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
            "description": "IMDb ID if found on the page (e.g. 'tt0903747'). Omit if not present."
        },
        "tmdb_id": {
            "type": "string",
            "description": "TMDB ID if found on the page (e.g. '1396'). Omit if not present."
        },
        "is_adult": {
            "type": "boolean",
            "description": (
                "True only if the series/page is clearly adult-only or explicit erotic content: "
                "e.g. 18+, Adults only, A-rated adult, XXX, uncensored adult anime. "
                "False for mainstream series (including mature themes, violence, or UNRATED episodes that are not "
                "marketed as adult erotica). When unsure, false."
            ),
        },
        "seasons": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "season_number": {"type": "integer"},
                    "download_items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["combo_pack", "partial_combo", "single_episode"]
                                },
                                "label":         {"type": "string", "description": "E.g. 'Season 1 Combo Pack', 'Season 2 Episode 01-08', 'Season 3 Episode 05'"},
                                "episode_range": {
                                    "type": "string",
                                    "description": (
                                        "REQUIRED for type single_episode and partial_combo — omit the key entirely only for combo_pack. "
                                        "Episode number(s) for this download row only, zero-padded 2 digits: single → '01', '12'; "
                                        "range (partial_combo) → '01-08', '10-12'. "
                                        "Never put the season index here (season is seasons[].season_number on the parent)."
                                    ),
                                },
                                "resolutions": {
                                    "type": "object",
                                    "additionalProperties": {"type": "string"},
                                    "description": (
                                        "File DOWNLOAD URLs per resolution only ('480p', '720p', …). "
                                        "Real download/generate.php/Download-button targets. "
                                        "Never watch/Watch Resolution/stream/player URLs — omit resolution if only streaming exists."
                                    ),
                                }
                            },
                            "required": ["type", "label", "resolutions"]
                        }
                    }
                },
                "required": ["season_number", "download_items"]
            }
        }
    },
    "required": ["website_tvshow_title", "title", "year", "is_adult"]
}

# ───────────────────────────────────────────────
# TV Show System Prompt
# ───────────────────────────────────────────────

TVSHOW_SYSTEM_PROMPT = f"""You are a web scraping assistant. Extract TV show data from HTML and return a single valid JSON object. No markdown, no backticks, no extra text.

## GENERAL RULES:
- Omit missing fields entirely (no null, no empty strings)
- Strip these site names from ALL fields (including website_tvshow_title): {_blocked_names_str}
- Prefer x264 encodes when multiple encode options exist
- rating: numeric only (e.g. 7.5) | year: integer only (e.g. 2024)
- cast_info: comma-separated actors if listed on page
- languages: array of audio languages found on page (e.g. ["Hindi", "English"] or ["Bengali"])
- countries: array of production countries from page (e.g. ["USA"])
- **is_adult** (required boolean): `true` only for clear adult/explicit erotic series (18+, Adults only, XXX, adult anime). `false` for mainstream shows including mature drama/violence. If ambiguous → `false`.

## IMPORTANT - website_tvshow_title (series — different format from movies, includes Season + EP):
**Must** include **Season** and **episode scope** before Source (movies do not use Season/EP):

**Format:**
`Title Year Season NN EPxx[-yy] Source Language - {SITE_NAME}`

Rules:
- **Title**: clean series name (no year/quality in title part — year follows separately)
- **Year**: 4-digit (series/first air or page year)
- **Season NN**: zero-padded, e.g. `Season 01`, `Season 02` (same style for all seasons)
- **Episode scope** (pick one):
  - **Range** (partial_combo or page says Ep 1–3): `EP01-03` (zero-pad both ends)
  - **Single episode**: `EP05` only
  - **Full season one block** (combo_pack, no per-episode split): use **`Season NN Complete`** and **omit** the EP segment — e.g. `Show 2020 Season 01 Complete WEB-DL Bengali - {SITE_NAME}`
- **Source**: WEB-DL, CAMRip, HDRip, BluRay, WEBRip, HDTS — **not** 1080p/720p
- **Language**: e.g. `Dual Audio [Hindi ORG. + English]`, `Bengali`
- **{SITE_NAME}**: always ` - {SITE_NAME}` at the end

Derive NN for `Season NN` from `seasons[].season_number`. Derive EP segment from `download_items[].episode_range` when set (preferred); use labels only if range is absent. If several items in the **same** season, mirror the **page’s main** batch; do not invent ranges.

Examples:
- `Single Papa 2025 Season 01 EP01-03 WEB-DL Dual Audio [Hindi ORG. + English] - {SITE_NAME}`
- `Breaking Bad 2008 Season 01 Complete WEB-DL Bengali - {SITE_NAME}`
- `Daredevil Born Again 2026 Season 02 EP01 WEB-DL English - {SITE_NAME}`

## SEO Meta Fields (MUST generate — do NOT skip):
- **meta_title**: Create a natural, human-like SEO title (50-60 chars). Place the show name early. Vary structure across pages. Include season info if applicable.
  Good examples:
  - "Money Heist Season 1-5 Hindi Dubbed 1080p Download"
  - "Download Mirzapur S3 (2025) Hindi WEB-DL 480p 720p"
  - "Squid Game Season 2 Korean 1080p WEB-DL Full Series"
- **meta_description**: Write a compelling, natural meta description (140-160 chars). Use human language — NOT a keyword list. Include a CTA. Mention show name, season, quality, language.
  Good examples:
  - "Download Money Heist all seasons in Hindi Dubbed. 480p to 1080p WEB-DL with subtitles. Fast GDrive links. Binge-watch the complete series now."
  - "Stream or download Mirzapur Season 3 in 480p, 720p & 1080p. Direct links, no waiting. Get all episodes instantly."
- **meta_keywords**: Generate 10-15 relevant, comma-separated keywords. Include show name variations, season info, quality variants, language, "download", "watch online", "all episodes", genre.
  Example: "Money Heist, Money Heist Hindi, Money Heist download, Money Heist Season 1, Money Heist 1080p, Hindi dubbed series, Netflix, WEB-DL, all episodes, watch online, GDrive"


## DOWNLOAD STRUCTURE:
Classify each download item based ENTIRELY on the page's HTML structure — not the specific episode numbers or how many resolution buttons appear.

**`combo_pack`** — One section covers the ENTIRE season with no episode breakdown in the heading.

**`partial_combo`** — The heading contains a NUMBER RANGE (two episode numbers joined by a hyphen, dash, or "to"), indicating multiple episodes in one bundle. The defining signal is the range in the label — NOT how many resolution buttons exist. Even a single resolution button qualifies.

**`single_episode`** — Each individual episode has its OWN separate heading/section. No range — each heading refers to exactly one episode.

### ✅ Decision order — classify by structure:
1. Heading covers the WHOLE season without episode breakdown → `combo_pack`
2. Heading contains a RANGE (any two episode identifiers connected) → `partial_combo`; set `episode_range` to that range as-is from the page
3. Heading refers to exactly ONE episode → `single_episode`

### ✅ episode_range (must be present for every non-combo row):
- **`single_episode`**: always set `episode_range` to that episode only, **zero-padded** (e.g. `"01"`, `"09"`, `"12"`). Downstream APIs use this field — do not rely on the label alone.
- **`partial_combo`**: always set `episode_range` to the inclusive range, **zero-padded both ends** (e.g. `"01-08"`, `"10-12"`).
- **`combo_pack`**: **omit** `episode_range` (whole season; no per-episode row).
- Season belongs in `seasons[].season_number` only — **never** encode season inside `episode_range`.

### ⚠️ Strict rules:
- Resolution button count (1, 2, or 3+) NEVER changes the type — a range heading is always `partial_combo`
- Do NOT merge separate single-episode sections into a range
- Do NOT split a range heading into individual single episodes

## NO DUPLICATE EPISODES (strict priority):
1. If `combo_pack` present for the season → include ONLY the combo_pack
2. If `partial_combo` covers a range → do NOT add singles for episodes within that range
3. `single_episode` only for episodes not covered by any combo or partial

## CORRECT EXAMPLE (generic pattern):
Pagestructure: [Season A — full season section] | [Season B — range section] | [Season B — another range section] | [Season B — individual episode sections]

Output:
- Season A → one `combo_pack` only
- Season B → two `partial_combo` items (one per range section) + one `single_episode` per individual episode section

## JSON SCHEMA:
{json.dumps(tvshow_schema, indent=2)}

Return only the JSON object. Nothing else."""


# ───────────────────────────────────────────────
# TV Show Filename Schema
# ───────────────────────────────────────────────

tvshow_filename_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "season_number": {"type": "integer"},
            "type": {"type": "string", "enum": ["combo_pack", "partial_combo", "single_episode"]},
            "label": {"type": "string", "description": "Exact label from the download item"},
            "resolutions": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": "Filenames keyed by resolution. Only include resolutions present in the download item."
            }
        },
        "required": ["season_number", "type", "label", "resolutions"]
    }
}

TVSHOW_FILENAME_SYSTEM_PROMPT = f"""You are a filename generator for TV show downloads.

Given TV show JSON, generate standardized filenames for every download item. Return a JSON array. No markdown, no backticks.

## FORMAT BY TYPE:
- combo_pack:    Title.Year.S01.Complete.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- partial_combo: Title.Year.S01E01-E08.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- single_episode: Title.Year.S01E05.Resolution.Source.WEB-DL.x264.{SITE_NAME}.mkv
- combo_pack archives (.zip/.rar): use matching extension instead of .mkv

## SOURCE DETECTION (from website_tvshow_title):
Netflix/NF → NF | Amazon/AMZN → AMZN | Hotstar/DSNP → DSNP | Jio/JC → JC | ZEE5/Zee5 → ZEE5 | none matched → WEB-DL

## RULES:
- Use dots instead of spaces
- Last part before extension MUST be "{SITE_NAME}"
- Only generate filenames for resolutions that exist in the item
- NEVER include site names: {_blocked_names_str}
- One entry per download_item (match by season_number + type + label)

## JSON SCHEMA:
{json.dumps(tvshow_filename_schema, indent=2)}

Return only the JSON array. Nothing else."""