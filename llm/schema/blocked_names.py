# ───────────────────────────────────────────────
# Shared Config — Site name & blocked names
# ───────────────────────────────────────────────

SITE_NAME = "FlixBD"

# LLM duplicate_check JSON property: content row id on SITE_NAME (API movie/series id).
# The server never infers this — only the model returns it or null. Not a MediaTask pk.
TARGET_SITE_ROW_ID_JSON_KEY = "target_site_row_id"

# Site names to strip from extracted titles/filenames
BLOCKED_SITE_NAMES = [
    "cinefreak", "cinefreak.net", "cinefreak.top",
    "mlsbd", "mlsbd.shop",
    "cinemaza", "mkvking", "hdmovie99",
    "moviesmod", "vegamovies", "katmoviehd",
    "extramovies", "filmyzilla", "bolly4u",
    "themoviesflix", "movieverse",
]
