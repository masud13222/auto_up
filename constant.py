"""
Project-wide tuning values (DB duplicate search, FlixBD search, LLM context caps).

Import from the project root, e.g. ``from constant import FUZZY_THRESHOLD_DB``.
"""

# --- MediaTask / DB fuzzy duplicate search ---
FUZZY_THRESHOLD_DB = 80

# Max DB candidate rows after fuzzy sort (upload combined LLM duplicate section).
DB_DUPLICATE_LLM_MAX_CANDIDATES = 5

# auto_up LLM filter: max rows per scraped item (DB and FlixBD are separate lists; merge is per-source).
AUTO_UP_DB_LLM_MAX_CANDIDATES = 2
AUTO_UP_FLIXBD_LLM_MAX_RESULTS = 2

# Per-keyword ORM slice when broad-fetching candidates (before merge + fuzzy).
DB_SEARCH_QUERY_SLICE_UPLOAD = 10
DB_SEARCH_QUERY_SLICE_AUTO_UP = 15

# --- FlixBD search (merged phases, fuzzy trim, LLM slim rows) ---
# Upload ``process_media_task`` combined LLM: max slim FlixBD rows after fuzzy.
FLIXBD_LLM_MAX_RESULTS = 3
FLIXBD_SEARCH_PER_PAGE = 20
FLIXBD_FUZZY_THRESHOLD = 80
