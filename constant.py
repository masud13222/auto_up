"""
Project-wide tuning values (DB duplicate search, FlixBD, LLM caps, subtitle/remux).

Import from the project root, e.g. ``from constant import FUZZY_THRESHOLD_DB``.
"""

import os

# --- MediaTask / DB fuzzy duplicate search ---
FUZZY_THRESHOLD_DB = 75

# Max DB candidate rows after fuzzy sort (upload combined LLM duplicate section).
DB_DUPLICATE_LLM_MAX_CANDIDATES = 3

# auto_up LLM filter: max rows per scraped item (DB and FlixBD are separate lists; merge is per-source).
AUTO_UP_DB_LLM_MAX_CANDIDATES = 2
AUTO_UP_FLIXBD_LLM_MAX_RESULTS = 2

# Per-keyword ORM slice when broad-fetching candidates (before merge + fuzzy).
DB_SEARCH_QUERY_SLICE_UPLOAD = 10
DB_SEARCH_QUERY_SLICE_AUTO_UP = 15

# --- Source URL → forced adult flag (upload pipeline) ---
# ICANN registrable *domain* label (before public suffix), case-insensitive. Matches that
# name on any TLD (primehub.me, primehub.to, www.primehub.com) via ``tldextract`` + PSL.
# Do not use bare multi-tenant suffixes as a "label" (e.g. avoid listing ``appspot`` alone).
#
# Override with env (comma-separated, case-insensitive). Unset = use default below.
# Example: FORCE_IS_ADULT_SOURCE_ROOT_DOMAIN_LABELS=primehub,othersite
# Set to empty string to disable forcing (no labels).
_FORCE_IS_ADULT_SOURCE_ROOT_DOMAIN_LABELS_ENV = "FORCE_IS_ADULT_SOURCE_ROOT_DOMAIN_LABELS"
_DEFAULT_FORCE_IS_ADULT_ROOT_LABELS: tuple[str, ...] = ("primehub",)


def _force_is_adult_root_domain_labels_from_env() -> list[str]:
    raw = os.environ.get(_FORCE_IS_ADULT_SOURCE_ROOT_DOMAIN_LABELS_ENV)
    if raw is None:
        return list(_DEFAULT_FORCE_IS_ADULT_ROOT_LABELS)
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    return parts


FORCE_IS_ADULT_SOURCE_ROOT_DOMAIN_LABELS = _force_is_adult_root_domain_labels_from_env()

# --- FlixBD search (merged phases, fuzzy trim, LLM slim rows) ---
# Upload ``process_media_task`` combined LLM: max slim FlixBD rows after fuzzy.
FLIXBD_LLM_MAX_RESULTS = 5
FLIXBD_SEARCH_PER_PAGE = 20
FLIXBD_FUZZY_THRESHOLD = 80

# --- Subtitle strip / FFmpeg remux (upload.utils.subtitle_remove) ---
# First N subtitle cues (per stream) scanned for blocklisted names in dialogue text.
SUBTITLE_CONTENT_SCAN_MAX_EVENTS = 30
# Remux: first attempt + (value - 1) retries (transient IO, busy file, etc.).
REMUX_MAX_ATTEMPTS = 2
