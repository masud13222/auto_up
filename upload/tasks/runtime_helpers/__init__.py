"""
Runtime helpers for django-q worker: duplicates, snapshots, merges, FlixBD search rows.

Submodules group code by responsibility (entry normalization, drive-link cleanup, site sync,
FlixBD fetch, merge logic, duplicate candidates). Import from ``upload.tasks.runtime_helpers``
unchanged — this package re-exports the same public surface as the legacy single module.
"""

from __future__ import annotations

from .duplicate_candidates import (
    build_db_candidate,
    build_db_match_candidates,
    donor_result_for_site_content,
    flixbd_site_id_set,
    normalize_duplicate_response,
)
from .flixbd_search import (
    _flixbd_merge_two_phase_raw,
    _flixbd_title_fuzzy_score,
    fetch_flixbd_results,
    flixbd_search_query,
    flixbd_slim_qualities_from_download_links,
    normalize_flixbd_resolution_keys,
)
from .merge_engine import merge_drive_links, merge_new_episodes
from .result_drive import (
    clean_result_keep_drive_links,
    has_drive_links,
    result_strip_non_drive_download_links,
)
from .site_sync_snapshot import (
    build_site_sync_snapshot,
    extract_site_sync_snapshot_result,
    hydrate_existing_result_from_snapshot,
    overlay_site_sync_snapshot,
    save_publish_state_with_snapshot,
    save_site_sync_snapshot,
    strip_movie_download_entries_by_flixbd_failures,
    strip_tvshow_download_entries_by_flixbd_failures,
)

__all__ = [
    "_flixbd_merge_two_phase_raw",
    "_flixbd_title_fuzzy_score",
    "build_db_candidate",
    "build_db_match_candidates",
    "build_site_sync_snapshot",
    "clean_result_keep_drive_links",
    "donor_result_for_site_content",
    "extract_site_sync_snapshot_result",
    "fetch_flixbd_results",
    "flixbd_search_query",
    "flixbd_site_id_set",
    "flixbd_slim_qualities_from_download_links",
    "has_drive_links",
    "hydrate_existing_result_from_snapshot",
    "merge_drive_links",
    "merge_new_episodes",
    "normalize_duplicate_response",
    "normalize_flixbd_resolution_keys",
    "overlay_site_sync_snapshot",
    "result_strip_non_drive_download_links",
    "save_publish_state_with_snapshot",
    "save_site_sync_snapshot",
    "strip_movie_download_entries_by_flixbd_failures",
    "strip_tvshow_download_entries_by_flixbd_failures",
]
