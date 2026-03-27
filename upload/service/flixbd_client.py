"""
Compatibility wrapper for the FlixBD API client.

The implementation now lives in smaller modules so the task/runtime code can
stay easier to review while external imports keep using
``upload.service.flixbd_client``.
"""

from .flixbd_api_base import (
    _FLIXBD_MAX_META_DESCRIPTION,
    _FLIXBD_MAX_META_KEYWORDS,
    _FLIXBD_MAX_META_TITLE,
    _TIMEOUT,
    _get_config,
    _headers,
    _safe_json,
    logger,
)
from .flixbd_api_content import (
    _build_movie_payload,
    _build_series_payload,
    _derive_language_string,
    _display_movie_title,
    _display_series_title,
    _parse_episode_number,
    _set_if,
    _set_if_truncated,
    _truncate_api_text,
    create_movie,
    create_series,
    format_file_size,
    movie_website_title,
    patch_movie_title,
    patch_series_title,
    search,
    series_website_title,
    update_movie,
    update_series,
)
from .flixbd_api_downloads import (
    _coerce_season_number,
    _episode_number_for_flixbd_item,
    _parse_episode_range_field,
    add_movie_download_links,
    add_series_download_links,
    clear_movie_download_links,
    clear_series_download_links,
    clear_series_download_links_for_scope,
    delete_movie_download,
    delete_series_download,
    fetch_movie_drive_links_by_quality,
    fetch_series_drive_links_tree,
    list_movie_downloads,
    list_series_downloads,
)
