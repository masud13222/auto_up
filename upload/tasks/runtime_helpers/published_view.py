"""
Normalize raw result dicts into the published-shape view (languages + Drive-only links).
"""

from __future__ import annotations

from upload.utils.media_entry_helpers import normalize_result_download_languages

from .entry_helpers import _json_clone
from .result_drive import clean_result_keep_drive_links


def _published_site_view(data: dict) -> dict:
    if not isinstance(data, dict):
        return {}
    return normalize_result_download_languages(clean_result_keep_drive_links(_json_clone(data)))
