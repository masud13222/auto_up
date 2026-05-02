"""
Low-level normalization for snapshot entries (links, filenames, FlixBD row ids).

Other runtime_helpers subpackages import from here; keep this module dependency-light.
"""

from __future__ import annotations

import json

from upload.utils.media_entry_helpers import (
    coerce_download_source_value,
    coerce_entry_language_value,
    entry_language_key,
    is_drive_link,
    primary_download_source_url,
)


def _normalize_flixbd_row_id(fid) -> int | str | None:
    """
    Canonical id for merge/dedupe so API ``201`` and ``\"201\"`` map to the same key.
    Prefer positive int; otherwise non-empty stripped string; else None.
    """
    if fid is None or isinstance(fid, bool):
        return None
    if isinstance(fid, int):
        return fid if fid > 0 else None
    if isinstance(fid, str):
        s = fid.strip()
        if not s:
            return None
        if s.isdigit():
            v = int(s)
            return v if v > 0 else None
        return s
    try:
        v = int(fid)
        return v if v > 0 else None
    except (TypeError, ValueError):
        s = str(fid).strip()
        return s or None


def _entry_language_key(entry: dict) -> str:
    return entry_language_key((entry or {}).get("l"))


def _entry_link(entry: dict) -> str:
    return primary_download_source_url((entry or {}).get("u"))


def _entry_filename(entry: dict) -> str:
    return str((entry or {}).get("f") or "").strip()


def _entry_copy(entry: dict, *, link: str) -> dict:
    out = {
        "u": coerce_download_source_value(link),
        "l": coerce_entry_language_value(entry.get("l")),
        "f": _entry_filename(entry),
    }
    if isinstance(entry.get("s"), str) and entry["s"].strip():
        out["s"] = entry["s"].strip()
    return out


def _json_clone(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def _entry_size(entry: dict) -> str:
    return str((entry or {}).get("s") or "").strip()


def _tv_download_item_has_any_drive_link(item: dict) -> bool:
    res = item.get("resolutions")
    if not isinstance(res, dict):
        return False
    for entries in res.values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict) and is_drive_link(_entry_link(entry)):
                return True
    return False


def _same_snapshot_entry(existing: dict, incoming: dict) -> bool:
    if _entry_language_key(existing) != _entry_language_key(incoming):
        return False
    existing_filename = _entry_filename(existing).lower()
    incoming_filename = _entry_filename(incoming).lower()
    if existing_filename and incoming_filename:
        return existing_filename == incoming_filename
    existing_size = _entry_size(existing).lower()
    incoming_size = _entry_size(incoming).lower()
    if existing_size and incoming_size:
        return existing_size == incoming_size
    existing_link = _entry_link(existing)
    incoming_link = _entry_link(incoming)
    if existing_link and incoming_link:
        return existing_link == incoming_link
    return False


def _snapshot_entry_with_metadata(incoming: dict, base_entries: list) -> dict:
    incoming_copy = _json_clone(incoming)
    incoming_copy.setdefault("f", "")
    incoming_copy["l"] = coerce_entry_language_value(incoming_copy.get("l"))
    incoming_copy["u"] = coerce_download_source_value(incoming_copy.get("u"))

    for existing in base_entries or []:
        if not isinstance(existing, dict):
            continue
        if not _same_snapshot_entry(existing, incoming_copy):
            continue
        if not _entry_filename(incoming_copy) and _entry_filename(existing):
            incoming_copy["f"] = _entry_filename(existing)
        if not _entry_size(incoming_copy) and _entry_size(existing):
            incoming_copy["s"] = _entry_size(existing)
        break
    return incoming_copy
