"""
Strip or retain only Drive-backed download rows in result-shaped dicts.
"""

from __future__ import annotations

from upload.utils.media_entry_helpers import is_drive_link

from .entry_helpers import _entry_link


def result_strip_non_drive_download_links(data: dict) -> dict:
    """For skip-without-upload rows: do not persist generate.php / host links as if final."""
    if not data:
        return data
    out = dict(data)
    dl = out.get("download_links")
    if isinstance(dl, dict):
        out["download_links"] = {
            k: [entry for entry in (entries if isinstance(entries, list) else []) if is_drive_link(_entry_link(entry))]
            for k, entries in dl.items()
        }
        out["download_links"] = {k: v for k, v in out["download_links"].items() if v}

    seasons = out.get("seasons")
    if isinstance(seasons, list):
        kept_seasons = []
        for season in seasons:
            if not isinstance(season, dict):
                continue
            kept_items = []
            for item in season.get("download_items", []) if isinstance(season.get("download_items"), list) else []:
                if not isinstance(item, dict):
                    continue
                kept_resolutions = {}
                for quality, entries in (item.get("resolutions") or {}).items():
                    if not isinstance(entries, list):
                        continue
                    kept_entries = [entry for entry in entries if isinstance(entry, dict) and is_drive_link(_entry_link(entry))]
                    if kept_entries:
                        kept_resolutions[quality] = kept_entries
                if kept_resolutions:
                    item_copy = dict(item)
                    item_copy["resolutions"] = kept_resolutions
                    item_copy.pop("download_filenames", None)
                    kept_items.append(item_copy)
            if kept_items:
                season_copy = dict(season)
                season_copy["download_items"] = kept_items
                kept_seasons.append(season_copy)
        out["seasons"] = kept_seasons

    out.pop("download_filenames", None)
    return out


def clean_result_keep_drive_links(result: dict) -> dict:
    """Strip resolutions without Drive links from a failed task result."""
    if not result:
        return result

    cleaned = dict(result)

    if "download_links" in cleaned:
        cleaned["download_links"] = {
            k: [entry for entry in (entries if isinstance(entries, list) else []) if is_drive_link(_entry_link(entry))]
            for k, entries in cleaned["download_links"].items()
        }
        cleaned["download_links"] = {k: v for k, v in cleaned["download_links"].items() if v}
        cleaned.pop("download_filenames", None)

    for season in cleaned.get("seasons", []):
        items_to_keep = []
        for item in season.get("download_items", []):
            res = item.get("resolutions", {})
            cleaned_res = {
                k: [entry for entry in (entries if isinstance(entries, list) else []) if is_drive_link(_entry_link(entry))]
                for k, entries in res.items()
            }
            cleaned_res = {k: v for k, v in cleaned_res.items() if v}
            if cleaned_res:
                item["resolutions"] = cleaned_res
                item.pop("download_filenames", None)
                items_to_keep.append(item)
        season["download_items"] = items_to_keep

    return cleaned


def has_drive_links(result: dict) -> bool:
    """Check if a result dict actually contains any Google Drive upload links."""
    if not result:
        return False
    for entries in result.get("download_links", {}).values():
        for entry in entries if isinstance(entries, list) else []:
            if is_drive_link(_entry_link(entry)):
                return True
    for season in result.get("seasons", []):
        for item in season.get("download_items", []):
            for entries in item.get("resolutions", {}).values():
                for entry in entries if isinstance(entries, list) else []:
                    if is_drive_link(_entry_link(entry)):
                        return True
    return False
