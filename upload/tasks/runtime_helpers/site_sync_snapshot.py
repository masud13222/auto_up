"""
Site sync snapshots: overlay live snapshot onto stored result, persist, hydrate from DB-only state.
"""

from __future__ import annotations

from django.db import transaction

from upload.models import MediaTask
from upload.utils.media_entry_helpers import (
    coerce_entry_language_value,
    is_drive_link,
    movie_download_entry_key,
    normalize_result_download_languages,
)
from upload.utils.tv_items import tv_item_key

from .entry_helpers import (
    _entry_link,
    _json_clone,
    _snapshot_entry_with_metadata,
)
from .published_view import _published_site_view


def extract_site_sync_snapshot_result(snapshot: dict | None) -> dict:
    if not isinstance(snapshot, dict):
        return {}
    data = snapshot.get("data")
    return _json_clone(data) if isinstance(data, dict) and data else {}


def overlay_site_sync_snapshot(existing_result: dict, snapshot_result: dict, content_type: str) -> dict:
    base = _json_clone(existing_result or {}) if isinstance(existing_result, dict) else {}
    if not isinstance(snapshot_result, dict) or not snapshot_result:
        return base
    snapshot_clean = _published_site_view(snapshot_result)

    if content_type == "movie":
        base_links = base.get("download_links") if isinstance(base.get("download_links"), dict) else {}
        live_links = snapshot_clean.get("download_links") if isinstance(snapshot_clean.get("download_links"), dict) else {}
        merged_links = {}
        for quality, entries in live_links.items():
            normalized_entries = []
            base_entries = base_links.get(quality, [])
            for entry in entries if isinstance(entries, list) else []:
                if not isinstance(entry, dict):
                    continue
                hydrated = _snapshot_entry_with_metadata(entry, base_entries)
                if is_drive_link(_entry_link(hydrated)):
                    normalized_entries.append(hydrated)
            if normalized_entries:
                merged_links[quality] = normalized_entries
        base["download_links"] = merged_links
        base.pop("download_filenames", None)
    else:
        base_seasons = {
            season.get("season_number"): _json_clone(season)
            for season in (base.get("seasons") or [])
            if isinstance(season, dict) and season.get("season_number") is not None
        }
        authoritative_seasons = []
        for season in snapshot_clean.get("seasons") or []:
            if not isinstance(season, dict):
                continue
            season_num = season.get("season_number")
            if season_num is None:
                continue
            target_season = {"season_number": season_num, "download_items": []}
            base_season = base_seasons.get(season_num) or {}
            existing_items = {
                tv_item_key(item): item
                for item in base_season.get("download_items", [])
                if isinstance(item, dict)
            }
            for incoming_item in season.get("download_items") or []:
                if not isinstance(incoming_item, dict):
                    continue
                key = tv_item_key(incoming_item)
                base_item = existing_items.get(key) or {}
                item_copy = _json_clone(incoming_item)
                if base_item.get("label"):
                    item_copy["label"] = base_item["label"]
                if not item_copy.get("episode_range") and base_item.get("episode_range"):
                    item_copy["episode_range"] = base_item.get("episode_range")
                merged_resolutions = {}
                for quality, entries in (incoming_item.get("resolutions") or {}).items():
                    normalized_entries = []
                    base_entries = (base_item.get("resolutions") or {}).get(quality, [])
                    for entry in entries if isinstance(entries, list) else []:
                        if not isinstance(entry, dict):
                            continue
                        hydrated = _snapshot_entry_with_metadata(entry, base_entries)
                        if is_drive_link(_entry_link(hydrated)):
                            normalized_entries.append(hydrated)
                    if normalized_entries:
                        merged_resolutions[quality] = normalized_entries
                if merged_resolutions:
                    item_copy["resolutions"] = merged_resolutions
                    item_copy.pop("download_filenames", None)
                    target_season["download_items"].append(item_copy)
            if target_season["download_items"]:
                authoritative_seasons.append(target_season)
        base["seasons"] = authoritative_seasons

    return normalize_result_download_languages(base)


def strip_movie_download_entries_by_flixbd_failures(movie_data: dict, failed: list[dict]) -> None:
    """
    Remove movie ``download_links`` entries that failed FlixBD POST so ``result`` /
    ``site_sync_snapshot`` only list rows we believe were accepted (after retries).
    No API fetch — matches ``failed`` records from ``add_movie_download_links``.

    When ``failed`` items include ``link_id`` (same as the fourth element of
    ``movie_download_entry_key``), only that specific row is stripped so duplicate
    basenames are safe.
    """
    if not failed or not isinstance(movie_data, dict):
        return
    dl = movie_data.get("download_links")
    if not isinstance(dl, dict):
        return
    fail_by_link: set[tuple[str, str, str, str]] = set()
    fail_triple_no_link_id: set[tuple[str, str, str]] = set()
    for f in failed:
        if not isinstance(f, dict):
            continue
        q = str(f.get("quality") or "").strip().lower()
        lang = coerce_entry_language_value(f.get("language"))
        fn = str(f.get("filename") or "").strip()
        lid = f.get("link_id")
        if isinstance(lid, str) and lid.strip():
            fail_by_link.add((q, lang, fn, lid.strip()))
        else:
            fail_triple_no_link_id.add((q, lang, fn))

    for res_key in list(dl.keys()):
        entries = dl.get(res_key)
        if not isinstance(entries, list):
            continue
        rk = str(res_key or "").strip().lower()
        kept = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            lang = coerce_entry_language_value(entry.get("l"))
            fn = str(entry.get("f") or "").strip()
            eid = movie_download_entry_key(rk, entry)
            if eid in fail_by_link:
                continue
            if (rk, lang, fn) in fail_triple_no_link_id:
                continue
            kept.append(entry)
        if kept:
            dl[res_key] = kept
        else:
            del dl[res_key]


def strip_tvshow_download_entries_by_flixbd_failures(tvshow_data: dict, failed: list[dict]) -> None:
    """
    Remove TV resolution entries that failed FlixBD POST (same matching as movie helper).
    """
    if not failed or not isinstance(tvshow_data, dict):
        return
    fail_set = set()
    for f in failed:
        if not isinstance(f, dict):
            continue
        sn = f.get("season_number")
        try:
            sn = int(sn) if sn is not None else None
        except (TypeError, ValueError):
            sn = None
        label = str(f.get("label") or "").strip()
        q = str(f.get("quality") or "").strip().lower()
        lang = coerce_entry_language_value(f.get("language"))
        fn = str(f.get("filename") or "").strip()
        fail_set.add((sn, label, q, lang, fn))

    for season in tvshow_data.get("seasons") or []:
        if not isinstance(season, dict):
            continue
        snum = season.get("season_number")
        try:
            snum = int(snum) if snum is not None else None
        except (TypeError, ValueError):
            snum = None
        for item in season.get("download_items") or []:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "").strip()
            res = item.get("resolutions")
            if not isinstance(res, dict):
                continue
            for qual_key in list(res.keys()):
                qk = str(qual_key or "").strip().lower()
                entries = res.get(qual_key)
                if not isinstance(entries, list):
                    continue
                kept = []
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    lang = coerce_entry_language_value(entry.get("l"))
                    fn = str(entry.get("f") or "").strip()
                    if (snum, label, qk, lang, fn) in fail_set:
                        continue
                    kept.append(entry)
                if kept:
                    res[qual_key] = kept
                else:
                    del res[qual_key]


def build_site_sync_snapshot(
    content_type: str,
    data: dict,
    *,
    website_title: str = "",
    site_content_id: int | None = None,
) -> dict:
    result = _published_site_view(data or {})
    payload = {
        "version": 1,
        "content_type": content_type,
        "website_title": str(website_title or "").strip(),
        "data": {},
    }
    if site_content_id is not None:
        payload["site_content_id"] = int(site_content_id)
    if result.get("title"):
        payload["title"] = result.get("title")
    if result.get("year") is not None:
        payload["year"] = result.get("year")
    if content_type == "movie":
        payload["data"]["download_links"] = result.get("download_links") or {}
    else:
        payload["data"]["seasons"] = result.get("seasons") or []
    return payload


def save_site_sync_snapshot(
    media_task: MediaTask,
    content_type: str,
    data: dict,
    *,
    website_title: str = "",
    site_content_id: int | None = None,
) -> dict:
    snapshot = build_site_sync_snapshot(
        content_type,
        data,
        website_title=website_title,
        site_content_id=site_content_id,
    )
    media_task.site_sync_snapshot = snapshot
    media_task.save(update_fields=["site_sync_snapshot", "updated_at"])
    return snapshot


def save_publish_state_with_snapshot(
    media_task: MediaTask,
    content_type: str,
    data: dict,
    *,
    website_title: str = "",
    site_content_id: int | None = None,
    update_site_sync_snapshot: bool = True,
) -> dict | None:
    """
    Atomically persist the post-publish local state so `result`, `website_title`, and
    `site_content_id` stay aligned. When ``update_site_sync_snapshot`` is False, the
    previous ``site_sync_snapshot`` row is left unchanged.
    """
    snapshot = None
    if update_site_sync_snapshot:
        snapshot = build_site_sync_snapshot(
            content_type,
            data,
            website_title=website_title,
            site_content_id=site_content_id,
        )
    result_copy = _json_clone(data)
    update_fields = ["result", "updated_at"]

    with transaction.atomic():
        media_task.result = result_copy
        if update_site_sync_snapshot and snapshot is not None:
            media_task.site_sync_snapshot = snapshot
            update_fields.append("site_sync_snapshot")
        media_task.website_title = str(website_title or "").strip()
        update_fields.append("website_title")
        if site_content_id is not None:
            media_task.site_content_id = int(site_content_id)
            update_fields.append("site_content_id")
        media_task.save(update_fields=update_fields)

    return snapshot


def hydrate_existing_result_from_snapshot(media_task: MediaTask, content_type: str) -> dict:
    """
    Build the current published view using only local MediaTask state.

    No live target-site/API fetch is performed here. The source of truth is the
    task's stored ``result`` plus ``site_sync_snapshot``.
    """
    site_content_id = getattr(media_task, "site_content_id", None)
    snapshot_data = extract_site_sync_snapshot_result(getattr(media_task, "site_sync_snapshot", None))
    base_result = _json_clone(media_task.result) if isinstance(media_task.result, dict) else {}
    merged = overlay_site_sync_snapshot(base_result, snapshot_data, content_type)
    hydrated = _published_site_view(merged or snapshot_data or base_result)
    snapshot = build_site_sync_snapshot(
        content_type,
        hydrated,
        website_title=getattr(media_task, "website_title", ""),
        site_content_id=site_content_id,
    )
    media_task.site_sync_snapshot = snapshot
    media_task.save(update_fields=["site_sync_snapshot", "updated_at"])
    return hydrated
