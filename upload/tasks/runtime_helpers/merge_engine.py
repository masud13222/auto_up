"""
Merge new TV episodes and carry over Drive links from older results.
"""

from __future__ import annotations

import logging

from upload.utils.media_entry_helpers import is_drive_link
from upload.utils.tv_items import tv_item_key

from .entry_helpers import (
    _entry_copy,
    _entry_filename,
    _entry_language_key,
    _entry_link,
    _json_clone,
    _tv_download_item_has_any_drive_link,
)

logger = logging.getLogger(__name__)


def merge_new_episodes(existing_result: dict, new_data: dict) -> dict:
    """
    Merge new TV show episodes from new_data INTO existing_result.
    """
    existing_seasons = existing_result.get("seasons", [])
    new_seasons = new_data.get("seasons", [])

    if not existing_seasons:
        return new_data

    if not new_seasons:
        logger.warning(
            "Episode merge: new_data has no seasons, preserving existing result to avoid data loss"
        )
        result = dict(existing_result)
        result.update({k: v for k, v in new_data.items() if k not in ("seasons",)})
        result["seasons"] = existing_seasons
        return result

    merged_seasons = {s["season_number"]: dict(s) for s in existing_seasons}
    for season in merged_seasons.values():
        season["download_items"] = list(season.get("download_items", []))

    for new_season in new_seasons:
        snum = new_season.get("season_number")
        new_items = new_season.get("download_items", [])

        if snum not in merged_seasons:
            merged_seasons[snum] = dict(new_season)
            logger.info("Episode merge: added new season %s", snum)
            continue

        existing_items_by_key = {}
        for item in merged_seasons[snum]["download_items"]:
            existing_items_by_key[tv_item_key(item)] = item
        existing_keys = set(existing_items_by_key.keys())

        added = []
        merged_res_labels = []
        for new_item in new_items:
            key = tv_item_key(new_item)
            if key not in existing_keys:
                merged_seasons[snum]["download_items"].append(new_item)
                existing_keys.add(key)
                added.append(new_item.get("label", ""))
            else:
                ex_item = existing_items_by_key[key]
                ex_res = ex_item.get("resolutions") or {}
                new_res = new_item.get("resolutions") or {}
                changed = False
                for quality, new_entries in new_res.items():
                    if quality not in ex_res:
                        ex_res[quality] = list(new_entries)
                        changed = True
                    else:
                        has_drive = any(
                            is_drive_link(_entry_link(e)) for e in ex_res[quality]
                        )
                        if not has_drive:
                            ex_res[quality] = list(new_entries)
                            changed = True
                if changed:
                    ex_item["resolutions"] = ex_res
                    merged_res_labels.append(new_item.get("label", ""))

        if added:
            logger.info(
                "Episode merge: appended %s new episode(s) to S%s: %s",
                len(added),
                snum,
                added,
            )
        if merged_res_labels:
            logger.info(
                "Episode merge: merged new resolutions into %s existing S%s item(s): %s",
                len(merged_res_labels),
                snum,
                merged_res_labels,
            )
        if not added and not merged_res_labels:
            logger.info("Episode merge: no new episodes or resolutions to add for S%s", snum)

    result = dict(existing_result)
    for key, value in new_data.items():
        if key == "seasons":
            continue
        current = result.get(key)
        if current in (None, "", [], {}):
            if value not in (None, "", [], {}):
                result[key] = value
        elif key == "total_seasons":
            try:
                result[key] = max(int(current), int(value))
            except (TypeError, ValueError):
                pass
    result["seasons"] = sorted(merged_seasons.values(), key=lambda s: s["season_number"])

    old_ss = existing_result.get("screen_shots_url")
    if isinstance(old_ss, list) and old_ss:
        cur = result.get("screen_shots_url")
        if not isinstance(cur, list) or not cur:
            result["screen_shots_url"] = list(old_ss)

    return result


def merge_drive_links(old_result: dict, new_data: dict) -> dict:
    """
    Merge existing Drive links from old_result into new_data.
    """
    old_dl = old_result.get("download_links", {})
    new_dl = new_data.get("download_links", {})
    if old_dl and new_dl:
        for res, old_entries in old_dl.items():
            if res not in new_dl:
                carried = [
                    _json_clone(entry)
                    for entry in (old_entries if isinstance(old_entries, list) else [])
                    if is_drive_link(_entry_link(entry))
                ]
                if carried:
                    new_dl[res] = carried
                continue
            existing_by_file = {
                (_entry_language_key(entry), _entry_filename(entry)): entry
                for entry in (old_entries if isinstance(old_entries, list) else [])
                if is_drive_link(_entry_link(entry))
            }
            merged_entries = []
            for cur in new_dl.get(res) or []:
                old_entry = existing_by_file.get((_entry_language_key(cur), _entry_filename(cur)))
                if old_entry:
                    merged_entries.append(_entry_copy(cur, link=_entry_link(old_entry)))
                    logger.debug("Preserved existing drive link for %s [%s] %s", res, cur.get("l"), _entry_filename(cur))
                else:
                    merged_entries.append(cur)
            new_dl[res] = merged_entries
        new_data["download_links"] = new_dl

    old_seasons = {s.get("season_number"): s for s in old_result.get("seasons", [])}
    new_seasons = {s.get("season_number"): s for s in new_data.get("seasons", [])}

    for snum, old_season in old_seasons.items():
        if snum not in new_seasons:
            carried_items = []
            for item in old_season.get("download_items", []):
                if not isinstance(item, dict):
                    continue
                if _tv_download_item_has_any_drive_link(item):
                    carried_items.append(_json_clone(item))
            if carried_items:
                new_data.setdefault("seasons", []).append(
                    {"season_number": snum, "download_items": carried_items}
                )

    for new_season in new_data.get("seasons", []):
        snum = new_season.get("season_number")
        old_season = old_seasons.get(snum)
        if not old_season:
            continue

        old_items = {}
        old_items_full = {}
        for item in old_season.get("download_items", []):
            key = tv_item_key(item)
            old_items[key] = item.get("resolutions", {})
            old_items_full[key] = item

        new_item_keys = {
            tv_item_key(item)
            for item in new_season.get("download_items", [])
            if isinstance(item, dict)
        }
        for old_key, old_item in old_items_full.items():
            if old_key in new_item_keys:
                continue
            if _tv_download_item_has_any_drive_link(old_item):
                new_season.setdefault("download_items", []).append(_json_clone(old_item))

        for new_item in new_season.get("download_items", []):
            label = new_item.get("label", "")
            key = tv_item_key(new_item)
            old_res = old_items.get(key, {})
            new_res = new_item.get("resolutions", {})

            for res, old_entries in old_res.items():
                if res not in new_res:
                    carried = [
                        _json_clone(entry)
                        for entry in (old_entries if isinstance(old_entries, list) else [])
                        if is_drive_link(_entry_link(entry))
                    ]
                    if carried:
                        new_res[res] = carried
                    continue
                existing_by_file = {
                    (_entry_language_key(entry), _entry_filename(entry)): entry
                    for entry in (old_entries if isinstance(old_entries, list) else [])
                    if is_drive_link(_entry_link(entry))
                }
                merged_entries = []
                for cur in new_res.get(res) or []:
                    old_entry = existing_by_file.get((_entry_language_key(cur), _entry_filename(cur)))
                    if old_entry:
                        merged_entries.append(_entry_copy(cur, link=_entry_link(old_entry)))
                        logger.debug(
                            "Preserved existing drive link for S%s %s %s [%s] %s",
                            snum,
                            label,
                            res,
                            cur.get("l"),
                            _entry_filename(cur),
                        )
                    else:
                        merged_entries.append(cur)
                new_res[res] = merged_entries

            new_item["resolutions"] = new_res
            new_item.pop("download_filenames", None)

    old_ss = old_result.get("screen_shots_url")
    if isinstance(old_ss, list) and old_ss:
        cur = new_data.get("screen_shots_url")
        if not isinstance(cur, list) or not cur:
            new_data["screen_shots_url"] = list(old_ss)

    if isinstance(new_data.get("seasons"), list):
        new_data["seasons"].sort(key=lambda s: s.get("season_number") or 0)

    return new_data
