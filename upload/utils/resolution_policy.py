"""
Enforce UploadSettings resolution flags on LLM extraction output.

Prompts already describe allowed tiers, but the model may still emit 2160p / sub-720p
from page text. This module strips disallowed keys before link resolution and pipeline.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_BASE_HEIGHTS = frozenset({480, 720, 1080})


def _height_from_quality_key(key: str) -> int | None:
    s = str(key).strip().lower()
    if s == "4k":
        return 2160
    m = re.fullmatch(r"(\d+)p", s)
    if not m:
        return None
    return int(m.group(1))


def _is_height_allowed(h: int, *, extra_below: bool, extra_above: bool) -> bool:
    if h in _BASE_HEIGHTS:
        return True
    if h > 1080:
        return extra_above
    if h < 480:
        return extra_below
    if 480 < h < 720:
        return extra_below
    # e.g. 900p — not in base trio; treat as disallowed unless we add a setting
    if 720 < h < 1080:
        return False
    return False


def _quality_allowed(key: str, *, extra_below: bool, extra_above: bool) -> bool:
    h = _height_from_quality_key(key)
    if h is None:
        return False
    return _is_height_allowed(h, extra_below=extra_below, extra_above=extra_above)


def _apply_max_extra_cap(
    keys: list[str],
    *,
    max_extra: int,
    extra_below: bool,
    extra_above: bool,
) -> list[str]:
    """
    Keep all base (480/720/1080) keys. Among allowed non-base keys, keep at most max_extra.
    When max_extra == 0, unlimited extras (only tier flags apply).
    """
    if max_extra <= 0:
        return keys

    base: list[str] = []
    extras: list[tuple[str, int]] = []
    for k in keys:
        h = _height_from_quality_key(k)
        if h is None:
            continue
        if h in _BASE_HEIGHTS:
            base.append(k)
        elif _is_height_allowed(h, extra_below=extra_below, extra_above=extra_above):
            extras.append((k, h))

    extras.sort(key=lambda t: t[1], reverse=True)
    kept_extra = [t[0] for t in extras[:max_extra]]
    return base + kept_extra


def filter_movie_data_for_upload_settings(
    data: dict[str, Any],
    *,
    extra_below: bool,
    extra_above: bool,
    max_extra: int,
) -> dict[str, Any]:
    """Remove movie download_links keys disallowed by settings."""
    if not isinstance(data, dict):
        return data

    dl = data.get("download_links")
    if not isinstance(dl, dict) or not dl:
        return data

    ordered = list(dl.keys())
    allowed_by_tier = [k for k in ordered if _quality_allowed(k, extra_below=extra_below, extra_above=extra_above)]
    capped = _apply_max_extra_cap(
        allowed_by_tier,
        max_extra=max_extra,
        extra_below=extra_below,
        extra_above=extra_above,
    )
    allowed_set = set(capped)

    removed = [k for k in ordered if k not in allowed_set]
    if removed:
        logger.info(
            "Resolution policy: removed disallowed movie qualities %s (extra_below=%s extra_above=%s max_extra=%s)",
            removed,
            extra_below,
            extra_above,
            max_extra,
        )

    new_dl = {k: v for k, v in dl.items() if k in allowed_set}
    out = dict(data)
    out["download_links"] = new_dl
    out.pop("download_filenames", None)
    return out


def filter_tvshow_data_for_upload_settings(
    data: dict[str, Any],
    *,
    extra_below: bool,
    extra_above: bool,
    max_extra: int,
) -> dict[str, Any]:
    """Remove per-item resolution keys disallowed by settings."""
    if not isinstance(data, dict):
        return data
    seasons = data.get("seasons")
    if not isinstance(seasons, list):
        return data

    new_seasons = []
    for season in seasons:
        if not isinstance(season, dict):
            new_seasons.append(season)
            continue
        items = season.get("download_items")
        if not isinstance(items, list):
            new_seasons.append(season)
            continue
        new_items = []
        for item in items:
            if not isinstance(item, dict):
                new_items.append(item)
                continue
            res = item.get("resolutions")
            if not isinstance(res, dict) or not res:
                new_items.append(item)
                continue
            ordered = list(res.keys())
            allowed_by_tier = [
                k
                for k in ordered
                if _quality_allowed(k, extra_below=extra_below, extra_above=extra_above)
            ]
            capped = _apply_max_extra_cap(
                allowed_by_tier,
                max_extra=max_extra,
                extra_below=extra_below,
                extra_above=extra_above,
            )
            allowed_set = set(capped)
            removed = [k for k in ordered if k not in allowed_set]
            if removed:
                sn = season.get("season_number", "?")
                lbl = item.get("label", "")
                logger.info(
                    "Resolution policy: removed S%s %s qualities %s (extra_below=%s extra_above=%s max_extra=%s)",
                    sn,
                    lbl,
                    removed,
                    extra_below,
                    extra_above,
                    max_extra,
                )
            it = dict(item)
            it["resolutions"] = {k: v for k, v in res.items() if k in allowed_set}
            it.pop("download_filenames", None)
            new_items.append(it)
        s2 = dict(season)
        s2["download_items"] = new_items
        new_seasons.append(s2)

    out = dict(data)
    out["seasons"] = new_seasons
    return out


def filter_duplicate_result_resolutions(
    dup_result: dict[str, Any] | None,
    *,
    extra_below: bool,
    extra_above: bool,
    max_extra: int,
) -> dict[str, Any] | None:
    """Filter missing_resolutions (and similar list fields) on duplicate_check output."""
    if not dup_result or not isinstance(dup_result, dict):
        return dup_result

    out = dict(dup_result)
    mr = out.get("missing_resolutions")
    if isinstance(mr, list) and mr:
        allowed_by_tier = [
            str(x).strip()
            for x in mr
            if x is not None
            and _quality_allowed(str(x).strip(), extra_below=extra_below, extra_above=extra_above)
        ]
        capped = _apply_max_extra_cap(
            allowed_by_tier,
            max_extra=max_extra,
            extra_below=extra_below,
            extra_above=extra_above,
        )
        removed = [str(x).strip() for x in mr if str(x).strip() not in set(capped)]
        if removed:
            logger.info(
                "Resolution policy: trimmed duplicate_check missing_resolutions removed=%s",
                removed,
            )
        out["missing_resolutions"] = capped
    return out


def apply_upload_resolution_policy(
    content_type: str,
    data: dict[str, Any],
    dup_result: dict[str, Any] | None,
    *,
    extra_below: bool,
    extra_above: bool,
    max_extra: int,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Apply all filters; returns (data, dup_result)."""
    ct = (content_type or "").strip().lower()
    if ct == "tvshow":
        data = filter_tvshow_data_for_upload_settings(
            data,
            extra_below=extra_below,
            extra_above=extra_above,
            max_extra=max_extra,
        )
    else:
        data = filter_movie_data_for_upload_settings(
            data,
            extra_below=extra_below,
            extra_above=extra_above,
            max_extra=max_extra,
        )
    dup_result = filter_duplicate_result_resolutions(
        dup_result,
        extra_below=extra_below,
        extra_above=extra_above,
        max_extra=max_extra,
    )
    return data, dup_result
