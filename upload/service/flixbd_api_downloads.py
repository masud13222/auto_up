import logging
import re
import time

import httpx

from upload.tasks.helpers import coerce_entry_language_value
from upload.utils.tv_items import tv_items_overlap

from .flixbd_api_base import _TIMEOUT, _get_config, _headers, _safe_json, _url

logger = logging.getLogger(__name__)

# Transient errors: retry POST before recording a hard failure.
_MAX_FLIXBD_DOWNLOAD_POST_ATTEMPTS = 4
_RETRYABLE_FLIXBD_HTTP = frozenset({408, 429, 500, 502, 503, 504})


def _sleep_flixbd_retry(attempt: int) -> None:
    time.sleep(min(2.0**attempt, 30.0))


def _post_flixbd_download_json(
    endpoint: str,
    api_key: str,
    payload: dict,
    *,
    log_label: str,
) -> tuple[httpx.Response | None, BaseException | None]:
    """
    POST JSON to FlixBD download endpoint. Retries on network errors and retryable HTTP codes.
    Returns ``(response, None)`` or ``(None, exception)`` if every attempt failed on the network.
    """
    last_net: BaseException | None = None
    for attempt in range(_MAX_FLIXBD_DOWNLOAD_POST_ATTEMPTS):
        try:
            with httpx.Client(timeout=_TIMEOUT) as client:
                resp = client.post(endpoint, json=payload, headers=_headers(api_key))
        except RuntimeError:
            raise
        except (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ReadError,
            httpx.WriteError,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
        ) as e:
            last_net = e
            if attempt + 1 >= _MAX_FLIXBD_DOWNLOAD_POST_ATTEMPTS:
                logger.error(
                    "FlixBD POST %s: network error after %s attempts: %s",
                    log_label,
                    _MAX_FLIXBD_DOWNLOAD_POST_ATTEMPTS,
                    e,
                )
                return None, e
            logger.warning(
                "FlixBD POST %s: network error attempt %s/%s: %s",
                log_label,
                attempt + 1,
                _MAX_FLIXBD_DOWNLOAD_POST_ATTEMPTS,
                e,
            )
            _sleep_flixbd_retry(attempt)
            continue

        sc = resp.status_code
        if sc in _RETRYABLE_FLIXBD_HTTP and attempt + 1 < _MAX_FLIXBD_DOWNLOAD_POST_ATTEMPTS:
            logger.warning(
                "FlixBD POST %s: HTTP %s attempt %s/%s — retrying",
                log_label,
                sc,
                attempt + 1,
                _MAX_FLIXBD_DOWNLOAD_POST_ATTEMPTS,
            )
            _sleep_flixbd_retry(attempt)
            continue
        return resp, None

    return None, last_net


def _normalized_resolution_key(value) -> str:
    return str(value or "").strip().lower()


def add_movie_download_links(
    content_id: int,
    drive_links: dict,
    file_sizes: dict,
    movie_data: dict,
    server_name: str = "GDrive",
    allowed_entry_ids: set[tuple[str, str, str]] | None = None,
) -> dict:
    """
    Add download links for a movie.

    Returns a summary dict: ``created_ids``, ``attempted``, ``succeeded``, ``failed``
    (each failure: quality, language, filename, reason).
    """
    api_url, api_key = _get_config()
    endpoint = _url(api_url, "api/v1/movies", content_id, "downloads")
    created_ids: list = []
    attempted = 0
    succeeded = 0
    failed: list[dict] = []

    for quality, entries in drive_links.items():
        for drive_item in entries if isinstance(entries, list) else []:
            drive_url = str(drive_item.get("u") or "").strip()
            if not drive_url:
                continue
            entry_language = coerce_entry_language_value(drive_item.get("l"))
            entry_id = (
                _normalized_resolution_key(quality),
                entry_language,
                str(drive_item.get("f") or "").strip(),
            )
            if allowed_entry_ids is not None and entry_id not in allowed_entry_ids:
                continue
            attempted += 1
            payload = {
                "server_name": server_name,
                "download_link": drive_url,
                "quality": _normalized_resolution_key(quality),
            }
            if entry_language:
                payload["language"] = entry_language
            size = (
                file_sizes.get(entry_id)
                or str(drive_item.get("s") or "").strip()
            )
            if size:
                payload["size"] = size

            log_label = f"movie id={content_id} {quality} [{entry_language}]"
            resp, net_err = _post_flixbd_download_json(
                endpoint, api_key, payload, log_label=log_label
            )
            if net_err is not None:
                failed.append(
                    {
                        "quality": str(quality),
                        "language": entry_language,
                        "filename": str(drive_item.get("f") or "").strip(),
                        "reason": "network",
                        "detail": str(net_err)[:500],
                    }
                )
                continue
            assert resp is not None

            if resp.status_code == 409:
                body = _safe_json(resp, f"movie {content_id} downloads 409")
                existing_id = body.get("errors", {}).get("existing_download_id")
                logger.info(
                    "FlixBD: duplicate link for movie %s %s [%s] (existing id=%s)",
                    content_id,
                    quality,
                    entry_language,
                    existing_id,
                )
                if existing_id:
                    created_ids.append(existing_id)
                succeeded += 1
                continue

            if resp.status_code in (400, 422):
                logger.warning(
                    "FlixBD: add download link error for movie %s %s [%s]: %s",
                    content_id,
                    quality,
                    entry_language,
                    resp.text,
                )
                failed.append(
                    {
                        "quality": str(quality),
                        "language": entry_language,
                        "filename": str(drive_item.get("f") or "").strip(),
                        "reason": f"HTTP {resp.status_code}",
                        "detail": (resp.text or "")[:500],
                    }
                )
                continue

            if resp.status_code in _RETRYABLE_FLIXBD_HTTP:
                logger.warning(
                    "FlixBD: add download link HTTP %s for movie %s %s [%s] after retries: %s",
                    resp.status_code,
                    content_id,
                    quality,
                    entry_language,
                    (resp.text or "")[:300],
                )
                failed.append(
                    {
                        "quality": str(quality),
                        "language": entry_language,
                        "filename": str(drive_item.get("f") or "").strip(),
                        "reason": f"HTTP {resp.status_code}",
                        "detail": (resp.text or "")[:500],
                    }
                )
                continue

            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                failed.append(
                    {
                        "quality": str(quality),
                        "language": entry_language,
                        "filename": str(drive_item.get("f") or "").strip(),
                        "reason": f"HTTP {e.response.status_code}",
                        "detail": (e.response.text or "")[:500],
                    }
                )
                continue

            try:
                dl_id = _safe_json(resp, f"movie {content_id} add_download {quality}")["data"]["id"]
            except (KeyError, TypeError, ValueError) as e:
                failed.append(
                    {
                        "quality": str(quality),
                        "language": entry_language,
                        "filename": str(drive_item.get("f") or "").strip(),
                        "reason": "bad_response",
                        "detail": str(e)[:500],
                    }
                )
                continue
            created_ids.append(dl_id)
            succeeded += 1
            logger.info(
                "FlixBD: added download link id=%s movie=%s quality=%s language=%s",
                dl_id,
                content_id,
                quality,
                entry_language,
            )

    return {
        "created_ids": created_ids,
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
    }


def list_movie_downloads(content_id: int) -> list[dict]:
    """GET movie downloads list (best-effort)."""
    api_url, api_key = _get_config()
    endpoint = _url(api_url, "api/v1/movies", content_id, "downloads")
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.get(endpoint, headers=_headers(api_key))
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        body = _safe_json(resp, f"movie {content_id} list_downloads")
        data = body.get("data", body.get("downloads"))
        if data is None:
            return []
        if isinstance(data, dict):
            inner = data.get("data") or data.get("items")
            data = inner if isinstance(inner, list) else []
        if not isinstance(data, list):
            return []
        return [row for row in data if isinstance(row, dict)]
    except Exception as e:
        logger.warning("FlixBD list_movie_downloads id=%s: %s", content_id, e)
        return []


def delete_movie_download(content_id: int, download_row_id: int) -> bool:
    """DELETE one movie download row. False on failure."""
    api_url, api_key = _get_config()
    endpoint = _url(api_url, "api/v1/downloads", download_row_id)
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.delete(endpoint, headers=_headers(api_key))
        if resp.status_code in (404, 405):
            logger.warning(
                "FlixBD delete_movie_download movie=%s dl=%s HTTP %s",
                content_id,
                download_row_id,
                resp.status_code,
            )
            return resp.status_code == 404
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.warning(
            "FlixBD delete_movie_download movie=%s dl=%s: %s",
            content_id,
            download_row_id,
            e,
        )
        return False


def clear_movie_download_links(content_id: int) -> int:
    """Remove all download rows for a movie."""
    rows = list_movie_downloads(content_id)
    deleted = 0
    for row in rows:
        rid = row.get("id")
        if rid is None:
            continue
        try:
            rid = int(rid)
        except (TypeError, ValueError):
            continue
        if delete_movie_download(content_id, rid):
            deleted += 1
    if deleted:
        logger.info("FlixBD: cleared %s download row(s) for movie id=%s", deleted, content_id)
    return deleted


def fetch_movie_drive_links_by_quality(content_id: int) -> dict[str, list[dict]]:
    """Build ``{quality: [{u, l, s?}]}`` from movie download rows."""
    out: dict[str, list[dict]] = {}
    for row in list_movie_downloads(content_id):
        link = row.get("download_link") or row.get("url") or row.get("link")
        if not link or "drive.google.com" not in str(link):
            continue
        quality = row.get("quality")
        if not quality:
            continue
        language = str(row.get("language") or "").strip()
        quality_key = _normalized_resolution_key(quality)
        if not quality_key:
            continue
        entry = {"u": str(link).strip(), "f": ""}
        if language:
            entry["l"] = language
        size = str(row.get("size") or "").strip()
        if size:
            entry["s"] = size
        out.setdefault(quality_key, []).append(entry)
    return out


def list_series_downloads(content_id: int) -> list[dict]:
    """GET series downloads list (best-effort)."""
    api_url, api_key = _get_config()
    endpoint = _url(api_url, "api/v1/series", content_id, "downloads")
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.get(endpoint, headers=_headers(api_key))
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        body = _safe_json(resp, f"series {content_id} list_downloads")
        data = body.get("data", body.get("downloads"))
        if data is None:
            return []
        if isinstance(data, dict):
            inner = data.get("data") or data.get("items")
            data = inner if isinstance(inner, list) else []
        if not isinstance(data, list):
            return []
        return [row for row in data if isinstance(row, dict)]
    except Exception as e:
        logger.warning("FlixBD list_series_downloads id=%s: %s", content_id, e)
        return []


def delete_series_download(content_id: int, download_row_id: int) -> bool:
    """DELETE one series download row. False on failure."""
    api_url, api_key = _get_config()
    endpoint = _url(api_url, "api/v1/downloads", download_row_id)
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.delete(endpoint, headers=_headers(api_key))
        if resp.status_code in (404, 405):
            return resp.status_code == 404
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.warning(
            "FlixBD delete_series_download series=%s dl=%s: %s",
            content_id,
            download_row_id,
            e,
        )
        return False


def clear_series_download_links(content_id: int) -> int:
    """Remove all series download rows."""
    rows = list_series_downloads(content_id)
    deleted = 0
    for row in rows:
        rid = row.get("id")
        if rid is None:
            continue
        try:
            rid = int(rid)
        except (TypeError, ValueError):
            continue
        if delete_series_download(content_id, rid):
            deleted += 1
    if deleted:
        logger.info("FlixBD: cleared %s download row(s) for series id=%s", deleted, content_id)
    return deleted


def _coerce_season_number(raw) -> int | None:
    """Normalize season number from API row."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw >= 0 else None
    text = str(raw).strip()
    if text.isdigit():
        return int(text)
    return None


def fetch_series_drive_links_tree(content_id: int) -> list[dict]:
    """
    Hydrate TV seasons/download_items tree from existing FlixBD series download rows.
    Uses explicit season_number + episode_number from the API response.
    """
    grouped: dict[int, dict[tuple[str, str | None], dict]] = {}
    for row in list_series_downloads(content_id):
        link = row.get("download_link") or row.get("url") or row.get("link")
        if not link or "drive.google.com" not in str(link):
            continue
        season_num = _coerce_season_number(row.get("season_number") or row.get("season"))
        if season_num is None:
            continue
        quality = str(row.get("quality") or "").strip()
        if not quality:
            continue
        language = str(row.get("language") or "").strip()
        quality_key = _normalized_resolution_key(quality)
        episode_range = _parse_episode_range_field(row.get("episode_number") or row.get("episode"))
        item_type = "combo_pack" if episode_range is None else (
            "partial_combo" if "-" in episode_range else "single_episode"
        )
        key = (item_type, episode_range)
        season_map = grouped.setdefault(season_num, {})
        item = season_map.setdefault(
            key,
            {
                "type": item_type,
                "label": "Complete Season" if item_type == "combo_pack" else f"Episode {episode_range}",
                "resolutions": {},
            },
        )
        if episode_range is not None:
            item["episode_range"] = episode_range
        entry = {"u": str(link).strip(), "f": ""}
        if language:
            entry["l"] = language
        size = row.get("size")
        if size:
            entry["s"] = str(size).strip()
        item["resolutions"].setdefault(quality_key, []).append(entry)

    out: list[dict] = []
    for season_num in sorted(grouped.keys()):
        items = list(grouped[season_num].values())
        items.sort(
            key=lambda item: (
                0 if item.get("type") == "combo_pack" else 1,
                item.get("episode_range") or "",
                item.get("label") or "",
            )
        )
        out.append({"season_number": season_num, "download_items": items})
    return out


def clear_series_download_links_for_scope(content_id: int, seasons_data: list) -> int:
    """
    Delete only overlapping existing series download rows for incoming TV items.
    Used by TV ``replace_items`` so the whole series is not wiped.
    """
    incoming_by_season = {
        season.get("season_number"): list(season.get("download_items", []))
        for season in seasons_data or []
    }
    rows = list_series_downloads(content_id)
    deleted = 0

    for row in rows:
        rid = row.get("id")
        if rid is None:
            continue
        try:
            rid = int(rid)
        except (TypeError, ValueError):
            continue

        season_num = _coerce_season_number(row.get("season_number") or row.get("season"))
        if season_num is None:
            continue
        incoming_items = incoming_by_season.get(season_num, [])
        if not incoming_items:
            continue
        if any(item.get("type") == "combo_pack" for item in incoming_items):
            logger.warning(
                "FlixBD: replace_items scope includes combo_pack for series id=%s season=%s; "
                "caller should have escalated to full replace",
                content_id,
                season_num,
            )
            continue

        episode_range = _parse_episode_range_field(row.get("episode_number") or row.get("episode"))
        row_item = {
            "type": "combo_pack" if episode_range is None else (
                "partial_combo" if "-" in episode_range else "single_episode"
            ),
            "episode_range": episode_range,
        }
        if row_item["type"] == "combo_pack":
            logger.warning(
                "FlixBD: replace_items hit existing combo_pack row for series id=%s season=%s; "
                "caller should have escalated to full replace",
                content_id,
                season_num,
            )
            continue

        if any(tv_items_overlap(row_item, item) for item in incoming_items):
            if delete_series_download(content_id, rid):
                deleted += 1

    if deleted:
        logger.info(
            "FlixBD: cleared %s overlapping series download row(s) for id=%s",
            deleted,
            content_id,
        )
    return deleted


def _parse_episode_range_field(raw) -> str | None:
    """
    Normalize ``download_item["episode_range"]`` for FlixBD (``01``, ``01-08``).
    Accepts int/float (whole) or str. Empty / unparseable → None.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return str(raw).zfill(2) if raw >= 0 else None
    if isinstance(raw, float) and raw.is_integer():
        value = int(raw)
        return str(value).zfill(2) if value >= 0 else None

    text = str(raw).strip()
    if not text:
        return None
    if "-" in text or "–" in text:
        parts = re.split(r"[-–]", text, maxsplit=1)
        if len(parts) == 2:
            a, b = parts[0].strip(), parts[1].strip()
            if a.isdigit() and b.isdigit():
                return f"{int(a):02d}-{int(b):02d}"
        return None
    if text.isdigit():
        return str(int(text)).zfill(2)
    return None


def _episode_number_for_flixbd_item(item: dict) -> str | None:
    """
    Value for FlixBD ``episode_number`` (``05``, ``01-08``) or ``None`` (combo pack).
    """
    item_type = item.get("type", "")
    if item_type == "combo_pack":
        return None

    label = (item.get("label") or "").strip()
    value = _parse_episode_range_field(item.get("episode_range"))
    if value:
        return value

    if item_type == "single_episode":
        match = re.search(r"(?i)(?:episode|ep\.?)\s*(\d+)", label)
        if match:
            return str(int(match.group(1))).zfill(2)
        return None

    if item_type == "partial_combo":
        match = re.search(r"(?i)(?:episode|ep\.?)\s*(\d+)\s*[-–]\s*(\d+)", label)
        if match:
            return f"{int(match.group(1)):02d}-{int(match.group(2)):02d}"
        match = re.search(r"(?i)(?:episode|ep\.?)\s*(\d+)", label)
        if match:
            return str(int(match.group(1))).zfill(2)
        return None

    return None


def add_series_download_links(
    content_id: int,
    seasons_data: list,
    file_sizes_map: dict,
    tvshow_data: dict,
    server_name: str = "GDrive",
    allowed_entry_ids: set[tuple[int, str, str, str, str]] | None = None,
) -> dict:
    """
    Add download links for a series.

    Returns ``created_ids``, ``attempted``, ``succeeded``, ``failed`` (failure records
    include season_number, label, quality, language, filename, reason).
    """
    api_url, api_key = _get_config()
    endpoint = _url(api_url, "api/v1/series", content_id, "downloads")
    created_ids: list = []
    attempted = 0
    succeeded = 0
    failed: list[dict] = []

    for season in seasons_data:
        season_num = season.get("season_number")
        for item in season.get("download_items", []):
            item_label = item.get("label", "")
            resolutions = item.get("resolutions", {})

            for quality, entries in resolutions.items():
                for drive_item in entries if isinstance(entries, list) else []:
                    drive_url = str(drive_item.get("u") or "").strip()
                    if not drive_url or not drive_url.startswith("https://drive.google.com"):
                        continue
                    entry_language = coerce_entry_language_value(drive_item.get("l"))
                    entry_id = (
                        season_num,
                        item_label,
                        _normalized_resolution_key(quality),
                        entry_language,
                        str(drive_item.get("f") or "").strip(),
                    )
                    if allowed_entry_ids is not None and entry_id not in allowed_entry_ids:
                        continue

                    attempted += 1
                    payload = {
                        "server_name": server_name,
                        "download_link": drive_url,
                        "quality": _normalized_resolution_key(quality),
                        "season_number": str(season_num).zfill(2),
                    }

                    ep_val = _episode_number_for_flixbd_item(item)
                    if ep_val is not None:
                        payload["episode_number"] = ep_val

                    if entry_language:
                        payload["language"] = entry_language

                    size = (
                        file_sizes_map.get(entry_id)
                        or str(drive_item.get("s") or "").strip()
                    )
                    if size:
                        payload["size"] = size

                    log_label = f"series id={content_id} S{season_num} {item_label!r} {quality}"
                    resp, net_err = _post_flixbd_download_json(
                        endpoint, api_key, payload, log_label=log_label
                    )
                    if net_err is not None:
                        failed.append(
                            {
                                "season_number": season_num,
                                "label": item_label,
                                "quality": str(quality),
                                "language": entry_language,
                                "filename": str(drive_item.get("f") or "").strip(),
                                "reason": "network",
                                "detail": str(net_err)[:500],
                            }
                        )
                        continue
                    assert resp is not None

                    if resp.status_code == 409:
                        body = _safe_json(resp, f"series {content_id} S{season_num} downloads 409")
                        existing_id = body.get("errors", {}).get("existing_download_id")
                        logger.info(
                            "FlixBD: duplicate link series %s S%s %r %s [%s] (existing id=%s)",
                            content_id,
                            season_num,
                            item_label,
                            quality,
                            entry_language,
                            existing_id,
                        )
                        if existing_id:
                            created_ids.append(existing_id)
                        succeeded += 1
                        continue

                    if resp.status_code in (400, 422):
                        logger.warning(
                            "FlixBD: add series link error S%s %r %s [%s]: %s",
                            season_num,
                            item_label,
                            quality,
                            entry_language,
                            resp.text,
                        )
                        failed.append(
                            {
                                "season_number": season_num,
                                "label": item_label,
                                "quality": str(quality),
                                "language": entry_language,
                                "filename": str(drive_item.get("f") or "").strip(),
                                "reason": f"HTTP {resp.status_code}",
                                "detail": (resp.text or "")[:500],
                            }
                        )
                        continue

                    if resp.status_code in _RETRYABLE_FLIXBD_HTTP:
                        logger.warning(
                            "FlixBD: add series link HTTP %s S%s %r %s [%s] after retries: %s",
                            resp.status_code,
                            season_num,
                            item_label,
                            quality,
                            entry_language,
                            (resp.text or "")[:300],
                        )
                        failed.append(
                            {
                                "season_number": season_num,
                                "label": item_label,
                                "quality": str(quality),
                                "language": entry_language,
                                "filename": str(drive_item.get("f") or "").strip(),
                                "reason": f"HTTP {resp.status_code}",
                                "detail": (resp.text or "")[:500],
                            }
                        )
                        continue

                    try:
                        resp.raise_for_status()
                    except httpx.HTTPStatusError as e:
                        failed.append(
                            {
                                "season_number": season_num,
                                "label": item_label,
                                "quality": str(quality),
                                "language": entry_language,
                                "filename": str(drive_item.get("f") or "").strip(),
                                "reason": f"HTTP {e.response.status_code}",
                                "detail": (e.response.text or "")[:500],
                            }
                        )
                        continue

                    try:
                        dl_id = _safe_json(
                            resp,
                            f"series {content_id} add_download S{season_num} {quality}",
                        )["data"]["id"]
                    except (KeyError, TypeError, ValueError) as e:
                        failed.append(
                            {
                                "season_number": season_num,
                                "label": item_label,
                                "quality": str(quality),
                                "language": entry_language,
                                "filename": str(drive_item.get("f") or "").strip(),
                                "reason": "bad_response",
                                "detail": str(e)[:500],
                            }
                        )
                        continue
                    created_ids.append(dl_id)
                    succeeded += 1
                    logger.info(
                        "FlixBD: added series link id=%s series=%s S%s %r %s [%s]",
                        dl_id,
                        content_id,
                        season_num,
                        item_label,
                        quality,
                        entry_language,
                    )

    return {
        "created_ids": created_ids,
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
    }
