import logging
import re

import httpx

from upload.utils.tv_items import tv_items_overlap

from .flixbd_api_base import _TIMEOUT, _get_config, _headers, _safe_json
from .flixbd_api_content import _derive_language_string

logger = logging.getLogger(__name__)


def add_movie_download_links(
    content_id: int,
    drive_links: dict,
    file_sizes: dict,
    movie_data: dict,
    server_name: str = "GDrive",
) -> list:
    """Add download links for a movie."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/movies/{content_id}/downloads"
    created_ids = []
    language = _derive_language_string(movie_data)

    for quality, drive_url in drive_links.items():
        payload = {
            "server_name": server_name,
            "download_link": drive_url,
            "quality": quality,
        }
        if language:
            payload["language"] = language
        size = file_sizes.get(quality)
        if size:
            payload["size"] = size

        try:
            with httpx.Client(timeout=_TIMEOUT) as client:
                resp = client.post(endpoint, json=payload, headers=_headers(api_key))

            if resp.status_code == 409:
                body = _safe_json(resp, f"movie {content_id} downloads 409")
                existing_id = body.get("errors", {}).get("existing_download_id")
                logger.info(
                    "FlixBD: duplicate link for movie %s %s (existing id=%s)",
                    content_id,
                    quality,
                    existing_id,
                )
                if existing_id:
                    created_ids.append(existing_id)
                continue

            if resp.status_code in (400, 422):
                logger.warning(
                    "FlixBD: add download link error for movie %s %s: %s",
                    content_id,
                    quality,
                    resp.text,
                )
                continue

            resp.raise_for_status()
            dl_id = _safe_json(resp, f"movie {content_id} add_download {quality}")["data"]["id"]
            created_ids.append(dl_id)
            logger.info(
                "FlixBD: added download link id=%s movie=%s quality=%s",
                dl_id,
                content_id,
                quality,
            )

        except RuntimeError:
            raise
        except Exception as e:
            logger.error("FlixBD: failed to add link for movie %s %s: %s", content_id, quality, e)

    return created_ids


def list_movie_downloads(content_id: int) -> list[dict]:
    """GET movie downloads list (best-effort)."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/movies/{content_id}/downloads"
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
    endpoint = f"{api_url}/api/v1/downloads/{download_row_id}"
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


def fetch_movie_drive_links_by_quality(content_id: int) -> dict[str, str]:
    """Build ``{quality: drive_url}`` from movie download rows."""
    out: dict[str, str] = {}
    for row in list_movie_downloads(content_id):
        link = row.get("download_link") or row.get("url") or row.get("link")
        if not link or "drive.google.com" not in str(link):
            continue
        quality = row.get("quality")
        if not quality:
            continue
        quality = str(quality).strip()
        if quality and quality not in out:
            out[quality] = str(link).strip()
    return out


def list_series_downloads(content_id: int) -> list[dict]:
    """GET series downloads list (best-effort)."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/series/{content_id}/downloads"
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
    endpoint = f"{api_url}/api/v1/downloads/{download_row_id}"
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
        item["resolutions"][quality] = str(link).strip()
        size = row.get("size")
        if size:
            sizes = item.setdefault("sizes", {})
            sizes[quality] = str(size).strip()

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
) -> list:
    """Add download links for a series."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/series/{content_id}/downloads"
    created_ids = []
    language = _derive_language_string(tvshow_data)

    for season in seasons_data:
        season_num = season.get("season_number")
        for item in season.get("download_items", []):
            item_label = item.get("label", "")
            resolutions = item.get("resolutions", {})

            for quality, drive_url in resolutions.items():
                if not drive_url or not drive_url.startswith("https://drive.google.com"):
                    continue

                payload = {
                    "server_name": server_name,
                    "download_link": drive_url,
                    "quality": quality,
                    "season_number": str(season_num).zfill(2),
                }

                ep_val = _episode_number_for_flixbd_item(item)
                if ep_val is not None:
                    payload["episode_number"] = ep_val

                if language:
                    payload["language"] = language

                size = file_sizes_map.get((season_num, item_label, quality)) or item.get("sizes", {}).get(quality)
                if size:
                    payload["size"] = size

                try:
                    with httpx.Client(timeout=_TIMEOUT) as client:
                        resp = client.post(endpoint, json=payload, headers=_headers(api_key))

                    if resp.status_code == 409:
                        body = _safe_json(resp, f"series {content_id} S{season_num} downloads 409")
                        existing_id = body.get("errors", {}).get("existing_download_id")
                        logger.info(
                            "FlixBD: duplicate link series %s S%s %r %s (existing id=%s)",
                            content_id,
                            season_num,
                            item_label,
                            quality,
                            existing_id,
                        )
                        if existing_id:
                            created_ids.append(existing_id)
                        continue

                    if resp.status_code in (400, 422):
                        logger.warning(
                            "FlixBD: add series link error S%s %r %s: %s",
                            season_num,
                            item_label,
                            quality,
                            resp.text,
                        )
                        continue

                    resp.raise_for_status()
                    dl_id = _safe_json(
                        resp,
                        f"series {content_id} add_download S{season_num} {quality}",
                    )["data"]["id"]
                    created_ids.append(dl_id)
                    logger.info(
                        "FlixBD: added series link id=%s series=%s S%s %r %s",
                        dl_id,
                        content_id,
                        season_num,
                        item_label,
                        quality,
                    )

                except RuntimeError:
                    raise
                except Exception as e:
                    logger.error(
                        "FlixBD: failed to add series link S%s %r %s: %s",
                        season_num,
                        item_label,
                        quality,
                        e,
                    )

    return created_ids
