import logging
import re

import httpx

from .flixbd_api_base import (
    _FLIXBD_MAX_META_DESCRIPTION,
    _FLIXBD_MAX_META_KEYWORDS,
    _FLIXBD_MAX_META_TITLE,
    _TIMEOUT,
    _get_config,
    _headers,
    _safe_json,
)

logger = logging.getLogger(__name__)


def search(title: str, content_type: str = "all") -> dict | None:
    """
    Search FlixBD for existing content by title.
    Returns a simple ``{"id": int, "title": str}`` match or ``None``.
    """
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/search"
    params = {"q": title, "type": content_type, "per_page": 5, "page": 1}

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.get(endpoint, params=params, headers=_headers(api_key))

        if resp.status_code == 422:
            logger.warning("FlixBD search: missing q param -- title=%r", title)
            return None

        resp.raise_for_status()
        body = _safe_json(resp, "search")

        results = body.get("data", [])
        if not results:
            logger.info("FlixBD search: no results for %r", title)
            return None

        title_lower = title.lower().strip()
        year_re = re.compile(r"\b(19|20)\d{2}\b")

        for item in results:
            item_title = item.get("title", "")
            year_match = year_re.search(item_title)
            clean = (
                item_title[:year_match.start()].strip().lower()
                if year_match
                else item_title.split("(")[0].strip().lower()
            )
            if clean == title_lower:
                logger.info("FlixBD search: exact match id=%s title=%r", item["id"], item_title)
                return {"id": item["id"], "title": item_title}

        for item in results:
            item_title = item.get("title", "")
            if title_lower in item_title.lower():
                logger.info(
                    "FlixBD search: substring match id=%s title=%r",
                    item["id"],
                    item_title,
                )
                return {"id": item["id"], "title": item_title}

        logger.info("FlixBD search: results returned but no match for %r", title)
        return None

    except RuntimeError:
        raise
    except Exception as e:
        logger.error("FlixBD search failed for %r: %s", title, e)
        raise


def _display_movie_title(movie_data: dict) -> str:
    return movie_data.get("website_movie_title") or movie_data.get("title", "Unknown")


def _display_series_title(tvshow_data: dict) -> str:
    return tvshow_data.get("website_tvshow_title") or tvshow_data.get("title", "Unknown")


def movie_website_title(movie_data: dict) -> str:
    """Public alias for logging / UI — same string sent to FlixBD as movie title."""
    return _display_movie_title(movie_data)


def series_website_title(tvshow_data: dict) -> str:
    """Public alias — same string sent to FlixBD as series title."""
    return _display_series_title(tvshow_data)


def create_movie(movie_data: dict) -> int:
    """Create a new movie on FlixBD using extracted data."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/movies"

    payload = _build_movie_payload(movie_data)
    logger.info("FlixBD: creating movie %r", payload.get("title"))

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.post(endpoint, json=payload, headers=_headers(api_key))

        if resp.status_code == 422:
            raise RuntimeError(f"FlixBD create_movie validation error: {resp.text}")
        if resp.status_code == 400:
            raise RuntimeError(f"FlixBD create_movie bad request: {resp.text}")

        resp.raise_for_status()
        body = _safe_json(resp, "create_movie")
        content_id = body["data"]["id"]
        logger.info(
            "FlixBD: movie created -- id=%s title=%r",
            content_id,
            body["data"].get("title"),
        )
        return content_id

    except RuntimeError as e:
        logger.error("FlixBD create_movie failed: %s", e)
        raise
    except Exception as e:
        logger.error("FlixBD create_movie failed: %s", e)
        raise


def create_series(tvshow_data: dict) -> int:
    """Create a new series on FlixBD using extracted data."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/series"

    payload = _build_series_payload(tvshow_data)
    logger.info("FlixBD: creating series %r", payload.get("title"))

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.post(endpoint, json=payload, headers=_headers(api_key))

        if resp.status_code == 422:
            raise RuntimeError(f"FlixBD create_series validation error: {resp.text}")
        if resp.status_code == 400:
            raise RuntimeError(f"FlixBD create_series bad request: {resp.text}")

        resp.raise_for_status()
        body = _safe_json(resp, "create_series")
        content_id = body["data"]["id"]
        logger.info(
            "FlixBD: series created -- id=%s title=%r",
            content_id,
            body["data"].get("title"),
        )
        return content_id

    except RuntimeError as e:
        logger.error("FlixBD create_series failed: %s", e)
        raise
    except Exception as e:
        logger.error("FlixBD create_series failed: %s", e)
        raise


def patch_movie_title(content_id: int, movie_data: dict) -> bool:
    """PATCH only the display title on an existing movie."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/movies/{content_id}"
    display = _display_movie_title(movie_data)
    payload = {"title": display}
    logger.info("FlixBD: PATCH movie id=%s title-only -> %r", content_id, display[:100])

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.patch(endpoint, json=payload, headers=_headers(api_key))

        if resp.status_code in (404, 405):
            logger.warning(
                "FlixBD: movie title PATCH not available (HTTP %s) id=%s",
                resp.status_code,
                content_id,
            )
            return False
        if resp.status_code in (400, 422):
            logger.warning("FlixBD: patch_movie_title id=%s: %s", content_id, resp.text[:500])
            return False

        resp.raise_for_status()
        logger.info("FlixBD: movie id=%s title updated", content_id)
        return True
    except Exception as e:
        logger.warning("FlixBD: patch_movie_title failed id=%s: %s", content_id, e)
        return False


def patch_series_title(content_id: int, tvshow_data: dict) -> bool:
    """PATCH only the display title on an existing series."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/series/{content_id}"
    display = _display_series_title(tvshow_data)
    payload = {"title": display}
    logger.info("FlixBD: PATCH series id=%s title-only -> %r", content_id, display[:100])

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.patch(endpoint, json=payload, headers=_headers(api_key))

        if resp.status_code in (404, 405):
            logger.warning(
                "FlixBD: series title PATCH not available (HTTP %s) id=%s",
                resp.status_code,
                content_id,
            )
            return False
        if resp.status_code in (400, 422):
            logger.warning("FlixBD: patch_series_title id=%s: %s", content_id, resp.text[:500])
            return False

        resp.raise_for_status()
        logger.info("FlixBD: series id=%s title updated", content_id)
        return True
    except Exception as e:
        logger.warning("FlixBD: patch_series_title failed id=%s: %s", content_id, e)
        return False


def _build_movie_payload(movie_data: dict) -> dict:
    """Map extracted movie fields to the FlixBD create-movie payload."""
    display_title = _display_movie_title(movie_data)

    payload: dict = {
        "title": display_title,
        "status": "published",
        "is_adult": bool(movie_data.get("is_adult")),
    }

    _set_if(payload, "description", movie_data.get("plot"))
    _set_if(payload, "poster", movie_data.get("poster_url"))
    _set_if(payload, "rating", movie_data.get("rating"))
    _set_if(payload, "director", movie_data.get("director"))
    _set_if(payload, "cast", movie_data.get("cast"))
    _set_if(payload, "imdb_id", movie_data.get("imdb_id"))
    _set_if(payload, "tmdb_id", movie_data.get("tmdb_id"))
    _set_if_truncated(payload, "meta_title", movie_data.get("meta_title"), _FLIXBD_MAX_META_TITLE)
    _set_if_truncated(
        payload,
        "meta_description",
        movie_data.get("meta_description"),
        _FLIXBD_MAX_META_DESCRIPTION,
    )
    _set_if_truncated(
        payload,
        "meta_keywords",
        movie_data.get("meta_keywords"),
        _FLIXBD_MAX_META_KEYWORDS,
    )

    year = movie_data.get("year")
    if year:
        payload["release_date"] = f"{year}-01-01"

    genre = movie_data.get("genre", "")
    if genre:
        payload["genres"] = [g.strip() for g in genre.split(",") if g.strip()]

    languages = movie_data.get("languages", [])
    if languages:
        payload["languages"] = languages

    countries = movie_data.get("countries", [])
    if countries:
        payload["countries"] = countries

    screenshots = movie_data.get("screen_shots_url", [])
    if screenshots:
        payload["screenshots"] = screenshots[:10]

    return payload


def _build_series_payload(tvshow_data: dict) -> dict:
    """Map extracted series fields to the FlixBD create-series payload."""
    display_title = _display_series_title(tvshow_data)

    payload: dict = {
        "title": display_title,
        "status": "published",
        "is_adult": bool(tvshow_data.get("is_adult")),
    }

    _set_if(payload, "description", tvshow_data.get("plot"))
    _set_if(payload, "poster", tvshow_data.get("poster_url"))
    _set_if(payload, "rating", tvshow_data.get("rating"))
    _set_if(payload, "director", tvshow_data.get("director"))
    _set_if(payload, "cast_info", tvshow_data.get("cast_info"))
    _set_if(payload, "imdb_id", tvshow_data.get("imdb_id"))
    _set_if(payload, "tmdb_id", tvshow_data.get("tmdb_id"))
    _set_if(payload, "total_seasons", tvshow_data.get("total_seasons"))
    _set_if_truncated(payload, "meta_title", tvshow_data.get("meta_title"), _FLIXBD_MAX_META_TITLE)
    _set_if_truncated(
        payload,
        "meta_description",
        tvshow_data.get("meta_description"),
        _FLIXBD_MAX_META_DESCRIPTION,
    )
    _set_if_truncated(
        payload,
        "meta_keywords",
        tvshow_data.get("meta_keywords"),
        _FLIXBD_MAX_META_KEYWORDS,
    )

    year = tvshow_data.get("year")
    if year:
        payload["release_date"] = f"{year}-01-01"

    genre = tvshow_data.get("genre", "")
    if genre:
        payload["genres"] = [g.strip() for g in genre.split(",") if g.strip()]

    languages = tvshow_data.get("languages", [])
    if languages:
        payload["languages"] = languages

    countries = tvshow_data.get("countries", [])
    if countries:
        payload["countries"] = countries

    screenshots = tvshow_data.get("screen_shots_url", [])
    if screenshots:
        payload["screenshots"] = screenshots[:10]

    return payload


def _set_if(payload: dict, key: str, value) -> None:
    """Only add key to payload if value is truthy."""
    if value:
        payload[key] = value


def _truncate_api_text(value, max_len: int) -> str | None:
    """Strip and cap string length for FlixBD payloads."""
    if value is None or max_len <= 0:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= max_len:
        return text
    return text[:max_len]


def _set_if_truncated(payload: dict, key: str, value, max_len: int) -> None:
    """Like _set_if but enforce max_len."""
    truncated = _truncate_api_text(value, max_len)
    if not truncated:
        return
    if isinstance(value, str) and len(value.strip()) > len(truncated):
        logger.debug(
            "FlixBD payload: truncated %r %d → %d chars",
            key,
            len(value.strip()),
            len(truncated),
        )
    payload[key] = truncated


def _derive_language_string(data: dict) -> str:
    """
    Build a comma-separated language string from an extracted ``languages`` array.
    """
    languages = data.get("languages", [])
    if isinstance(languages, list) and languages:
        return ", ".join(str(lang) for lang in languages if lang)
    return ""


def _parse_episode_number(item: dict) -> int | None:
    """For single_episode items, parse episode number from episode_range."""
    if item.get("type") != "single_episode":
        return None
    ep_range = item.get("episode_range", "")
    if not ep_range:
        return None
    first_part = str(ep_range).strip().split("-")[0].strip()
    try:
        return int(first_part)
    except (ValueError, AttributeError):
        return None


def format_file_size(size_bytes: int) -> str:
    """
    Convert bytes to human-readable string.
    e.g. 1_234_567_890 -> '1.15 GB'
    """
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / (1024 ** 3):.2f} GB"
    if size_bytes >= 1024 ** 2:
        return f"{size_bytes / (1024 ** 2):.1f} MB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes} B"
