"""
Site API Client
===============
Handles all communication with the target site REST API.

Flow for publishing:
  1. search() -- check if content already exists (max 5 results)
  2. If found -> return existing site_content_id
  3. If not found -> create_movie() / create_series() -> get new site_content_id
  4. If content already existed -> patch_movie_title / patch_series_title (PATCH body: ``title`` only)
  5. add_download_links() -- attach Drive links with quality, language, size

All methods raise on fatal errors. Callers should catch and log.
Title-only PATCH failures are logged; download links are still added.
"""

import json
import logging
import re
import httpx
from llm.schema.blocked_names import SITE_NAME

logger = logging.getLogger(__name__)

# Shared httpx timeout (read bumped for slow API / Docker → avoids truncated empty reads)
_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=5.0)

# FlixBD MySQL columns are often VARCHAR(255); longer LLM meta strings cause PDO "Data too long" + HTML error body.
_FLIXBD_MAX_META_TITLE = 255
_FLIXBD_MAX_META_KEYWORDS = 255
_FLIXBD_MAX_META_DESCRIPTION = 2048


def _get_config():
    """
    Lazily load FlixBDSettings to avoid import-time DB access.
    Returns (api_url, api_key) or raises RuntimeError if not configured / disabled.
    """
    from settings.models import FlixBDSettings
    cfg = FlixBDSettings.objects.first()
    if not cfg:
        raise RuntimeError(f"{SITE_NAME} settings not configured. Add it in Admin → Settings → {SITE_NAME} Settings.")
    if not cfg.is_enabled:
        raise RuntimeError(f"{SITE_NAME} publishing is disabled in settings.")
    api_url = cfg.api_url.rstrip("/")
    return api_url, cfg.api_key


def _headers(api_key: str) -> dict:
    return {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }


def _safe_json(resp: httpx.Response, operation: str) -> dict:
    """
    Parse API JSON. Failures often look like JSONDecodeError with no context — loggable body here.
    """
    raw = resp.text or ""
    stripped = raw.strip()
    ct = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
    if not stripped:
        raise RuntimeError(
            f"FlixBD {operation}: empty response body "
            f"(HTTP {resp.status_code}, content-type={ct!r})"
        )
    try:
        out = json.loads(raw)
    except json.JSONDecodeError as e:
        snippet = stripped[:1200].replace("\n", "\\n")
        raise RuntimeError(
            f"FlixBD {operation}: response is not JSON "
            f"(HTTP {resp.status_code}, content-type={ct!r}): {e}; "
            f"body_start={snippet!r}"
        ) from e
    if not isinstance(out, dict):
        raise RuntimeError(
            f"FlixBD {operation}: expected JSON object, got {type(out).__name__}"
        )
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Search
# ──────────────────────────────────────────────────────────────────────────────

def search(title: str, content_type: str = "all") -> dict | None:
    """
    Search FlixBD for existing content by title.

    Returns:
        dict with keys: {"id": int, "title": str}
        or None if not found.

    Strategy:
        - Fetch max 5 results (per_page=5, page=1)
        - Prefer exact match, fall back to first substring match
    """
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/search"
    params = {
        "q": title,
        "type": content_type,
        "per_page": 5,
        "page": 1,
    }

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.get(endpoint, params=params, headers=_headers(api_key))

        if resp.status_code == 422:
            logger.warning(f"FlixBD search: missing q param -- title='{title}'")
            return None

        resp.raise_for_status()
        body = _safe_json(resp, "search")

        results = body.get("data", [])
        if not results:
            logger.info(f"FlixBD search: no results for '{title}'")
            return None

        title_lower = title.lower().strip()

        # 1st pass: exact title match
        # Site titles are stored as "Inception 2010 WEB-DL Hindi - {SITE_NAME}"
        # We extract the title portion by finding a 4-digit year token
        _year_re = re.compile(r'\b(19|20)\d{2}\b')

        for item in results:
            item_title = item.get("title", "")
            # Extract everything before the year as the clean title
            year_match = _year_re.search(item_title)
            if year_match:
                clean = item_title[:year_match.start()].strip().lower()
            else:
                clean = item_title.split("(")[0].strip().lower()

            if clean == title_lower:
                logger.info(f"FlixBD search: exact match id={item['id']} title='{item_title}'")
                return {"id": item["id"], "title": item_title}

        # 2nd pass: substring match — clean title appears anywhere in site title
        for item in results:
            item_title = item.get("title", "")
            if title_lower in item_title.lower():
                logger.info(f"FlixBD search: substring match id={item['id']} title='{item_title}'")
                return {"id": item["id"], "title": item_title}

        logger.info(f"FlixBD search: results returned but no match for '{title}'")
        return None


    except RuntimeError:
        raise
    except Exception as e:
        logger.error(f"FlixBD search failed for '{title}': {e}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Create Movie
# ──────────────────────────────────────────────────────────────────────────────

def create_movie(movie_data: dict) -> int:
    """
    Create a new movie on FlixBD using LLM-extracted data.
    Returns the new movie ID.
    """
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/movies"

    payload = _build_movie_payload(movie_data)
    logger.info(f"FlixBD: creating movie '{payload.get('title')}'")

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
        logger.info(f"FlixBD: movie created -- id={content_id} title={body['data'].get('title')}")
        return content_id

    except RuntimeError as e:
        logger.error(f"FlixBD create_movie failed: {e}")
        raise
    except Exception as e:
        logger.error(f"FlixBD create_movie failed: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Create Series
# ──────────────────────────────────────────────────────────────────────────────

def create_series(tvshow_data: dict) -> int:
    """
    Create a new series on FlixBD using LLM-extracted data.
    Returns the new series ID.
    """
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/series"

    payload = _build_series_payload(tvshow_data)
    logger.info(f"FlixBD: creating series '{payload.get('title')}'")

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
        logger.info(f"FlixBD: series created -- id={content_id} title={body['data'].get('title')}")
        return content_id

    except RuntimeError as e:
        logger.error(f"FlixBD create_series failed: {e}")
        raise
    except Exception as e:
        logger.error(f"FlixBD create_series failed: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Patch title only (existing movie / series on FlixBD)
# ──────────────────────────────────────────────────────────────────────────────


def _display_movie_title(movie_data: dict) -> str:
    return movie_data.get("website_movie_title") or movie_data.get("title", "Unknown")


def _display_series_title(tvshow_data: dict) -> str:
    return tvshow_data.get("website_tvshow_title") or tvshow_data.get("title", "Unknown")


def movie_website_title(movie_data: dict) -> str:
    """Public alias for logging / UI — same string sent to FlixBD as movie ``title``."""
    return _display_movie_title(movie_data)


def series_website_title(tvshow_data: dict) -> str:
    """Public alias — same string sent to FlixBD as series ``title``."""
    return _display_series_title(tvshow_data)


def patch_movie_title(content_id: int, movie_data: dict) -> bool:
    """
    PATCH ``/api/v1/movies/{id}`` with ``{"title": ...}`` only — sync display title when
    re-processing an item that already has ``site_content_id``. Does not change meta, plot, poster, etc.
    """
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/movies/{content_id}"
    display = _display_movie_title(movie_data)
    payload = {"title": display}
    logger.info(f"FlixBD: PATCH movie id={content_id} title-only -> {display[:100]!r}")

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.patch(endpoint, json=payload, headers=_headers(api_key))

        if resp.status_code in (404, 405):
            logger.warning(
                f"FlixBD: movie title PATCH not available (HTTP {resp.status_code}) id={content_id}"
            )
            return False
        if resp.status_code in (400, 422):
            logger.warning(f"FlixBD: patch_movie_title id={content_id}: {resp.text[:500]}")
            return False

        resp.raise_for_status()
        logger.info(f"FlixBD: movie id={content_id} title updated")
        return True
    except Exception as e:
        logger.warning(f"FlixBD: patch_movie_title failed id={content_id}: {e}")
        return False


def patch_series_title(content_id: int, tvshow_data: dict) -> bool:
    """PATCH ``/api/v1/series/{id}`` with ``{"title": ...}`` only."""
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/series/{content_id}"
    display = _display_series_title(tvshow_data)
    payload = {"title": display}
    logger.info(f"FlixBD: PATCH series id={content_id} title-only -> {display[:100]!r}")

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.patch(endpoint, json=payload, headers=_headers(api_key))

        if resp.status_code in (404, 405):
            logger.warning(
                f"FlixBD: series title PATCH not available (HTTP {resp.status_code}) id={content_id}"
            )
            return False
        if resp.status_code in (400, 422):
            logger.warning(f"FlixBD: patch_series_title id={content_id}: {resp.text[:500]}")
            return False

        resp.raise_for_status()
        logger.info(f"FlixBD: series id={content_id} title updated")
        return True
    except Exception as e:
        logger.warning(f"FlixBD: patch_series_title failed id={content_id}: {e}")
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Add Download Links -- Movie
# ──────────────────────────────────────────────────────────────────────────────

def add_movie_download_links(
    content_id: int,
    drive_links: dict,
    file_sizes: dict,
    movie_data: dict,
    server_name: str = "GDrive",
) -> list:
    """
    Add download links for a movie.

    Args:
        content_id: FlixBD movie ID
        drive_links: {quality: drive_url}
        file_sizes: {quality: "2.1 GB"} -- from os.path.getsize after download
        movie_data: full LLM-extracted movie dict (for language field)
        server_name: display name for download server

    Returns:
        List of created download link IDs on FlixBD.
    """
    api_url, api_key = _get_config()
    endpoint = f"{api_url}/api/v1/movies/{content_id}/downloads"
    created_ids = []

    # Derive language string from LLM-extracted languages array
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
                # Duplicate -- Drive file already linked, skip gracefully
                body = _safe_json(resp, f"movie {content_id} downloads 409")
                existing_id = body.get("errors", {}).get("existing_download_id")
                logger.info(f"FlixBD: duplicate link for movie {content_id} {quality} (existing id={existing_id})")
                if existing_id:
                    created_ids.append(existing_id)
                continue

            if resp.status_code in (400, 422):
                logger.warning(f"FlixBD: add download link error for movie {content_id} {quality}: {resp.text}")
                continue

            resp.raise_for_status()
            dl_id = _safe_json(resp, f"movie {content_id} add_download {quality}")["data"]["id"]
            created_ids.append(dl_id)
            logger.info(f"FlixBD: added download link id={dl_id} movie={content_id} quality={quality}")

        except RuntimeError:
            raise
        except Exception as e:
            logger.error(f"FlixBD: failed to add link for movie {content_id} {quality}: {e}")

    return created_ids


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
        i = int(raw)
        return str(i).zfill(2) if i >= 0 else None

    s = str(raw).strip()
    if not s:
        return None
    if "-" in s or "–" in s:
        parts = re.split(r"[-–]", s, maxsplit=1)
        if len(parts) == 2:
            a, b = parts[0].strip(), parts[1].strip()
            if a.isdigit() and b.isdigit():
                return f"{int(a):02d}-{int(b):02d}"
        return None
    if s.isdigit():
        return str(int(s)).zfill(2)
    return None


def _episode_number_for_flixbd_item(item: dict) -> str | None:
    """
    Value for FlixBD ``episode_number`` (``05``, ``01-08``) or ``None`` (combo pack).

    * ``season_number`` on each season dict is the only season source for the API payload
      (e.g. ``4`` → ``"04"``). Never infer season from the label.
    * **Always** use ``episode_range`` first — extraction keeps it correct (``"01"``,
      ``1``, ``"01-04"``). Only if it is missing or unparseable, fall back to the label
      using ``Episode`` / ``EP`` (never the first ``\\d+`` in the whole string, which
      would catch ``4`` from ``Season 4 Episode 01``).
    """
    item_type = item.get("type", "")
    if item_type == "combo_pack":
        return None

    label = (item.get("label") or "").strip()

    v = _parse_episode_range_field(item.get("episode_range"))
    if v:
        return v

    if item_type == "single_episode":
        m = re.search(r"(?i)(?:episode|ep\.?)\s*(\d+)", label)
        if m:
            return str(int(m.group(1))).zfill(2)
        return None

    if item_type == "partial_combo":
        m = re.search(r"(?i)(?:episode|ep\.?)\s*(\d+)\s*[-–]\s*(\d+)", label)
        if m:
            return f"{int(m.group(1)):02d}-{int(m.group(2)):02d}"
        m = re.search(r"(?i)(?:episode|ep\.?)\s*(\d+)", label)
        if m:
            return str(int(m.group(1))).zfill(2)
        return None

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Add Download Links -- Series
# ──────────────────────────────────────────────────────────────────────────────

def add_series_download_links(
    content_id: int,
    seasons_data: list,
    file_sizes_map: dict,
    tvshow_data: dict,
    server_name: str = "GDrive",
) -> list:
    """
    Add download links for a series (season/episode aware).

    Args:
        content_id: FlixBD series ID
        seasons_data: list of season dicts from tvshow_data["seasons"]
                      Each download_item["resolutions"] has Drive links at this point.
        file_sizes_map: {(season_num, label, quality): "size_str"}
        tvshow_data: full LLM-extracted tvshow dict (for language)
        server_name: display name for download server

    Returns:
        List of created download link IDs.
    """
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
                    # Skip non-Drive links (not yet uploaded)
                    continue

                payload = {
                    "server_name": server_name,
                    "download_link": drive_url,
                    "quality": quality,
                    # Season only from structured field (e.g. 4 → "04"), not from label text.
                    "season_number": str(season_num).zfill(2),
                }

                # episode_number: always episode_range first; label only if range missing (see _episode_number_for_flixbd_item).
                ep_val = _episode_number_for_flixbd_item(item)
                if ep_val is not None:
                    payload["episode_number"] = ep_val

                if language:
                    payload["language"] = language

                # size: first from file_sizes_map (current run), then from item["sizes"] (persisted)
                size = (
                    file_sizes_map.get((season_num, item_label, quality))
                    or item.get("sizes", {}).get(quality)
                )
                if size:
                    payload["size"] = size


                try:
                    with httpx.Client(timeout=_TIMEOUT) as client:
                        resp = client.post(endpoint, json=payload, headers=_headers(api_key))

                    if resp.status_code == 409:
                        body = _safe_json(
                            resp,
                            f"series {content_id} S{season_num} downloads 409",
                        )
                        existing_id = body.get("errors", {}).get("existing_download_id")
                        logger.info(
                            f"FlixBD: duplicate link series {content_id} S{season_num} "
                            f"'{item_label}' {quality} (existing id={existing_id})"
                        )
                        if existing_id:
                            created_ids.append(existing_id)
                        continue

                    if resp.status_code in (400, 422):
                        logger.warning(
                            f"FlixBD: add series link error S{season_num} '{item_label}' {quality}: {resp.text}"
                        )
                        continue

                    resp.raise_for_status()
                    dl_id = _safe_json(
                        resp,
                        f"series {content_id} add_download S{season_num} {quality}",
                    )["data"]["id"]
                    created_ids.append(dl_id)
                    logger.info(
                        f"FlixBD: added series link id={dl_id} series={content_id} "
                        f"S{season_num} '{item_label}' {quality}"
                    )

                except RuntimeError:
                    raise
                except Exception as e:
                    logger.error(
                        f"FlixBD: failed to add series link S{season_num} '{item_label}' {quality}: {e}"
                    )

    return created_ids


# ──────────────────────────────────────────────────────────────────────────────
# Payload Builders
# ──────────────────────────────────────────────────────────────────────────────

def _build_movie_payload(movie_data: dict) -> dict:
    """
    Map LLM-extracted movie_data fields to FlixBD API create-movie payload.
    website_movie_title (LLM-generated formatted title) is used as the FlixBD title.
    """
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
        payload, "meta_description", movie_data.get("meta_description"), _FLIXBD_MAX_META_DESCRIPTION
    )
    _set_if_truncated(
        payload, "meta_keywords", movie_data.get("meta_keywords"), _FLIXBD_MAX_META_KEYWORDS
    )

    year = movie_data.get("year")
    if year:
        payload["release_date"] = f"{year}-01-01"

    # genres -- split comma-separated string to list
    genre = movie_data.get("genre", "")
    if genre:
        payload["genres"] = [g.strip() for g in genre.split(",") if g.strip()]

    # languages -- LLM gives array directly
    languages = movie_data.get("languages", [])
    if languages:
        payload["languages"] = languages

    # countries -- LLM gives array directly
    countries = movie_data.get("countries", [])
    if countries:
        payload["countries"] = countries

    screenshots = movie_data.get("screen_shots_url", [])
    if screenshots:
        payload["screenshots"] = screenshots[:10]

    return payload


def _build_series_payload(tvshow_data: dict) -> dict:
    """
    Map LLM-extracted tvshow_data fields to FlixBD API create-series payload.
    website_tvshow_title (LLM-generated formatted title) is used as the FlixBD title.
    """
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
        payload, "meta_description", tvshow_data.get("meta_description"), _FLIXBD_MAX_META_DESCRIPTION
    )
    _set_if_truncated(
        payload, "meta_keywords", tvshow_data.get("meta_keywords"), _FLIXBD_MAX_META_KEYWORDS
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
    """Strip and cap string length for FlixBD create/patch payloads."""
    if value is None or max_len <= 0:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) <= max_len:
        return s
    return s[:max_len]


def _set_if_truncated(payload: dict, key: str, value, max_len: int) -> None:
    """Like _set_if but enforce max_len (avoids SQLSTATE 22001 on meta_* columns)."""
    t = _truncate_api_text(value, max_len)
    if not t:
        return
    if isinstance(value, str) and len(value.strip()) > len(t):
        logger.debug(
            "FlixBD payload: truncated %r %d → %d chars",
            key,
            len(value.strip()),
            len(t),
        )
    payload[key] = t


def _derive_language_string(data: dict) -> str:
    """
    Build a comma-separated language string from LLM-extracted 'languages' array.
    Used for the download link 'language' field.
    e.g. ["Hindi", "English"] -> "Hindi, English"
    Returns empty string if not available.
    """
    languages = data.get("languages", [])
    if isinstance(languages, list) and languages:
        return ", ".join(str(l) for l in languages if l)
    return ""


def _parse_episode_number(item: dict) -> int | None:
    """
    For single_episode items, parse episode number from episode_range.
    Returns None for combo_pack or partial_combo.
    """
    if item.get("type") != "single_episode":
        return None
    ep_range = item.get("episode_range", "")
    if not ep_range:
        return None
    # Take the first number in case of range like '01-08'
    first_part = str(ep_range).strip().split("-")[0].strip()
    try:
        return int(first_part)  # int('01') == 1, int('10') == 10
    except (ValueError, AttributeError):
        return None


# ──────────────────────────────────────────────────────────────────────────────
# File Size Helper
# ──────────────────────────────────────────────────────────────────────────────

def format_file_size(size_bytes: int) -> str:
    """
    Convert bytes to human-readable string.
    e.g. 1_234_567_890 -> '1.15 GB'
    """
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / (1024 ** 3):.2f} GB"
    elif size_bytes >= 1024 ** 2:
        return f"{size_bytes / (1024 ** 2):.1f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes} B"
