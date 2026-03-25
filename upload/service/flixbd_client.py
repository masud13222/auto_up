"""
Site API Client
===============
Handles all communication with the target site REST API.

Flow for publishing:
  1. search() -- check if content already exists (max 5 results)
  2. If found -> return existing site_content_id (just add new download links)
  3. If not found -> create_movie() / create_series() -> get new site_content_id
  4. add_download_links() -- attach Drive links with quality, language, size

All methods raise on fatal errors. Callers should catch and log.
"""

import logging
import re
import httpx
from llm.schema.blocked_names import SITE_NAME

logger = logging.getLogger(__name__)

# Shared httpx timeout config (seconds)
_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=5.0)


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
        body = resp.json()

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
        body = resp.json()
        content_id = body["data"]["id"]
        logger.info(f"FlixBD: movie created -- id={content_id} title={body['data'].get('title')}")
        return content_id

    except RuntimeError:
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
        body = resp.json()
        content_id = body["data"]["id"]
        logger.info(f"FlixBD: series created -- id={content_id} title={body['data'].get('title')}")
        return content_id

    except RuntimeError:
        raise
    except Exception as e:
        logger.error(f"FlixBD create_series failed: {e}")
        raise


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
                body = resp.json()
                existing_id = body.get("errors", {}).get("existing_download_id")
                logger.info(f"FlixBD: duplicate link for movie {content_id} {quality} (existing id={existing_id})")
                if existing_id:
                    created_ids.append(existing_id)
                continue

            if resp.status_code in (400, 422):
                logger.warning(f"FlixBD: add download link error for movie {content_id} {quality}: {resp.text}")
                continue

            resp.raise_for_status()
            dl_id = resp.json()["data"]["id"]
            created_ids.append(dl_id)
            logger.info(f"FlixBD: added download link id={dl_id} movie={content_id} quality={quality}")

        except RuntimeError:
            raise
        except Exception as e:
            logger.error(f"FlixBD: failed to add link for movie {content_id} {quality}: {e}")

    return created_ids


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
            item_type = item.get("type", "")
            resolutions = item.get("resolutions", {})

            for quality, drive_url in resolutions.items():
                if not drive_url or not drive_url.startswith("https://drive.google.com"):
                    # Skip non-Drive links (not yet uploaded)
                    continue

                payload = {
                    "server_name": server_name,
                    "download_link": drive_url,
                    "quality": quality,
                    "season_number": str(season_num).zfill(2),  # "01", "02", etc.
                }

                # episode_number field (string, supports ranges):
                #   single_episode → extract from label ("Episode 05" → "05")
                #   partial_combo  → extract from label ("Episode 01-08" → "01-08")
                #   combo_pack     → omit (null = full-season pack)
                if item_type == "single_episode":
                    m = re.search(r"(\d+)", item_label)
                    if m:
                        payload["episode_number"] = str(int(m.group(1))).zfill(2)
                elif item_type == "partial_combo":
                    nums = re.findall(r"\d+", item_label)
                    if len(nums) >= 2:
                        payload["episode_number"] = f"{int(nums[0]):02d}-{int(nums[-1]):02d}"
                    elif len(nums) == 1:
                        payload["episode_number"] = str(int(nums[0])).zfill(2)
                # combo_pack: no episode_number — full-season pack

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
                        body = resp.json()
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
                    dl_id = resp.json()["data"]["id"]
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
    # LLM already formats website_movie_title as: 'Title Year WEB-DL Language - SiteName'
    display_title = movie_data.get("website_movie_title") or movie_data.get("title", "Unknown")

    payload: dict = {
        "title": display_title,
        "status": "published",
    }

    _set_if(payload, "description", movie_data.get("plot"))
    _set_if(payload, "poster", movie_data.get("poster_url"))
    _set_if(payload, "rating", movie_data.get("rating"))
    _set_if(payload, "director", movie_data.get("director"))
    _set_if(payload, "cast", movie_data.get("cast"))
    _set_if(payload, "imdb_id", movie_data.get("imdb_id"))
    _set_if(payload, "tmdb_id", movie_data.get("tmdb_id"))
    _set_if(payload, "meta_title", movie_data.get("meta_title"))
    _set_if(payload, "meta_description", movie_data.get("meta_description"))
    _set_if(payload, "meta_keywords", movie_data.get("meta_keywords"))

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
    display_title = tvshow_data.get("website_tvshow_title") or tvshow_data.get("title", "Unknown")

    payload: dict = {
        "title": display_title,
        "status": "published",
    }

    _set_if(payload, "description", tvshow_data.get("plot"))
    _set_if(payload, "poster", tvshow_data.get("poster_url"))
    _set_if(payload, "rating", tvshow_data.get("rating"))
    _set_if(payload, "director", tvshow_data.get("director"))
    _set_if(payload, "cast_info", tvshow_data.get("cast_info"))
    _set_if(payload, "imdb_id", tvshow_data.get("imdb_id"))
    _set_if(payload, "tmdb_id", tvshow_data.get("tmdb_id"))
    _set_if(payload, "total_seasons", tvshow_data.get("total_seasons"))
    _set_if(payload, "meta_title", tvshow_data.get("meta_title"))
    _set_if(payload, "meta_description", tvshow_data.get("meta_description"))
    _set_if(payload, "meta_keywords", tvshow_data.get("meta_keywords"))

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
