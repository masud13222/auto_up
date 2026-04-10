import json
import logging
import time

import httpx

from llm.schema.blocked_names import SITE_NAME

logger = logging.getLogger(__name__)

# Shared httpx timeout (read bumped for slow API / Docker → avoids truncated empty reads)
_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=5.0)

_MAX_FLIXBD_SEARCH_GET_ATTEMPTS = 3
_RETRYABLE_FLIXBD_SEARCH_HTTP = frozenset({408, 429, 500, 502, 503, 504})

# FlixBD MySQL columns are often VARCHAR(255); longer LLM meta strings cause PDO errors.
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
        raise RuntimeError(
            f"{SITE_NAME} settings not configured. Add it in Admin → Settings → {SITE_NAME} Settings."
        )
    if not cfg.is_enabled:
        raise RuntimeError(f"{SITE_NAME} publishing is disabled in settings.")
    api_url = cfg.api_url.rstrip("/")
    return api_url, cfg.api_key


def _url(api_url: str, *parts: str | int) -> str:
    """
    Build an API endpoint URL that always ends with a trailing slash.
    Django REST Framework returns 301 for slash-less URLs which converts
    PUT/POST to GET — always include the slash to avoid the redirect.

    Example: _url(base, "api/v1/movies", 42, "downloads") ->
             "{base}/api/v1/movies/42/downloads/"
    """
    path = "/".join(str(p).strip("/") for p in parts)
    return f"{api_url}/{path}/"


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


def _sleep_flixbd_search_retry(attempt: int) -> None:
    time.sleep(min(2.0**attempt, 15.0))


def flixbd_search_response_dict(api_url: str, api_key: str, params: dict) -> dict | None:
    """
    GET /api/v1/search/ (trailing slash per DRF), with follow_redirects=True.

    Retries transient network errors and retryable HTTP status codes.
    Returns the parsed JSON **object** on HTTP 200 with a dict body; otherwise None.

    Used by upload.tasks.runtime_helpers and auto_up (same contract as OpenAPI path
    ``/api/v1/search/`` with query params ``q``, ``type``, ``per_page``, ``page``).
    """
    endpoint = _url(api_url, "api/v1/search")
    last_net: BaseException | None = None

    for attempt in range(_MAX_FLIXBD_SEARCH_GET_ATTEMPTS):
        try:
            with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
                resp = client.get(endpoint, params=params, headers=_headers(api_key))
        except (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ReadError,
            httpx.WriteError,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
        ) as e:
            last_net = e
            if attempt + 1 >= _MAX_FLIXBD_SEARCH_GET_ATTEMPTS:
                logger.warning(
                    "FlixBD search GET: network error after %s attempts: %s",
                    _MAX_FLIXBD_SEARCH_GET_ATTEMPTS,
                    e,
                )
                return None
            logger.debug(
                "FlixBD search GET: network error attempt %s/%s: %s",
                attempt + 1,
                _MAX_FLIXBD_SEARCH_GET_ATTEMPTS,
                e,
            )
            _sleep_flixbd_search_retry(attempt)
            continue

        sc = resp.status_code
        if sc == 200:
            raw = resp.text or ""
            if not raw.strip():
                logger.debug("FlixBD search GET: empty body (HTTP 200)")
                return None
            try:
                out = json.loads(raw)
            except json.JSONDecodeError as e:
                logger.warning("FlixBD search GET: invalid JSON (HTTP 200): %s", e)
                return None
            if not isinstance(out, dict):
                logger.warning(
                    "FlixBD search GET: expected JSON object, got %s",
                    type(out).__name__,
                )
                return None
            return out

        if sc in _RETRYABLE_FLIXBD_SEARCH_HTTP and attempt + 1 < _MAX_FLIXBD_SEARCH_GET_ATTEMPTS:
            logger.debug(
                "FlixBD search GET: HTTP %s attempt %s/%s — retrying",
                sc,
                attempt + 1,
                _MAX_FLIXBD_SEARCH_GET_ATTEMPTS,
            )
            _sleep_flixbd_search_retry(attempt)
            continue

        logger.debug("FlixBD search GET: HTTP %s (no retry)", sc)
        return None

    if last_net is not None:
        return None
    return None


def _safe_json(resp: httpx.Response, operation: str) -> dict:
    """
    Parse API JSON and raise a descriptive error on malformed responses.
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
