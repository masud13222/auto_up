import json
import logging

import httpx

from llm.schema.blocked_names import SITE_NAME

logger = logging.getLogger(__name__)

# Shared httpx timeout (read bumped for slow API / Docker → avoids truncated empty reads)
_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=5.0)

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
