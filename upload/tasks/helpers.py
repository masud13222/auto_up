import ast
import logging
from urllib.parse import urlparse

from upload.utils.web_scrape_html import normalize_http_url

logger = logging.getLogger(__name__)
_EMPTY_DOWNLOAD_VALUES = {"", "none", "null", "nil", "[]", "()", "{}", "false"}


def save_task(media_task, **fields):
    """Update task fields and save to DB immediately (only changed fields)."""
    for key, value in fields.items():
        setattr(media_task, key, value)
    if fields:
        media_task.save(update_fields=list(fields.keys()) + ['updated_at'])
    else:
        media_task.save()


def is_drive_link(url):
    """Check if a URL is already a Google Drive link (successfully uploaded)."""
    return any("drive.google.com" in link for link in download_source_urls(url))


def _flatten_download_value(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in value:
            out.extend(_flatten_download_value(item))
        return out
    if not isinstance(value, str):
        return []

    s = value.strip()
    if not s or s.casefold() in _EMPTY_DOWNLOAD_VALUES:
        return []

    if s[0] in "[({" and s[-1] in "])}":
        try:
            parsed = ast.literal_eval(s)
        except (SyntaxError, ValueError):
            parsed = None
        if parsed is not None and parsed != value:
            return _flatten_download_value(parsed)

    return [s]


def download_source_urls(value) -> list[str]:
    """
    Normalize download sources into a de-duplicated list of absolute HTTP(S) URLs.

    Accepts:
    - a single URL string
    - a list/tuple of URL strings
    - stringified Python/JSON lists like "['https://a', 'https://b']"
    - empty-ish values like "None", "null", "", []
    """
    out: list[str] = []
    seen: set[str] = set()

    for raw in _flatten_download_value(value):
        s = raw.strip().strip('"').strip("'")
        if not s or s.casefold() in _EMPTY_DOWNLOAD_VALUES:
            continue
        s = normalize_http_url(s)
        try:
            parsed = urlparse(s)
        except Exception:
            continue
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def primary_download_source_url(value) -> str:
    """Best-effort primary download URL from a mixed/stringified source field."""
    urls = download_source_urls(value)
    return urls[0] if urls else ""


def coerce_download_source_value(value):
    """
    Canonical stored value for an entry `u` field.
    Returns "" | "https://..." | ["https://...", ...]
    """
    urls = download_source_urls(value)
    if not urls:
        return ""
    if len(urls) == 1:
        return urls[0]
    return urls


def _flatten_language_value(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in value:
            out.extend(_flatten_language_value(item))
        return out
    if not isinstance(value, str):
        return []

    s = value.strip()
    if not s or s.casefold() in _EMPTY_DOWNLOAD_VALUES:
        return []

    if s[0] in "[({" and s[-1] in "])}":
        try:
            parsed = ast.literal_eval(s)
        except (SyntaxError, ValueError):
            parsed = None
        if parsed is not None and parsed != value:
            return _flatten_language_value(parsed)

    return [s]


def coerce_entry_language_value(value) -> str:
    """
    Canonical language label for an entry `l` field.
    Returns a comma-separated string, e.g. "Hindi, English".
    """
    out: list[str] = []
    seen: set[str] = set()

    for raw in _flatten_language_value(value):
        s = " ".join(str(raw).strip().split())
        if not s:
            continue
        key = s.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)

    return ", ".join(out)


def entry_language_key(value) -> str:
    """Normalized lowercase key for matching entry languages."""
    return coerce_entry_language_value(value).casefold()


def normalize_result_download_languages(data: dict) -> dict:
    """Normalize every download entry language field in-place to a comma-separated string."""
    if not isinstance(data, dict):
        return data

    for entries in (data.get("download_links") or {}).values():
        for entry in entries if isinstance(entries, list) else []:
            if isinstance(entry, dict):
                entry["l"] = coerce_entry_language_value(entry.get("l"))

    for season in data.get("seasons", []):
        if not isinstance(season, dict):
            continue
        for item in season.get("download_items", []):
            if not isinstance(item, dict):
                continue
            for entries in (item.get("resolutions") or {}).values():
                for entry in entries if isinstance(entries, list) else []:
                    if isinstance(entry, dict):
                        entry["l"] = coerce_entry_language_value(entry.get("l"))

    return data


def validate_llm_download_basename(value, *, context: str) -> str:
    """
    Ensure LLM-supplied download basename is safe for local filesystem use.
    Reject path traversal / directory injection (validation gate for structured extract).
    """
    if not isinstance(value, str):
        raise ValueError(f"{context}: expected string basename, got {type(value).__name__}")
    s = value.strip()
    if not s:
        raise ValueError(f"{context}: empty basename")
    if any(c in s for c in "/\\"):
        raise ValueError(f"{context}: basename must not contain path separators (use filename only, no folders)")
    if s in (".", ".."):
        raise ValueError(f"{context}: invalid basename {s!r}")
    if "\x00" in s:
        raise ValueError(f"{context}: basename contains null byte")
    if ":" in s:
        raise ValueError(f"{context}: basename must not contain ':' (Windows / ADS)")
    return s


def get_memory_mb():
    """Get current process RSS memory usage in MB (Linux only)."""
    try:
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    # VmRSS:   123456 kB
                    kb = int(line.split()[1])
                    return round(kb / 1024, 1)
    except Exception:
        pass
    return 0.0


def log_memory(label=""):
    """Log current memory usage."""
    mb = get_memory_mb()
    if mb > 0:
        logger.info(f"[MEM] {label}: {mb} MB")
    return mb

