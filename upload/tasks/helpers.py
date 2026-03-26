import logging

logger = logging.getLogger(__name__)


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
    if not url or not isinstance(url, str):
        return False
    return 'drive.google.com' in url


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

