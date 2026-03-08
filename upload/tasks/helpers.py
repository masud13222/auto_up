import logging

logger = logging.getLogger(__name__)


def save_task(media_task, **fields):
    """Update task fields and save to DB immediately."""
    for key, value in fields.items():
        setattr(media_task, key, value)
    media_task.save()


def is_drive_link(url):
    """Check if a URL is already a Google Drive link (successfully uploaded)."""
    if not url or not isinstance(url, str):
        return False
    return 'drive.google.com' in url


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

