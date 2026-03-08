import logging

logger = logging.getLogger(__name__)


def save_task(media_task, **fields):
    """Update task fields and save to DB immediately."""
    for key, value in fields.items():
        setattr(media_task, key, value)
    media_task.save()


def is_drive_link(url):
    """Check if a URL is already a Google Drive link (already uploaded)."""
    if not url or not isinstance(url, str):
        return False
    return 'drive.google.com' in url or url.startswith('UPLOAD_FAILED')
