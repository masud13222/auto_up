"""Process startup routines (runs after Django is ready — DB access safe)."""

from .recovery import schedule_upload_startup_hooks

__all__ = ["schedule_upload_startup_hooks"]
