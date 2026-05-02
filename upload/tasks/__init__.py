"""
Django-Q task registration for ``upload``.

Worker implementation lives in :mod:`upload.tasks.process_media_worker` so this package stays a thin facade.
"""

from __future__ import annotations

from .process_media_worker import process_media_task, process_movie_task

__all__ = ["process_media_task", "process_movie_task"]
