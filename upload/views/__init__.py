"""
HTTP endpoints owned by ``upload``.

Panel UI routes live under ``admin_panel``; here we keep enqueue APIs only.
"""

from .media_queue import process_media

__all__ = ["process_media"]
