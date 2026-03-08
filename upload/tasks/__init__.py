import json
import logging

from upload.models import MediaTask
from upload.service.info import get_content_info

from .helpers import save_task
from .movie_pipeline import process_movie_pipeline
from .tvshow_pipeline import process_tvshow_pipeline

logger = logging.getLogger(__name__)


def process_media_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    Auto-detects whether content is a Movie or TV Show and routes accordingly.
    Saves progress to DB at every step for crash recovery.
    """
    media_task = MediaTask.objects.get(pk=task_pk)

    # Skip if already completed (prevents duplicate processing from stale queue entries)
    if media_task.status == 'completed':
        logger.info(f"Task already completed, skipping: {media_task.title or media_task.url[:50]} (pk={task_pk})")
        return json.dumps({"status": "skipped", "message": "Already completed"})

    save_task(media_task, status='processing')

    try:
        url = media_task.url
        logger.info(f"Task started for URL: {url}")

        def _on_progress(data):
            title = data.get("title", "")
            if title and not media_task.title:
                save_task(media_task, title=title, result=data)
                logger.info(f"Saved title: {title}")
            else:
                save_task(media_task, result=data)

        # Step 1: Auto-detect content type and extract info
        content_type, data = get_content_info(url, on_progress=_on_progress)
        title = data.get("title", "Unknown")

        # Save: fully resolved data
        save_task(media_task, title=title, result=data)
        logger.info(f"Detected content type: {content_type} — Title: {title}")

        # Step 2: Route to appropriate pipeline
        if content_type == "tvshow":
            return process_tvshow_pipeline(media_task, data)
        else:
            return process_movie_pipeline(media_task, data)

    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        save_task(media_task, status='failed', error_message=str(e))
        return json.dumps({"status": "error", "message": str(e)})


# Backward compatibility: old queued tasks still reference this name
process_movie_task = process_media_task
