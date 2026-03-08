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
    Updates MediaTask status at each step.
    Supports resume — skips already-uploaded files on re-queue.
    """
    media_task = MediaTask.objects.get(pk=task_pk)
    save_task(media_task, status='processing')

    try:
        url = media_task.url
        logger.info(f"Task started for URL: {url}")

        # Step 1: Auto-detect content type and extract info
        content_type, data = get_content_info(url)
        title = data.get("title", "Unknown")

        # Save: Title + initial extraction result
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
