import json
import logging

from upload.models import MediaTask
from upload.service.info import get_content_info
from upload.service.duplicate_checker import check_duplicate

from .helpers import save_task
from .movie_pipeline import process_movie_pipeline
from .tvshow_pipeline import process_tvshow_pipeline

logger = logging.getLogger(__name__)


def process_media_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    1. Duplicate check (title fetch + DB search + LLM compare)
    2. Auto-detect content type and extract data (single LLM call)
    3. Route to movie or tvshow pipeline
    """
    media_task = MediaTask.objects.get(pk=task_pk)

    # Skip if already completed
    if media_task.status == 'completed':
        logger.info(f"Task already completed, skipping: {media_task.title or media_task.url[:50]} (pk={task_pk})")
        return json.dumps({"status": "skipped", "message": "Already completed"})

    save_task(media_task, status='processing')

    try:
        url = media_task.url
        logger.info(f"Task started for URL: {url}")

        # ── Step 0: Duplicate Check ──
        dup = check_duplicate(url, current_task_pk=media_task.pk)
        action = dup["action"]
        reason = dup["reason"]

        if action == "skip":
            logger.info(f"DUPLICATE SKIP: {reason} — deleting new entry (pk={media_task.pk})")
            media_task.delete()
            return json.dumps({"status": "skipped", "message": reason})

        if action in ("update", "replace"):
            existing_task = dup.get("existing_task")
            if existing_task:
                logger.info(f"DUPLICATE {action.upper()}: {reason} — using existing task [{existing_task.pk}], deleting new entry (pk={media_task.pk})")
                media_task.delete()
                # Continue pipeline with existing task
                media_task = existing_task
                media_task.status = 'processing'
                media_task.error_message = ''
                media_task.save(update_fields=['status', 'error_message', 'updated_at'])

        # ── Step 1: Extract content info ──
        def _on_progress(data):
            title = data.get("title", "")
            if title and not media_task.title:
                save_task(media_task, title=title, result=data)
                logger.info(f"Saved title: {title}")
            else:
                save_task(media_task, result=data)

        content_type, data = get_content_info(url, on_progress=_on_progress)
        title = data.get("title", "Unknown")

        media_task.content_type = content_type
        # Extract website_title from result data
        web_title = data.get("website_movie_title") or data.get("website_tvshow_title") or ""
        save_task(media_task, title=title, website_title=web_title, result=data)
        logger.info(f"Detected content type: {content_type} — Title: {title}")

        # Step 2: Route to appropriate pipeline
        # Pass dup context so pipeline can handle partial downloads
        if content_type == "tvshow":
            return process_tvshow_pipeline(media_task, data, dup_info=dup)
        else:
            return process_movie_pipeline(media_task, data, dup_info=dup)

    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        save_task(media_task, status='failed', error_message=str(e))
        return json.dumps({"status": "error", "message": str(e)})


# Backward compatibility: old queued tasks still reference this name
process_movie_task = process_media_task
