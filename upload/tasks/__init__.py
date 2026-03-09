import json
import logging

from upload.models import MediaTask
from upload.service.info import get_content_info
from upload.service.duplicate_checker import check_duplicate

from .helpers import save_task, is_drive_link
from .movie_pipeline import process_movie_pipeline
from .tvshow_pipeline import process_tvshow_pipeline

logger = logging.getLogger(__name__)


def _merge_drive_links(old_result: dict, new_data: dict) -> dict:
    """
    Merge existing Drive links from old_result into new_data.
    When action=update, old_result has drive.google.com links for
    already-uploaded resolutions. new_data has fresh download URLs.
    We replace download URLs with drive links where they exist,
    so the pipeline's is_drive_link() check skips them.
    """
    # ── Movie: download_links ──
    old_dl = old_result.get("download_links", {})
    new_dl = new_data.get("download_links", {})
    if old_dl and new_dl:
        for res, link in old_dl.items():
            if is_drive_link(link) and res in new_dl:
                new_dl[res] = link
                logger.debug(f"Preserved existing drive link for {res}")
        new_data["download_links"] = new_dl

    # ── TV Show: seasons → download_items → resolutions ──
    old_seasons = {s.get("season_number"): s for s in old_result.get("seasons", [])}
    for new_season in new_data.get("seasons", []):
        snum = new_season.get("season_number")
        old_season = old_seasons.get(snum)
        if not old_season:
            continue

        # Build lookup: label → {resolution: link}
        old_items = {}
        for item in old_season.get("download_items", []):
            old_items[item.get("label", "")] = item.get("resolutions", {})

        for new_item in new_season.get("download_items", []):
            label = new_item.get("label", "")
            old_res = old_items.get(label, {})
            new_res = new_item.get("resolutions", {})

            for res, link in old_res.items():
                if is_drive_link(link) and res in new_res:
                    new_res[res] = link
                    logger.debug(f"Preserved existing drive link for S{snum} {label} {res}")

            new_item["resolutions"] = new_res

    return new_data


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

        existing_result = {}  # Will be populated if action=update
        if action in ("update", "replace"):
            existing_task = dup.get("existing_task")
            if existing_task:
                logger.info(f"DUPLICATE {action.upper()}: {reason} — using existing task [{existing_task.pk}], deleting new entry (pk={media_task.pk})")
                # Save existing result BEFORE overwriting (has drive links)
                existing_result = existing_task.result or {}
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

        # ── Merge existing drive links for update action ──
        # When action=update, the old result has drive.google.com links for
        # already-uploaded resolutions. The new extraction has fresh download
        # URLs for ALL resolutions. We merge old drive links into the new data
        # so the pipeline skips already-uploaded files.
        if action == "update" and existing_result:
            data = _merge_drive_links(existing_result, data)
            logger.info(f"Merged existing drive links into new extraction data")

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
