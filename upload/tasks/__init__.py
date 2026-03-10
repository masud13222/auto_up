import json
import logging

from upload.models import MediaTask
from upload.service.info import get_content_info
from upload.service.duplicate_checker import _search_db, _get_existing_resolutions
from upload.utils.web_scrape import WebScrapeService
from upload.utils.drive_file_delete import cleanup_old_drive_files
from llm.utils.name_extractor import extract_title_info

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


def _build_db_match_info(existing_task: MediaTask) -> dict:
    """Build the DB match info dict to inject into the combined LLM prompt."""
    result_data = existing_task.result or {}
    existing_resolutions = _get_existing_resolutions(existing_task)

    is_tvshow = existing_task.content_type == 'tvshow' if existing_task.content_type else bool(result_data.get("seasons"))

    info = {
        "existing_title": existing_task.title,
        "existing_resolutions": existing_resolutions,
        "existing_type": "tvshow" if is_tvshow else "movie",
    }

    if is_tvshow:
        episodes = []
        for season in result_data.get("seasons", []):
            for item in season.get("download_items", []):
                label = item.get("label", "")
                res = item.get("resolutions", {})
                ep_res = sorted(k for k, v in res.items() if v)
                episodes.append(f"{label}: {','.join(ep_res)}")
        info["existing_episode_count"] = len(episodes)
        info["existing_episodes"] = episodes

    return info


def process_media_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    Combined flow (1 LLM call):
    1. Title fetch + DB search (no LLM)
    2. Full page scrape + LLM (extract + duplicate check in one call)
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

        # ── Step 0: Title fetch + DB search (no LLM call) ──
        website_title = WebScrapeService.cinefreak_title(url)
        db_match_info = None
        existing_task = None
        existing_result = {}
        resume_result = media_task.result or {}

        if website_title:
            logger.info(f"Website title: {website_title}")
            info = extract_title_info(website_title)
            name, year = info.title, info.year
            logger.info(f"Extracted: name='{name}', year='{year}'")

            if name:
                matches = _search_db(name, year, exclude_pk=media_task.pk)
                if matches:
                    existing_task = matches[0]
                    logger.info(f"Found existing match: [{existing_task.pk}] {existing_task.title}")
                    db_match_info = _build_db_match_info(existing_task)
                elif resume_result:
                    # Reused task: no OTHER match found, but this task has its own
                    # previous result — use it for LLM duplicate comparison
                    logger.info(f"No other match, but task has existing result (reused task). Using self for dup check.")
                    db_match_info = _build_db_match_info(media_task)
                else:
                    logger.info(f"No existing match for '{name}'. New content.")

        # ── Step 1: Full scrape + combined LLM call (extract + dup check) ──
        def _on_progress(data):
            title = data.get("title", "")
            if title and not media_task.title:
                save_task(media_task, title=title, result=data)
                logger.info(f"Saved title: {title}")
            else:
                save_task(media_task, result=data)

        content_type, data, dup_result = get_content_info(
            url, on_progress=_on_progress, db_match_info=db_match_info,
            existing_result=resume_result if resume_result else None,
        )
        title = data.get("title", "Unknown")

        # ── Merge resume drive links (restart recovery) ──
        # If this task had partial uploads before restart, preserve those drive links
        if resume_result and not existing_task:
            data = _merge_drive_links(resume_result, data)
            logger.info("Checked for drive links from previous partial upload (resume)")

        # ── Handle duplicate result ──
        action = "process"
        if dup_result:
            action = dup_result.get("action", "process")
            reason = dup_result.get("reason", "LLM decision")

            # Validate action
            if action not in ("skip", "update", "replace", "process"):
                action = "process"
                reason = f"Invalid LLM action, defaulting to process: {dup_result}"

            logger.info(f"Duplicate check result: action={action}, reason={reason}")

            if action == "skip":
                if resume_result:
                    # Reused task — restore to completed, don't delete!
                    logger.info(f"DUPLICATE SKIP: {reason} — restoring reused task to completed (pk={media_task.pk})")
                    save_task(media_task, status='completed')
                else:
                    # New task — safe to delete
                    logger.info(f"DUPLICATE SKIP: {reason} — deleting task (pk={media_task.pk})")
                    media_task.delete()
                return json.dumps({"status": "skipped", "message": reason})

            if action in ("update", "replace") and existing_task:
                logger.info(f"DUPLICATE {action.upper()}: {reason} — using existing task [{existing_task.pk}], deleting new entry (pk={media_task.pk})")
                existing_result = existing_task.result or {}

                # Replace: clean up old Drive files before re-downloading
                if action == "replace" and existing_result:
                    logger.info(f"Cleaning up old Drive files for replace action...")
                    cleanup_old_drive_files(existing_result)

                media_task.delete()
                media_task = existing_task
                media_task.status = 'processing'
                media_task.error_message = ''
                media_task.save(update_fields=['status', 'error_message', 'updated_at'])

        # ── Merge existing drive links for update action ──
        if action == "update" and existing_result:
            data = _merge_drive_links(existing_result, data)
            logger.info(f"Merged existing drive links into new extraction data")

        media_task.content_type = content_type
        web_title = data.get("website_movie_title") or data.get("website_tvshow_title") or ""
        save_task(media_task, title=title, website_title=web_title, result=data)
        logger.info(f"Detected content type: {content_type} — Title: {title}")

        # ── Step 2: Route to appropriate pipeline ──
        dup_info = {"action": action, "existing_task": existing_task if action != "process" else None}
        if content_type == "tvshow":
            return process_tvshow_pipeline(media_task, data, dup_info=dup_info)
        else:
            return process_movie_pipeline(media_task, data, dup_info=dup_info)

    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        save_task(media_task, status='failed', error_message=str(e))
        return json.dumps({"status": "error", "message": str(e)})


# Backward compatibility: old queued tasks still reference this name
process_movie_task = process_media_task
