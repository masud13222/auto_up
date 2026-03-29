import json
import logging
import os
from multiprocessing import current_process
from urllib.parse import urlparse

from django.conf import settings

from upload.models import MediaTask
from upload.service.info import get_content_info
from upload.service.duplicate_checker import (
    _search_db,
    coerce_matched_task_pk,
    site_row_id_from_duplicate_result,
)
from upload.utils.drive_file_delete import cleanup_old_drive_files
from upload.utils.tv_items import split_tv_replace_scope
from upload.utils.web_scrape import WebScrapeService, normalize_http_url
from llm.schema.blocked_names import SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY
from llm.utils.name_extractor import extract_title_info
from upload.task_locks import acquire_runtime_lock

from .helpers import normalize_result_download_languages, save_task
from .movie_pipeline import process_movie_pipeline
from .runtime_helpers import (
    build_db_candidate,
    build_db_match_candidates,
    clean_result_keep_drive_links,
    donor_result_for_site_content,
    fetch_flixbd_results,
    has_drive_links,
    merge_drive_links,
    merge_new_episodes,
    normalize_duplicate_response,
    refresh_site_sync_snapshot_from_api,
    result_strip_non_drive_download_links,
)
from .tvshow_pipeline import process_tvshow_pipeline

logger = logging.getLogger(__name__)
_TASK_LOCK_GRACE_SECONDS = 300


def _is_valid_task_url(url: str) -> bool:
    """Reject malformed task URLs early so the worker fails gracefully."""
    if not url or not isinstance(url, str):
        return False
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return False
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return False
    tail = url.split("://", 1)[1] if "://" in url else url
    if "http://" in tail or "https://" in tail:
        return False
    return True


def process_media_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    Combined flow (1 LLM call):
    1. Title fetch + DB search (no LLM)
    2. Full page scrape + LLM (extract + duplicate check in one call)
    3. Route to movie or tvshow pipeline
    """
    lock_ttl = int(settings.Q_CLUSTER.get("timeout", 7200)) + _TASK_LOCK_GRACE_SECONDS
    task_lock = acquire_runtime_lock(
        f"media-task-{task_pk}",
        stale_after_seconds=lock_ttl,
    )
    if task_lock is None:
        logger.warning(
            "process_media_task: duplicate worker claim skipped for MediaTask pk=%s",
            task_pk,
        )
        return json.dumps({"status": "skipped", "message": "Already running in another worker"})

    try:
        try:
            media_task = MediaTask.objects.get(pk=task_pk)
        except MediaTask.DoesNotExist:
            # Stale django-q job after duplicate-skip delete, admin delete, or re-queue race.
            logger.warning(
                "process_media_task: MediaTask pk=%s missing (row deleted); stale queue job — skipping",
                task_pk,
            )
            return json.dumps({"status": "skipped", "message": "MediaTask does not exist"})

        # Skip if already completed
        if media_task.status == 'completed':
            logger.info(f"Task already completed, skipping: {media_task.title or media_task.url[:50]} (pk={task_pk})")
            return json.dumps({"status": "skipped", "message": "Already completed"})

        save_task(media_task, status='processing')

        url = normalize_http_url((media_task.url or "").strip())
        if url != (media_task.url or "").strip():
            media_task.url = url
            media_task.save(update_fields=["url", "updated_at"])
            logger.info(f"Normalized task URL saved: {url}")

        if not _is_valid_task_url(url):
            msg = f"Invalid URL: {url}"
            logger.warning(msg)
            save_task(media_task, status='failed', error_message=msg)
            return json.dumps({"status": "error", "message": msg})

        logger.info(
            "Task started for URL: %s (pid=%s worker=%s)",
            url,
            os.getpid(),
            getattr(current_process(), "name", "main"),
        )

        # ── Step 0: Title fetch + DB search (no LLM call) ──
        website_title = WebScrapeService.cinefreak_title(url)
        db_match_candidates = None
        db_candidate_map = {}
        flixbd_results = []
        existing_task = None
        existing_result = {}
        resume_result_raw = clean_result_keep_drive_links(media_task.result or {})
        has_existing_drive = has_drive_links(resume_result_raw)
        resume_result = resume_result_raw if has_existing_drive else {}

        if website_title:
            logger.info(f"Website title: {website_title}")
            info = extract_title_info(website_title)
            name, year = info.title, info.year
            logger.info(f"Extracted: name='{name}', year='{year}'")

            if name:
                matches = _search_db(name, year, exclude_pk=media_task.pk)
                if matches:
                    db_match_candidates = build_db_match_candidates(matches)
                    db_candidate_map = {t.pk: t for t in matches}
                    logger.info(
                        f"Found {len(matches)} DB candidate(s): "
                        + ", ".join(f"[{t.pk}] {t.title}" for t in matches)
                    )
                elif resume_result:
                    logger.info(f"No other match, but task has existing result (reused task). Using self for dup check.")
                    db_match_candidates = [build_db_candidate(media_task)]
                    db_candidate_map = {media_task.pk: media_task}
                else:
                    logger.info(f"No existing match for '{name}'. New content.")

                # ── FlixBD search (pre-LLM, results passed to LLM as context) ──
                flixbd_results = fetch_flixbd_results(name)

        # ── Step 1: Full scrape + combined LLM call (extract + dup check) ──
        def _on_progress(data):
            title = data.get("title", "")
            if title and not media_task.title:
                save_task(media_task, title=title, result=data)
                logger.info(f"Saved title: {title}")
            else:
                save_task(media_task, result=data)

        content_type, data, dup_result = get_content_info(
            url,
            on_progress=_on_progress,
            db_match_candidates=db_match_candidates,
            flixbd_results=flixbd_results if flixbd_results else None,
            existing_result=resume_result if resume_result else None,
        )
        title = data.get("title", "Unknown")

        # ── Normalize duplicate_check (split MediaTask pk vs FlixBD site id) ──
        _flix_ctx = flixbd_results if flixbd_results else []
        if dup_result:
            normalize_duplicate_response(
                dup_result, db_candidate_map, _flix_ctx, media_task.pk
            )

        target_site_row_id = site_row_id_from_duplicate_result(dup_result) if dup_result else None

        # ── Resolve existing_task from matched_task_id (DB only) ──
        existing_task = None
        if dup_result:
            matched_pk = coerce_matched_task_pk(dup_result.get("matched_task_id"))
            if matched_pk is not None:
                existing_task = db_candidate_map.get(matched_pk)
                if existing_task:
                    logger.info(
                        "LLM matched DB candidate: [%s] %s",
                        existing_task.pk,
                        existing_task.title,
                    )
                else:
                    dup_result["matched_task_id"] = None
                    logger.warning(
                        "matched_task_id=%s not in DB candidates %s (task pk=%s)",
                        matched_pk,
                        list(db_candidate_map.keys()),
                        media_task.pk,
                    )
            else:
                logger.info("matched_task_id=null — no DB row targeted for merge")

        # ── Merge resume drive links (restart recovery) ──
        if resume_result and not existing_task:
            data = merge_drive_links(resume_result, data)
            logger.info("Checked for drive links from previous partial upload (resume)")

        # ── Handle duplicate result ──
        action = "process"
        replace_scope_data = None
        if dup_result:
            action = dup_result.get("action", "process")
            reason = dup_result.get("reason", "LLM decision")

            # Validate action
            if action not in ("skip", "update", "replace", "replace_items", "process"):
                action = "process"
                reason = f"Invalid LLM action, defaulting to process: {dup_result}"

            logger.info(f"Duplicate check result: action={action}, reason={reason}")

            # update/replace without DB merge: require LLM target site row id for site-targeted partial flows
            if action in ("update", "replace", "replace_items") and not existing_task and target_site_row_id is None:
                logger.warning(
                    "PipelineWarning: duplicate action=%s but no MediaTask match and no %s — "
                    "full process (pk=%s).",
                    action,
                    TARGET_SITE_ROW_ID_JSON_KEY,
                    media_task.pk,
                )
                action = "process"
                dup_result["action"] = "process"
                dup_result["missing_resolutions"] = []
            elif (
                action == "skip"
                and not resume_result
                and not existing_task
                and target_site_row_id is None
            ):
                logger.warning(
                    "Duplicate skip without %s — forcing process (pk=%s)",
                    TARGET_SITE_ROW_ID_JSON_KEY,
                    media_task.pk,
                )
                action = "process"
                dup_result["action"] = "process"

            if dup_result and action == "skip":
                if resume_result:
                    # Reused task — restore to completed with the merged data (preserving Drive links)
                    logger.info(f"DUPLICATE SKIP: {reason} — restoring reused task to completed (pk={media_task.pk})")
                    save_task(media_task, status='completed', result=data)
                else:
                    if not existing_task:
                        sid = target_site_row_id
                        if sid is not None:
                            web_title = data.get("website_movie_title") or data.get("website_tvshow_title") or ""
                            result_skip = result_strip_non_drive_download_links(data)
                            result_skip = {
                                **result_skip,
                                "skipped_without_upload": True,
                                "skipped_duplicate_source": "flixbd",
                                "flixbd_site_content_id": sid,
                            }
                            save_task(
                                media_task,
                                status="completed",
                                content_type=content_type,
                                title=title,
                                website_title=web_title,
                                result=result_skip,
                                error_message="",
                                site_content_id=sid,
                            )
                            logger.info(
                                "DUPLICATE SKIP: %s — saved %s site id=%s to DB (pk=%s)",
                                reason,
                                SITE_NAME,
                                sid,
                                media_task.pk,
                            )
                        else:
                            logger.info(
                                "DUPLICATE SKIP: %s — deleting task (no %s) (pk=%s)",
                                reason,
                                TARGET_SITE_ROW_ID_JSON_KEY,
                                media_task.pk,
                            )
                            media_task.delete()
                    else:
                        logger.info(f"DUPLICATE SKIP: {reason} — deleting task (pk={media_task.pk})")
                        media_task.delete()
                return json.dumps({"status": "skipped", "message": reason})

            if action in ("update", "replace", "replace_items") and existing_task:
                logger.info(f"DUPLICATE {action.upper()}: {reason} — using existing task [{existing_task.pk}], deleting new entry (pk={media_task.pk})")
                existing_result = existing_task.result or {}
                if action == "replace_items" and content_type == "tvshow":
                    replace_scope_data = json.loads(json.dumps(data, ensure_ascii=False))

                # Register this new URL in the existing task's extra_urls
                new_url = url
                if existing_task.add_extra_url(new_url):
                    existing_task.save(update_fields=['extra_urls', 'updated_at'])
                    logger.info(f"Registered new source URL in existing task extra_urls: {new_url}")

                # Replace: clean up old Drive files before re-downloading
                if action == "replace" and existing_result:
                    logger.info(f"Cleaning up old Drive files for replace action...")
                    cleanup_old_drive_files(existing_result)
                elif action == "replace_items" and existing_result and content_type == "tvshow":
                    delete_result, keep_result, requires_full_replace = split_tv_replace_scope(
                        existing_result, data
                    )
                    if requires_full_replace:
                        logger.warning(
                            "replace_items overlaps an existing combo pack; escalating to full replace "
                            "(task pk=%s)",
                            existing_task.pk,
                        )
                        action = "replace"
                        dup_result["action"] = "replace"
                        replace_scope_data = None
                        cleanup_old_drive_files(existing_result)
                        existing_result = {}
                    else:
                        if delete_result.get("seasons"):
                            logger.info("Cleaning up overlapping Drive files for replace_items action...")
                            cleanup_old_drive_files(delete_result)
                        existing_result = keep_result

                media_task.delete()
                media_task = existing_task
                media_task.status = 'processing'
                media_task.error_message = ''
                media_task.save(update_fields=['status', 'error_message', 'updated_at'])

        # LLM target site row id → site_content_id for update/replace only (not plain process)
        if (
            target_site_row_id is not None
            and not media_task.site_content_id
            and action in ("update", "replace", "replace_items")
        ):
            media_task.site_content_id = target_site_row_id
            save_task(media_task, site_content_id=target_site_row_id)
            logger.info(
                "LLM %s site_content_id=%s for '%s' (pk=%s)",
                SITE_NAME,
                target_site_row_id,
                title,
                media_task.pk,
            )

        if action in ("update", "replace", "replace_items") and media_task.site_content_id:
            site_snapshot_result = refresh_site_sync_snapshot_from_api(media_task, content_type)
            if site_snapshot_result:
                existing_result = site_snapshot_result

        # ── Merge existing data for update (DB row and/or donor / API by site id) ──
        if action in ("update", "replace_items") and not existing_result and target_site_row_id:
            existing_result = donor_result_for_site_content(
                target_site_row_id, media_task.pk, content_type
            )
            if not existing_result:
                if action == "replace_items":
                    logger.warning(
                        "%s=%s: replace_items has no donor MediaTask or API snapshot — "
                        "escalating to full replace for consistency (pk=%s)",
                        TARGET_SITE_ROW_ID_JSON_KEY,
                        target_site_row_id,
                        media_task.pk,
                    )
                    action = "replace"
                    dup_result["action"] = "replace"
                    replace_scope_data = None
                else:
                    mr = dup_result.get("missing_resolutions") if dup_result else None
                    if (
                        action == "update"
                        and content_type == "movie"
                        and isinstance(mr, list)
                        and mr
                    ):
                        logger.warning(
                            "%s=%s: no donor MediaTask and no API drive map — "
                            "cannot hydrate existing movie qualities, but will still process only "
                            "LLM missing_resolutions=%s (pk=%s)",
                            TARGET_SITE_ROW_ID_JSON_KEY,
                            target_site_row_id,
                            mr,
                            media_task.pk,
                        )
                    else:
                        logger.warning(
                            "%s=%s: no donor MediaTask and no API drive map — "
                            "cannot hydrate existing qualities; running full downloads (pk=%s)",
                            TARGET_SITE_ROW_ID_JSON_KEY,
                            target_site_row_id,
                            media_task.pk,
                        )
                        if dup_result and isinstance(dup_result.get("missing_resolutions"), list):
                            dup_result["missing_resolutions"] = []

        if action == "replace_items" and content_type == "tvshow" and existing_result:
            if replace_scope_data is None:
                replace_scope_data = json.loads(json.dumps(data, ensure_ascii=False))
            delete_result, keep_result, requires_full_replace = split_tv_replace_scope(
                existing_result, data
            )
            if requires_full_replace:
                logger.warning(
                    "replace_items is unsafe because a combo pack is involved; escalating to full replace "
                    "(task pk=%s)",
                    media_task.pk,
                )
                action = "replace"
                dup_result["action"] = "replace"
                replace_scope_data = None
                cleanup_old_drive_files(existing_result)
                existing_result = {}
            else:
                if delete_result.get("seasons"):
                    logger.info("Cleaning up overlapping Drive files for site/db replace_items action...")
                    cleanup_old_drive_files(delete_result)
                existing_result = keep_result

        if action in ("update", "replace_items") and existing_result:
            is_tvshow = content_type == "tvshow" or bool(existing_result.get("seasons"))
            has_new_eps = dup_result.get("has_new_episodes", False) if dup_result else False

            if action == "update" and is_tvshow and has_new_eps:
                data = merge_new_episodes(existing_result, data)
                logger.info("Merged new episodes into existing TV show seasons")
            elif action == "replace_items" and is_tvshow:
                data = merge_new_episodes(existing_result, data)
                logger.info("Merged preserved TV items with incoming replace_items scope")
            else:
                data = merge_drive_links(existing_result, data)
                logger.info("Merged existing drive links into new extraction data")

            from upload.service.info import resolve_movie_links, resolve_tvshow_links

            if content_type == "movie":
                data = resolve_movie_links(data, existing_result=existing_result)
            else:
                data = resolve_tvshow_links(
                    data, on_item_resolved=None, existing_result=existing_result
                )

        if dup_result:
            updated_title = dup_result.get("updated_title")
            if isinstance(updated_title, str) and updated_title.strip():
                if content_type == "tvshow":
                    data["website_tvshow_title"] = updated_title.strip()
                else:
                    data["website_movie_title"] = updated_title.strip()

        data = normalize_result_download_languages(data)

        from upload.service.flixbd_api_content import movie_website_title, series_website_title

        web_title = (
            series_website_title(data)
            if content_type == "tvshow"
            else movie_website_title(data)
        )
        save_task(media_task, content_type=content_type, title=title, website_title=web_title, result=data)
        logger.info(f"Detected content type: {content_type} — Title: {title}")

        # ── Step 2: Route to appropriate pipeline ──
        dup_info = {
            "action": action,
            "existing_task": existing_task if action != "process" and existing_task is not None else None,
            "clear_flixbd_links": action == "replace",
            "clear_flixbd_scope": replace_scope_data if action == "replace_items" else None,
        }
        if dup_result:
            mr = dup_result.get("missing_resolutions")
            if isinstance(mr, list) and mr:
                dup_info["missing_resolutions"] = mr

        if content_type == "tvshow":
            return process_tvshow_pipeline(media_task, data, dup_info=dup_info)
        else:
            return process_movie_pipeline(media_task, data, dup_info=dup_info)

    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        # Clean result: only keep items that have Drive links (remove unprocessed scrape data)
        cleaned = clean_result_keep_drive_links(media_task.result)
        save_task(media_task, status='failed', error_message=str(e), result=cleaned)
        return json.dumps({"status": "error", "message": str(e)})
    finally:
        task_lock.release()


# Backward compatibility: old queued tasks still reference this name
process_movie_task = process_media_task
