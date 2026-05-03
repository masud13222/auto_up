import json
import logging
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

from django.conf import settings

from screenshot.services.capture import capture_screenshots_for_publish
from settings.models import UploadSettings
from upload.service.downloader import Downloader
from upload.service.uploader import DriveUploader
from upload.utils.subtitle_remove import process_downloaded_files

from .media_entry_bridge import (
    coerce_download_source_value,
    coerce_entry_language_value,
    download_source_urls,
    is_drive_link,
    movie_download_entry_key,
    save_task,
    validate_llm_download_basename,
)

logger = logging.getLogger(__name__)
_RESOLUTION_KEY_RE = re.compile(r"^(?:\d{3,4}p|4k)$", re.I)


def _normalized_resolution_key(value) -> str:
    text = str(value or "").strip().lower()
    if not _RESOLUTION_KEY_RE.fullmatch(text):
        raise ValueError(f"Invalid movie resolution key: {value!r}")
    return text


def _movie_entry_label(resolution: str, entry: dict) -> str:
    return f"{resolution} [{entry['l']}]"


def _movie_download_entries_from_llm(movie_data: dict) -> tuple[dict, list[str]]:
    """Normalize download_links; return (movie_data, validation issue strings)."""
    download_links = movie_data.get("download_links") or {}
    if not isinstance(download_links, dict):
        raise ValueError("Movie `download_links` must be a JSON object")
    out = {}
    issues: list[str] = []
    for raw_resolution, raw_entries in download_links.items():
        ctx = f"Movie download_links[{raw_resolution!r}]"
        try:
            resolution = _normalized_resolution_key(raw_resolution)
        except ValueError as e:
            issues.append(str(e))
            continue
        if not isinstance(raw_entries, list) or not raw_entries:
            issues.append(f"{ctx}: expected a non-empty array of file objects")
            continue
        entries = []
        for idx, raw_entry in enumerate(raw_entries):
            entry_ctx = f"{ctx}[{idx}]"
            if not isinstance(raw_entry, dict):
                issues.append(f"{entry_ctx}: expected object")
                continue
            source_urls = download_source_urls(raw_entry.get("u"))
            if not source_urls:
                issues.append(f"{entry_ctx}.u: missing or invalid")
                continue
            language = coerce_entry_language_value(raw_entry.get("l"))
            if not language:
                issues.append(f"{entry_ctx}.l: missing or invalid")
                continue
            filename_raw = raw_entry.get("f")
            if not isinstance(filename_raw, str) or not filename_raw.strip():
                issues.append(f"{entry_ctx}.f: missing or invalid")
                continue
            try:
                safe_filename = validate_llm_download_basename(
                    filename_raw, context=f"{entry_ctx}.f"
                )
            except ValueError as e:
                issues.append(str(e))
                continue
            entry = {
                "u": coerce_download_source_value(source_urls),
                "l": language,
                "f": safe_filename,
            }
            if isinstance(raw_entry.get("s"), str) and raw_entry["s"].strip():
                entry["s"] = raw_entry["s"].strip()
            entries.append(entry)
        if entries:
            out[resolution] = entries
    movie_data["download_links"] = out
    movie_data.pop("download_filenames", None)
    return out, issues


def process_movie_pipeline(media_task, movie_data, dup_info=None):
    """Validate sources, parallel download/clean/upload, optional screenshots, FlixBD publish."""
    title = movie_data.get("title", "Unknown")

    download_links = movie_data.get("download_links", {})
    if not download_links:
        logger.warning(f"No download links found for {title}")
        save_task(media_task, status='failed', error_message='No download links found', result=movie_data)
        return json.dumps({"status": "error", "message": "No download links found"})

    save_task(media_task, result=movie_data)
    logger.info(f"Saved LLM extraction result for: {title}")

    is_dup_update = bool(dup_info and dup_info.get("action") == "update")
    if not is_dup_update:
        movie_data.pop("screen_shots_url", None)

    logger.info(f"Validating download links for movie: {title}")
    try:
        download_links, validation_issues = _movie_download_entries_from_llm(movie_data)
    except ValueError as e:
        msg = str(e)
        logger.error("Movie pipeline: %s", msg)
        save_task(media_task, status="failed", error_message=msg, result=movie_data)
        return json.dumps({"status": "error", "message": msg})

    service = DriveUploader._get_drive_service()
    upload_settings = UploadSettings.objects.first()
    if not upload_settings:
        raise Exception("UploadSettings not configured.")

    year = movie_data.get("year", "")
    folder_name = DriveUploader.build_root_folder_name(title, year, "movie")
    movie_folder_id = DriveUploader._get_or_create_folder(
        service, folder_name, upload_settings.upload_folder_id
    )

    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in title).strip()
    drive_links: dict[str, list[dict]] = {}
    file_sizes = {}

    missing_only: set[str] | None = None
    if dup_info and dup_info.get("action") == "update":
        mr = dup_info.get("missing_resolutions")
        if isinstance(mr, list) and mr:
            missing_only = {str(x).strip().lower() for x in mr if x is not None and str(x).strip()}
            logger.info(
                "Duplicate update: only resolutions %s (others already on site)",
                sorted(missing_only),
            )

    def _download_and_clean(item):
        from upload.service.flixbd_client import format_file_size
        resolution = item["resolution"]
        entry = item["entry"]
        label = _movie_entry_label(resolution, entry)
        url_list = download_source_urls(entry.get("u"))

        file_path = None
        for url in url_list:
            file_path = Downloader.download(url, entry["f"], sub_folder=safe_title)
            if file_path:
                break

        if not file_path:
            logger.warning(f"Could not download {label}")
            return item, None, None

        try:
            raw_size = os.path.getsize(file_path)
            size_str = format_file_size(raw_size)
            logger.debug(f"File size for {label}: {size_str}")
        except OSError:
            size_str = None

        logger.info(f"Cleaning subtitles for {label}")
        cleaned = process_downloaded_files({label: file_path})
        file_path = cleaned.get(label, file_path)
        return item, file_path, size_str

    def _upload_and_delete(item, file_path):
        if not file_path or not os.path.exists(file_path):
            return item, None
        logger.info("Uploading %s to Drive", _movie_entry_label(item["resolution"], item["entry"]))
        try:
            link = DriveUploader.upload_file_with_retry(
                file_path,
                movie_folder_id,
                upload_name=item["entry"]["f"],
            )
        except Exception as e:
            logger.error("Upload failed for %s: %s", _movie_entry_label(item["resolution"], item["entry"]), e)
            link = None
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Removed local file: {file_path}")
        return item, link

    to_process: list[dict] = []
    uploaded_entry_ids: set[tuple[str, str, str, str]] = set()
    fresh_upload_entry_ids: set[tuple[str, str, str, str]] = set()
    for resolution, entries in download_links.items():
        kept_uploaded = []
        for entry in entries:
            entry_id = movie_download_entry_key(resolution, entry)
            if is_drive_link(entry["u"]):
                logger.info("Skipping %s: already uploaded to Drive", _movie_entry_label(resolution, entry))
                kept_uploaded.append(dict(entry))
                uploaded_entry_ids.add(entry_id)
                continue
            if missing_only is not None and resolution.lower() not in missing_only:
                logger.info(
                    "Skipping %s: duplicate update, not in missing_resolutions",
                    _movie_entry_label(resolution, entry),
                )
                continue
            to_process.append({"resolution": resolution, "entry": dict(entry)})
        if kept_uploaded:
            drive_links[resolution] = kept_uploaded

    if not to_process:
        if drive_links:
            movie_data["download_links"] = drive_links
            if validation_issues:
                movie_data["partial_upload"] = True
                movie_data["partial_upload_missing_resolutions"] = validation_issues
                msg = (
                    "Partial upload: some qualities were skipped due to invalid or missing source data: "
                    + ", ".join(validation_issues)
                )
                save_task(media_task, status='partial', result=movie_data, error_message=msg)
                logger.warning(msg)
            else:
                movie_data.pop("partial_upload", None)
                movie_data.pop("partial_upload_missing_resolutions", None)
                save_task(media_task, status='completed', result=movie_data, error_message='')
                logger.info(f"Movie already fully uploaded: {title}")
            flixbd_partial = _publish_to_flixbd_movie(
                media_task,
                movie_data,
                drive_links,
                file_sizes,
                dup_info=dup_info,
            )
            if flixbd_partial:
                prev = (media_task.error_message or "").strip()
                extra = (
                    "FlixBD: one or more download link POSTs failed; "
                    "saved download_links merged from API listing where possible."
                )
                save_task(
                    media_task,
                    status="partial",
                    error_message=f"{prev}; {extra}" if prev else extra,
                    result=movie_data,
                )
            if validation_issues or flixbd_partial:
                return json.dumps({"status": "partial", "type": "movie", "data": movie_data})
            return json.dumps({"status": "success", "type": "movie", "data": movie_data})

        logger.warning(f"No valid download links found for: {title}")
        msg = validation_issues[0] if validation_issues else 'No valid download links found (after link resolution)'
        save_task(media_task, status='failed', error_message=msg, result=movie_data)
        return json.dumps({"status": "error", "message": msg})

    logger.info(
        "Starting parallel download+clean: %s",
        [_movie_entry_label(item["resolution"], item["entry"]) for item in to_process],
    )
    with ThreadPoolExecutor(max_workers=min(3, len(to_process))) as executor:
        futures = {
            executor.submit(_download_and_clean, item): _movie_entry_label(item["resolution"], item["entry"])
            for item in to_process
        }
        results = []
        for future in as_completed(futures):
            results.append(future.result())

    paths_ok = [(item, p, s) for item, p, s in results if p]
    if paths_ok and not is_dup_update:
        _, best_path, _ = max(paths_ok, key=lambda x: os.path.getsize(x[1]))
        ss_urls = capture_screenshots_for_publish(best_path, f"{safe_title}-ss")
        if ss_urls:
            movie_data["screen_shots_url"] = ss_urls
            save_task(media_task, result=movie_data)
            logger.info(f"Set {len(ss_urls)} screenshot URL(s) from largest local file")
        else:
            logger.warning(
                "No screen_shots_url for %s — keyframes failed or screenshot settings incomplete.",
                title,
            )
    elif paths_ok and is_dup_update:
        logger.info("Duplicate update: skipping screenshot capture")

    logger.info(
        "Starting parallel upload: %s",
        [_movie_entry_label(item["resolution"], item["entry"]) for item in to_process],
    )
    with ThreadPoolExecutor(max_workers=min(3, len(to_process))) as executor:
        futures = {
            executor.submit(_upload_and_delete, item, p): _movie_entry_label(item["resolution"], item["entry"])
            for item, p, s in results
            if p
        }

        for future in as_completed(futures):
            item, link = future.result()
            size_str = next((s for processed_item, p, s in results if processed_item == item), None)
            if link:
                resolution = item["resolution"]
                src_entry = dict(item["entry"])
                src_entry["u"] = link
                if size_str:
                    src_entry["s"] = size_str
                    file_sizes[movie_download_entry_key(resolution, src_entry)] = size_str
                drive_links.setdefault(resolution, []).append(src_entry)
                entry_id = movie_download_entry_key(resolution, src_entry)
                uploaded_entry_ids.add(entry_id)
                uploaded_entry_ids.add(movie_download_entry_key(resolution, item["entry"]))
                fresh_upload_entry_ids.add(entry_id)
                movie_data["download_links"] = drive_links
                save_task(media_task, result=movie_data)
                logger.info("Saved Drive link for %s", _movie_entry_label(resolution, src_entry))

    movie_dir = os.path.join(settings.DOWNLOADS_DIR, safe_title)
    if os.path.isdir(movie_dir) and not os.listdir(movie_dir):
        shutil.rmtree(movie_dir, ignore_errors=True)

    if drive_links:
        movie_data["download_links"] = drive_links

    if not drive_links:
        save_task(media_task, status='failed', error_message='No files could be downloaded or uploaded', result=movie_data)
        return json.dumps({"status": "error", "message": "Pipeline failed"})

    missing_targets = validation_issues + [
        _movie_entry_label(item["resolution"], item["entry"])
        for item in to_process
        if movie_download_entry_key(item["resolution"], item["entry"]) not in uploaded_entry_ids
    ]
    if missing_targets:
        movie_data["partial_upload"] = True
        movie_data["partial_upload_missing_resolutions"] = missing_targets
        msg = (
            "Partial upload: some qualities failed after all retries: "
            + ", ".join(missing_targets)
        )
        save_task(media_task, status='partial', result=movie_data, error_message=msg)
        logger.warning(msg)
    else:
        movie_data.pop("partial_upload", None)
        movie_data.pop("partial_upload_missing_resolutions", None)
        save_task(media_task, status='completed', result=movie_data, error_message='')
        logger.info(f"Movie pipeline complete for: {title}")

    flixbd_partial = _publish_to_flixbd_movie(
        media_task,
        movie_data,
        drive_links,
        file_sizes,
        dup_info=dup_info,
        publish_entry_ids=fresh_upload_entry_ids,
    )
    if flixbd_partial:
        prev = (media_task.error_message or "").strip()
        extra = (
            "FlixBD: one or more download link POSTs failed; "
            "saved download_links merged from API listing where possible."
        )
        save_task(
            media_task,
            status="partial",
            error_message=f"{prev}; {extra}" if prev else extra,
            result=movie_data,
        )

    if missing_targets or flixbd_partial:
        return json.dumps({"status": "partial", "type": "movie", "data": movie_data})
    return json.dumps({"status": "success", "type": "movie", "data": movie_data})


def _publish_to_flixbd_movie(
    media_task,
    movie_data,
    drive_links,
    file_sizes,
    dup_info=None,
    publish_entry_ids=None,
) -> bool:
    """
    POST Drive links to FlixBD. Duplicate update: links only; title PATCH only if
    dup_info['patch_flixbd_website_title']. Replace may full-update metadata and clear rows.
    Returns True if any publish step reported failures. Never raises.
    """
    from upload.service import flixbd_client as fx
    from upload.tasks.runtime_helpers import (
        save_publish_state_with_snapshot,
        strip_movie_download_entries_by_flixbd_failures,
    )

    title = movie_data.get("title", "Unknown")

    try:
        fx._get_config()
    except RuntimeError as e:
        logger.info(f"FlixBD publish skipped: {e}")
        return False

    try:
        if getattr(media_task, "pk", None):
            try:
                media_task.refresh_from_db(fields=["site_content_id"])
            except Exception:
                pass

        web_t = fx.movie_website_title(movie_data)
        cid = media_task.site_content_id
        logger.info(
            "FlixBD publish (movie): task_pk=%s site_content_id=%s website_movie_title=%r",
            getattr(media_task, "pk", None),
            cid,
            web_t[:120] + ("…" if len(web_t) > 120 else ""),
        )

        allowed_entry_ids = None
        if cid:
            logger.info(f"FlixBD: existing row id={cid} — update existing movie then add links")
            content_id = int(cid)
            if dup_info and dup_info.get("action") == "update":
                if dup_info.get("patch_flixbd_website_title"):
                    fx.patch_movie_title(content_id, movie_data)
                allowed_entry_ids = set(publish_entry_ids or ())
                logger.info(
                    "FlixBD: update — preserving rows for movie id=%s; POST only %s new entr(y/ies)",
                    content_id,
                    len(allowed_entry_ids),
                )
            elif dup_info and dup_info.get("clear_flixbd_links"):
                if not fx.update_movie(content_id, movie_data):
                    fx.patch_movie_title(content_id, movie_data)
                n = fx.clear_movie_download_links(content_id)
                logger.info(
                    "FlixBD: replace — cleared %s existing download row(s) for movie id=%s",
                    n,
                    content_id,
                )
            else:
                fx.patch_movie_title(content_id, movie_data)
                if publish_entry_ids:
                    allowed_entry_ids = set(publish_entry_ids)
        else:
            logger.warning(
                "FlixBD: no site_content_id on task pk=%s — POST create_movie (title will be set on new row only; "
                "existing FlixBD row will NOT get a title update)",
                getattr(media_task, "pk", None),
            )
            content_id = fx.create_movie(movie_data)

        post_stats = fx.add_movie_download_links(
            content_id=content_id,
            drive_links=drive_links,
            file_sizes=file_sizes,
            movie_data=movie_data,
            allowed_entry_ids=allowed_entry_ids,
        )
        failed = post_stats.get("failed") or []
        attempted = int(post_stats.get("attempted") or 0)
        succeeded = int(post_stats.get("succeeded") or 0)
        had_failures = bool(failed) or (attempted > 0 and succeeded < attempted)

        if had_failures and failed:
            strip_movie_download_entries_by_flixbd_failures(movie_data, failed)

        if had_failures:
            movie_data["flixbd_publish_partial"] = True
            movie_data["flixbd_publish_failures"] = failed

        web_t = fx.movie_website_title(movie_data)
        save_publish_state_with_snapshot(
            media_task,
            "movie",
            movie_data,
            website_title=web_t,
            site_content_id=content_id,
        )
        logger.info(f"FlixBD: movie done -- site_content_id={content_id} clean_title='{title}'")
        return had_failures

    except Exception as e:
        logger.error(f"FlixBD publish failed for movie '{title}': {e}", exc_info=True)
        return True
