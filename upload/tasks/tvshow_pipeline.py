import os
import json
import shutil
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from upload.service.downloader import Downloader
from upload.service.uploader import DriveUploader
from upload.utils.subtitle_remove import process_downloaded_files
from screenshot.services.capture import capture_screenshots_for_publish
from django.conf import settings

from .helpers import (
    coerce_download_source_value,
    download_source_urls,
    is_drive_link,
    log_memory,
    save_task,
    validate_llm_download_basename,
)

logger = logging.getLogger(__name__)
_RESOLUTION_KEY_RE = re.compile(r"^(?:\d{3,4}p|4k)$", re.I)


def _tv_item_download_entries_from_llm(item: dict, season_num) -> tuple[dict, list[dict]]:
    """Keep valid TV entries and collect invalid-entry issues for partial uploads."""
    resolutions = item.get("resolutions") or {}
    if not isinstance(resolutions, dict):
        raise ValueError(
            f"S{season_num} {item.get('label')!r}: resolutions must be a JSON object"
        )
    item_label = item.get("label", "Unknown")
    out = {}
    issues: list[dict] = []
    for raw_resolution, raw_entries in resolutions.items():
        resolution = str(raw_resolution or "").strip().lower()
        if not _RESOLUTION_KEY_RE.fullmatch(resolution):
            issues.append({
                "season_number": season_num,
                "label": item_label,
                "quality": str(raw_resolution or "").strip() or "?",
                "language": "",
                "filename": "",
                "reason": "invalid resolution key",
            })
            continue
        ctx = f"S{season_num} {item_label!r} resolutions[{raw_resolution!r}]"
        if not isinstance(raw_entries, list) or not raw_entries:
            issues.append({
                "season_number": season_num,
                "label": item_label,
                "quality": resolution,
                "language": "",
                "filename": "",
                "reason": "expected a non-empty array of file objects",
            })
            continue
        entries = []
        for idx, raw_entry in enumerate(raw_entries):
            entry_ctx = f"{ctx}[{idx}]"
            if not isinstance(raw_entry, dict):
                issues.append({
                    "season_number": season_num,
                    "label": item_label,
                    "quality": resolution,
                    "language": "",
                    "filename": "",
                    "reason": f"{entry_ctx}: expected object",
                })
                continue
            language_raw = raw_entry.get("l")
            language = (
                " ".join(language_raw.strip().split())
                if isinstance(language_raw, str) and language_raw.strip()
                else ""
            )
            filename_raw = raw_entry.get("f")
            filename = filename_raw.strip() if isinstance(filename_raw, str) and filename_raw.strip() else ""
            source_urls = download_source_urls(raw_entry.get("u"))
            if not source_urls:
                issues.append({
                    "season_number": season_num,
                    "label": item_label,
                    "quality": resolution,
                    "language": language,
                    "filename": filename,
                    "reason": "missing or invalid URL",
                })
                continue
            if not language:
                issues.append({
                    "season_number": season_num,
                    "label": item_label,
                    "quality": resolution,
                    "language": "",
                    "filename": filename,
                    "reason": "missing or invalid language",
                })
                continue
            if not filename:
                issues.append({
                    "season_number": season_num,
                    "label": item_label,
                    "quality": resolution,
                    "language": language,
                    "filename": "",
                    "reason": "missing or invalid filename",
                })
                continue
            try:
                safe_filename = validate_llm_download_basename(
                    filename_raw, context=f"{entry_ctx}.f"
                )
            except ValueError as e:
                issues.append({
                    "season_number": season_num,
                    "label": item_label,
                    "quality": resolution,
                    "language": language,
                    "filename": filename,
                    "reason": str(e),
                })
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
    item["resolutions"] = out
    item.pop("download_filenames", None)
    return out, issues


def _tv_entry_label(resolution: str, entry: dict) -> str:
    return f"{resolution} [{entry['l']}]"


def _tv_entry_id(season_num, item_label, resolution: str, entry: dict) -> tuple[int, str, str, str, str]:
    return (season_num, item_label, resolution, entry["l"], entry["f"])


def _tv_missing_issue_text(issue: dict) -> str:
    text = f"S{issue['season_number']} {issue['label']} {issue['quality']}"
    if issue.get("language"):
        text += f" [{issue['language']}]"
    if issue.get("reason"):
        text += f" ({issue['reason']})"
    return text


def process_tvshow_pipeline(media_task, tvshow_data, dup_info=None):
    """
    TV Show pipeline: per-file `resolutions` entries from main LLM extract -> Download/Clean/Upload -> FlixBD -> Cleanup

    Parallelism strategy:
    - Items (combo/partial/single) processed SEQUENTIALLY
    - Downloads:  PARALLEL (all resolutions download at once)
    - FFmpeg:     SEQUENTIAL (one at a time, prevents OOM)
    - Uploads:    PARALLEL (runs in background alongside next ffmpeg)
    - Supports resume -- skips items/resolutions already uploaded to Drive

    File size is captured from local file after download (before deletion).
    """
    title = tvshow_data.get("title", "Unknown")

    # Step 1: Check if seasons have download items
    seasons = tvshow_data.get("seasons", [])
    if not seasons:
        logger.warning(f"No seasons/download data found for {title}")
        save_task(media_task, status='failed', error_message='No seasons or download links found', result=tvshow_data)
        return json.dumps({"status": "error", "message": "No seasons found"})

    has_download_items = any(
        item for s in seasons for item in s.get("download_items", [])
    )
    if not has_download_items:
        logger.warning(f"Seasons found but no download items for {title}")
        save_task(media_task, status='failed', error_message='No download items found in seasons', result=tvshow_data)
        return json.dumps({"status": "error", "message": "No download items found"})

    # Save: LLM extraction complete
    save_task(media_task, result=tvshow_data)
    logger.info(f"Saved LLM extraction result for TV show: {title}")

    is_dup_update = bool(dup_info and dup_info.get("action") in ("update", "replace_items"))
    if not is_dup_update:
        tvshow_data.pop("screen_shots_url", None)

    # Step 2: File metadata embedded in each resolutions entry
    logger.info(f"Validating resolution entries for TV show: {title}")

    # Step 3: Setup Drive
    service = DriveUploader._get_drive_service()

    from settings.models import UploadSettings
    upload_settings = UploadSettings.objects.first()
    if not upload_settings:
        raise Exception("UploadSettings not configured.")

    parent_folder_id = upload_settings.upload_folder_id
    year = tvshow_data.get("year", "")
    folder_name = f"{title} {year}" if year else title
    show_folder_id = DriveUploader._get_or_create_folder(service, folder_name, parent_folder_id)

    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in title).strip()

    # Pre-create season folders
    season_folders = {}
    for season in seasons:
        season_num = season.get("season_number")
        if season_num not in season_folders:
            season_folder_name = f"Season {season_num}"
            season_folders[season_num] = DriveUploader._get_or_create_folder(
                service, season_folder_name, show_folder_id
            )

    # Build list of all download items
    all_items = []
    validation_issues: list[dict] = []
    try:
        for season in seasons:
            season_num = season.get("season_number")
            for item in season.get("download_items", []):
                item_type = item.get("type")
                item_label = item.get("label", "Unknown")
                normalized_resolutions, item_issues = _tv_item_download_entries_from_llm(
                    item, season_num
                )
                validation_issues.extend(item_issues)

                if normalized_resolutions:
                    all_items.append({
                        "season_number": season_num,
                        "type": item_type,
                        "label": item_label,
                        "resolutions": normalized_resolutions,
                        "season_folder_id": season_folders.get(season_num),
                    })
    except ValueError as e:
        msg = str(e)
        logger.error("TV show pipeline: %s", msg)
        save_task(media_task, status="failed", error_message=msg, result=tvshow_data)
        return json.dumps({"status": "error", "message": msg})

    if not all_items:
        msg = (
            _tv_missing_issue_text(validation_issues[0])
            if validation_issues
            else "No valid TV download entries found"
        )
        logger.warning("TV show pipeline: %s", msg)
        save_task(media_task, status="failed", error_message=msg, result=tvshow_data)
        return json.dumps({"status": "error", "message": msg})

    # -- Helper functions --

    def _download_one(source_item, season_num, item_label):
        """Download only -- runs in PARALLEL thread."""
        from upload.service.flixbd_client import format_file_size
        resolution = source_item["resolution"]
        entry = source_item["entry"]
        label = _tv_entry_label(resolution, entry)
        log_memory(f"Before download {label}")
        url_list = download_source_urls(entry.get("u"))

        file_path = None
        for url in url_list:
            file_path = Downloader.download(url, entry["f"], sub_folder=safe_title)
            if file_path:
                break

        if not file_path:
            logger.warning(f"Could not download {label} for S{season_num} {item_label}")
            log_memory(f"After download {label}")
            return source_item, file_path, None

        # Capture file size BEFORE ffmpeg/deletion
        try:
            raw_size = os.path.getsize(file_path)
            size_str = format_file_size(raw_size)
        except OSError:
            size_str = None

        log_memory(f"After download {label}")
        return source_item, file_path, size_str

    def _clean_one(source_item, file_path):
        """FFmpeg subtitle clean -- runs SEQUENTIALLY in main thread."""
        label = _tv_entry_label(source_item["resolution"], source_item["entry"])
        log_memory(f"Before ffmpeg {label}")
        cleaned = process_downloaded_files({label: file_path})
        file_path = cleaned.get(label, file_path)
        log_memory(f"After ffmpeg {label}")
        return file_path

    def _upload_one(source_item, file_path, season_folder_id, season_num, item_label):
        """Upload to Drive with retries; delete local after final outcome."""
        label = _tv_entry_label(source_item["resolution"], source_item["entry"])
        log_memory(f"Before upload {label}")
        try:
            link = DriveUploader.upload_file_with_retry(
                file_path,
                season_folder_id,
                upload_name=source_item["entry"]["f"],
            )
            logger.info(f"Uploaded S{season_num} {item_label} {label}")
        except Exception as e:
            logger.error(f"Upload failed for S{season_num} {item_label} {label}: {e}")
            link = None

        # Delete local file after final upload outcome
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Removed local file: {file_path}")

        log_memory(f"After upload+delete {label}")
        return source_item, link

    # -- Step 4: Process items --
    # Items: SEQUENTIAL | Downloads: PARALLEL | FFmpeg: SEQUENTIAL | Uploads: PARALLEL
    uploaded_count = 0
    total_items = len(all_items)

    # Largest video seen so far (bytes) — refresh series screenshots from bigger files only
    tvshow_ss_best_size = 0

    # {(season_num, label, quality): "2.1 GB"} -- collected across all items
    file_sizes_map = {}
    expected_upload_targets = set()

    logger.info(f"Processing {total_items} TV show item(s)")
    log_memory("Pipeline start")

    for idx, item_info in enumerate(all_items, 1):
        season_num = item_info["season_number"]
        item_type = item_info["type"]
        item_label = item_info["label"]
        resolutions = item_info["resolutions"]
        season_folder_id = item_info["season_folder_id"]

        # Collect files to process (skip already-uploaded ones)
        to_process = []
        already_uploaded = {}
        for resolution, entries in resolutions.items():
            kept_uploaded = []
            for entry in entries:
                entry_id = _tv_entry_id(season_num, item_label, resolution, entry)
                expected_upload_targets.add(entry_id)
                if is_drive_link(entry["u"]):
                    logger.info(
                        "Skipping S%s %s %s: already uploaded",
                        season_num,
                        item_label,
                        _tv_entry_label(resolution, entry),
                    )
                    kept_uploaded.append(dict(entry))
                    if entry.get("size"):
                        file_sizes_map[entry_id] = entry["size"]
                    continue
                to_process.append({"resolution": resolution, "entry": dict(entry)})
            if kept_uploaded:
                already_uploaded[resolution] = kept_uploaded

        if not to_process:
            if already_uploaded:
                logger.info(f"Skipping S{season_num} {item_label}: already uploaded to Drive")
                uploaded_count += 1
            else:
                logger.warning(f"No downloadable resolutions for S{season_num} {item_label}")
            continue

        logger.info(
            f"[{idx}/{total_items}] Processing S{season_num} {item_label}: "
            f"{[_tv_entry_label(item['resolution'], item['entry']) for item in to_process]}"
        )

        # -- Phase 1: Download ALL resolutions PARALLEL --
        uploaded_resolutions = dict(already_uploaded)

        with ThreadPoolExecutor(max_workers=3) as upload_executor:
            upload_futures = []

            with ThreadPoolExecutor(max_workers=len(to_process)) as dl_executor:
                download_futures = {
                    dl_executor.submit(_download_one, item, season_num, item_label): item
                    for item in to_process
                }

                # -- Phase 2: As each download completes --
                #   FFmpeg: SEQUENTIAL (blocks in main thread)
                #   Upload: PARALLEL (fire to background thread)
                cleaned_batch = []
                for future in as_completed(download_futures):
                    try:
                        source_item, file_path, size_str = future.result()
                    except Exception as e:
                        source_item = download_futures[future]
                        logger.error(
                            "Download failed for S%s %s %s: %s",
                            season_num,
                            item_label,
                            _tv_entry_label(source_item["resolution"], source_item["entry"]),
                            e,
                        )
                        continue
                    if not file_path:
                        continue

                    file_path = _clean_one(source_item, file_path)
                    cleaned_batch.append((source_item, file_path, size_str))

                if cleaned_batch and not is_dup_update:
                    best_item, best_path, _ = max(
                        cleaned_batch, key=lambda x: os.path.getsize(x[1])
                    )
                    bsz = os.path.getsize(best_path)
                    if bsz > tvshow_ss_best_size:
                        tvshow_ss_best_size = bsz
                        ss_urls = capture_screenshots_for_publish(
                            best_path,
                            f"{safe_title}-S{season_num}-ss",
                        )
                        if ss_urls:
                            tvshow_data["screen_shots_url"] = ss_urls
                            save_task(media_task, result=tvshow_data)
                            logger.info(
                                "Updated series screenshots (%d URLs) from S%s %s %s (%s bytes)",
                                len(ss_urls),
                                season_num,
                                item_label,
                                _tv_entry_label(best_item["resolution"], best_item["entry"]),
                                bsz,
                            )
                        else:
                            logger.warning(
                                "No screen_shots_url from S%s %s — keyframes failed or "
                                "Telegram/Worker settings incomplete (see screenshot logs).",
                                season_num,
                                item_label,
                            )
                elif cleaned_batch and is_dup_update:
                    logger.debug(
                        "Duplicate update: skipping screenshot capture for S%s %s",
                        season_num,
                        item_label,
                    )

                for source_item, file_path, size_str in cleaned_batch:
                    uf = upload_executor.submit(
                        _upload_one, source_item, file_path,
                        season_folder_id, season_num, item_label
                    )
                    upload_futures.append((uf, source_item))

            # Wait for all background uploads to finish (with statement handles shutdown)
            for uf, source_item in upload_futures:
                try:
                    uploaded_item, link = uf.result()
                except Exception as e:
                    logger.error(f"Upload thread failed for S{season_num} {item_label}: {e}")
                    continue
                if link:
                    size_str = next(
                        (s for item, _path, s in cleaned_batch if item == uploaded_item),
                        None,
                    )
                    entry = dict(uploaded_item["entry"])
                    entry["u"] = link
                    if size_str:
                        entry["s"] = size_str
                        file_sizes_map[_tv_entry_id(season_num, item_label, uploaded_item["resolution"], entry)] = size_str
                    uploaded_resolutions.setdefault(uploaded_item["resolution"], []).append(entry)

        if uploaded_resolutions:
            uploaded_count += 1

            # Update tvshow_data with drive links
            for season in tvshow_data.get("seasons", []):
                if season.get("season_number") == season_num:
                    for item in season.get("download_items", []):
                        if item.get("type") == item_type and item.get("label") == item_label:
                            item["resolutions"] = uploaded_resolutions
                            item.pop("sizes", None)
                            break

            save_task(media_task, result=tvshow_data)
            logger.info(f"[{idx}/{total_items}] Saved: S{season_num} {item_label}")
            log_memory(f"After item {idx}/{total_items}")

    # Clean empty folder
    show_dir = os.path.join(settings.DOWNLOADS_DIR, safe_title)
    if os.path.isdir(show_dir) and not os.listdir(show_dir):
        shutil.rmtree(show_dir, ignore_errors=True)

    if not uploaded_count:
        save_task(media_task, status='failed', error_message='No files could be downloaded or uploaded', result=tvshow_data)
        return json.dumps({"status": "error", "message": "Pipeline failed"})

    uploaded_targets = set()
    for season in tvshow_data.get("seasons", []):
        snum = season.get("season_number")
        for item in season.get("download_items", []):
            label = item.get("label")
            for resolution, entries in (item.get("resolutions") or {}).items():
                for entry in entries:
                    if is_drive_link(entry.get("u")):
                        uploaded_targets.add(_tv_entry_id(snum, label, resolution, entry))
    missing = list(validation_issues) + [
        {
            "season_number": s,
            "label": label,
            "quality": q,
            "language": lang,
            "filename": filename,
            "reason": "",
        }
        for s, label, q, lang, filename in sorted(expected_upload_targets - uploaded_targets)
    ]

    # Final save
    if missing:
        tvshow_data["partial_upload"] = True
        tvshow_data["partial_upload_missing_items"] = missing
        msg = (
            "Partial upload: some TV items were missing, invalid, or failed after retries: "
            + ", ".join(_tv_missing_issue_text(issue) for issue in missing)
        )
        save_task(media_task, status='partial', result=tvshow_data, error_message=msg)
        logger.warning(msg)
    else:
        tvshow_data.pop("partial_upload", None)
        tvshow_data.pop("partial_upload_missing_items", None)
        save_task(media_task, status='completed', result=tvshow_data, error_message='')
        logger.info(f"TV Show pipeline complete for: {title}. Uploaded {uploaded_count}/{total_items} items.")

    log_memory("Pipeline complete")

    # Step 5: Publish to FlixBD
    _publish_to_flixbd_series(
        media_task, tvshow_data, file_sizes_map, dup_info=dup_info
    )

    if missing:
        return json.dumps({"status": "partial", "type": "tvshow", "data": tvshow_data})
    return json.dumps({"status": "success", "type": "tvshow", "data": tvshow_data})


def _publish_to_flixbd_series(media_task, tvshow_data, file_sizes_map, dup_info=None):
    """
    Add Drive links to FlixBD after upload completes.

    **Update path:** same as movie — ``site_content_id`` (LLM duplicate_check site row id or DB row)
    triggers ``patch_series_title`` with latest
    ``website_tvshow_title``. Without id, POST ``create_series`` only.

    Never raises -- errors are logged only.
    """
    from upload.service import flixbd_client as fx

    title = tvshow_data.get("title", "Unknown")

    try:
        fx._get_config()
    except RuntimeError as e:
        logger.info(f"FlixBD publish skipped: {e}")
        return

    try:
        if getattr(media_task, "pk", None):
            try:
                media_task.refresh_from_db(fields=["site_content_id"])
            except Exception:
                pass

        web_t = fx.series_website_title(tvshow_data)
        cid = media_task.site_content_id
        logger.info(
            "FlixBD publish (series): task_pk=%s site_content_id=%s website_tvshow_title=%r",
            getattr(media_task, "pk", None),
            cid,
            web_t[:120] + ("…" if len(web_t) > 120 else ""),
        )

        if cid:
            logger.info(f"FlixBD: existing series id={cid} — update existing series then add links")
            content_id = int(cid)
            if dup_info and dup_info.get("clear_flixbd_links"):
                if not fx.update_series(content_id, tvshow_data):
                    fx.patch_series_title(content_id, tvshow_data)
                n = fx.clear_series_download_links(content_id)
                logger.info(
                    "FlixBD: replace — cleared %s existing download row(s) for series id=%s",
                    n,
                    content_id,
                )
            elif dup_info and dup_info.get("clear_flixbd_scope"):
                n = fx.clear_series_download_links_for_scope(
                    content_id,
                    dup_info["clear_flixbd_scope"].get("seasons", []),
                )
                logger.info(
                    "FlixBD: replace_items — cleared %s overlapping download row(s) for series id=%s",
                    n,
                    content_id,
                )
                fx.patch_series_title(content_id, tvshow_data)
            else:
                fx.patch_series_title(content_id, tvshow_data)
        else:
            logger.warning(
                "FlixBD: no site_content_id on task pk=%s — POST create_series (existing row title not updated)",
                getattr(media_task, "pk", None),
            )
            content_id = fx.create_series(tvshow_data)
            if not content_id:
                logger.error(f"FlixBD: create_series returned None for '{title}' — skipping publish")
                return

        # Add download links
        fx.add_series_download_links(
            content_id=content_id,
            seasons_data=tvshow_data.get("seasons", []),
            file_sizes_map=file_sizes_map,
            tvshow_data=tvshow_data,
        )

        if not media_task.site_content_id:
            media_task.site_content_id = content_id
            media_task.save(update_fields=["site_content_id", "updated_at"])
        logger.info(f"FlixBD: series done -- site_content_id={content_id} clean_title='{title}'")


    except Exception as e:
        logger.error(f"FlixBD publish failed for series '{title}': {e}", exc_info=True)
