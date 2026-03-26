import os
import json
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from upload.service.downloader import Downloader
from upload.service.uploader import DriveUploader
from upload.utils.subtitle_remove import process_downloaded_files
from screenshot.services.capture import capture_screenshots_for_publish
from django.conf import settings

from .helpers import save_task, is_drive_link, log_memory, validate_llm_download_basename

logger = logging.getLogger(__name__)


def _tv_item_filenames_from_llm(item: dict, season_num) -> dict:
    """Use LLM output only; raise ValueError if any resolution lacks a safe non-empty basename."""
    raw = item.get("download_filenames")
    if not isinstance(raw, dict):
        raise ValueError(
            f"S{season_num} {item.get('label')!r}: download_filenames must be a JSON object"
        )
    resolutions = item.get("resolutions") or {}
    out = {}
    for q in resolutions:
        ctx = f"S{season_num} {item.get('label')!r} download_filenames[{q!r}]"
        if q not in raw:
            raise ValueError(
                f"{ctx}: missing key — need one basename per resolutions key "
                f"{list(resolutions.keys())!r}"
            )
        out[q] = validate_llm_download_basename(raw.get(q), context=ctx)
    item["download_filenames"] = out
    return out


def process_tvshow_pipeline(media_task, tvshow_data, dup_info=None):
    """
    TV Show pipeline: download_filenames from main LLM extract -> Download/Clean/Upload -> FlixBD -> Cleanup

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

    is_dup_update = bool(dup_info and dup_info.get("action") == "update")
    if not is_dup_update:
        tvshow_data.pop("screen_shots_url", None)

    # Step 2: Filenames embedded in extract (per-item download_filenames)
    logger.info(f"Resolving download_filenames for TV show: {title}")

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
    try:
        for season in seasons:
            season_num = season.get("season_number")
            for item in season.get("download_items", []):
                item_type = item.get("type")
                item_label = item.get("label", "Unknown")
                resolutions = item.get("resolutions", {})

                fname_resolutions = _tv_item_filenames_from_llm(item, season_num)

                all_items.append({
                    "season_number": season_num,
                    "type": item_type,
                    "label": item_label,
                    "resolutions": resolutions,
                    "fname_resolutions": fname_resolutions,
                    "season_folder_id": season_folders.get(season_num),
                })
    except ValueError as e:
        msg = str(e)
        logger.error("TV show pipeline: %s", msg)
        save_task(media_task, status="failed", error_message=msg, result=tvshow_data)
        return json.dumps({"status": "error", "message": msg})

    # -- Helper functions --

    def _download_one(quality, urls, fname, season_num, item_label):
        """Download only -- runs in PARALLEL thread."""
        from upload.service.flixbd_client import format_file_size
        log_memory(f"Before download {quality}")
        url_list = urls if isinstance(urls, list) else [urls]

        file_path = None
        for url in url_list:
            file_path = Downloader.download(url, fname, sub_folder=safe_title)
            if file_path:
                break

        if not file_path:
            logger.warning(f"Could not download {quality} for S{season_num} {item_label}")
            log_memory(f"After download {quality}")
            return quality, file_path, None

        # Capture file size BEFORE ffmpeg/deletion
        try:
            raw_size = os.path.getsize(file_path)
            size_str = format_file_size(raw_size)
        except OSError:
            size_str = None

        log_memory(f"After download {quality}")
        return quality, file_path, size_str

    def _clean_one(quality, file_path):
        """FFmpeg subtitle clean -- runs SEQUENTIALLY in main thread."""
        log_memory(f"Before ffmpeg {quality}")
        cleaned = process_downloaded_files({quality: file_path})
        file_path = cleaned.get(quality, file_path)
        log_memory(f"After ffmpeg {quality}")
        return file_path

    def _upload_one(quality, file_path, season_folder_id, season_num, item_label):
        """Upload to Drive + delete -- runs in PARALLEL thread."""
        log_memory(f"Before upload {quality}")
        try:
            # Each thread gets its OWN service (google-api-python-client is NOT thread-safe)
            thread_service = DriveUploader._get_drive_service()
            link = DriveUploader._upload_file(thread_service, file_path, season_folder_id)
            logger.info(f"Uploaded S{season_num} {item_label} {quality}")
        except Exception as e:
            logger.error(f"Upload failed for S{season_num} {item_label} {quality}: {e}")
            link = None

        # Delete local file after upload
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Removed local file: {file_path}")

        log_memory(f"After upload+delete {quality}")
        return quality, link

    # -- Step 4: Process items --
    # Items: SEQUENTIAL | Downloads: PARALLEL | FFmpeg: SEQUENTIAL | Uploads: PARALLEL
    uploaded_count = 0
    total_items = len(all_items)

    # Largest video seen so far (bytes) — refresh series screenshots from bigger files only
    tvshow_ss_best_size = 0

    # {(season_num, label, quality): "2.1 GB"} -- collected across all items
    file_sizes_map = {}

    logger.info(f"Processing {total_items} TV show item(s)")
    log_memory("Pipeline start")

    for idx, item_info in enumerate(all_items, 1):
        season_num = item_info["season_number"]
        item_type = item_info["type"]
        item_label = item_info["label"]
        resolutions = item_info["resolutions"]
        fname_resolutions = item_info["fname_resolutions"]
        season_folder_id = item_info["season_folder_id"]

        # Collect resolutions to process (skip already-uploaded ones)
        to_process = []
        already_uploaded = {}
        # size_staging: {quality: size_str} -- sizes captured during download
        # Pre-populate from persisted item["sizes"] so already-uploaded resolutions
        # carry their sizes forward into file_sizes_map and the merged item["sizes"].
        item_in_data = next(
            (i for s in tvshow_data.get("seasons", [])
             if s.get("season_number") == season_num
             for i in s.get("download_items", [])
             if i.get("type") == item_type and i.get("label") == item_label),
            {}
        )
        size_staging = dict(item_in_data.get("sizes", {}))  # pre-load persisted sizes

        for quality in resolutions:
            urls = resolutions.get(quality)
            fname = fname_resolutions.get(quality)

            if is_drive_link(urls):
                logger.info(f"Skipping S{season_num} {item_label} {quality}: already uploaded")
                already_uploaded[quality] = urls
                continue

            if urls and fname:
                to_process.append((quality, urls, fname))

        if not to_process:
            if already_uploaded:
                logger.info(f"Skipping S{season_num} {item_label}: already uploaded to Drive")
                uploaded_count += 1
                # Still populate file_sizes_map so FlixBD publish has sizes
                for q, size in size_staging.items():
                    if size:
                        file_sizes_map[(season_num, item_label, q)] = size
            else:
                logger.warning(f"No downloadable resolutions for S{season_num} {item_label}")
            continue


        logger.info(f"[{idx}/{total_items}] Processing S{season_num} {item_label}: {[q for q, _, _ in to_process]}")

        # -- Phase 1: Download ALL resolutions PARALLEL --
        uploaded_resolutions = dict(already_uploaded)

        with ThreadPoolExecutor(max_workers=3) as upload_executor:
            upload_futures = []

            with ThreadPoolExecutor(max_workers=len(to_process)) as dl_executor:
                download_futures = {
                    dl_executor.submit(
                        _download_one, q, u, f, season_num, item_label
                    ): q
                    for q, u, f in to_process
                }

                # -- Phase 2: As each download completes --
                #   FFmpeg: SEQUENTIAL (blocks in main thread)
                #   Upload: PARALLEL (fire to background thread)
                cleaned_batch = []
                for future in as_completed(download_futures):
                    try:
                        quality, file_path, size_str = future.result()
                    except Exception as e:
                        quality = download_futures[future]
                        logger.error(f"Download failed for S{season_num} {item_label} {quality}: {e}")
                        continue
                    if not file_path:
                        continue

                    if size_str:
                        size_staging[quality] = size_str

                    file_path = _clean_one(quality, file_path)
                    cleaned_batch.append((quality, file_path, size_str))

                if cleaned_batch and not is_dup_update:
                    best_q, best_path, _ = max(
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
                                best_q,
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

                for quality, file_path, size_str in cleaned_batch:
                    uf = upload_executor.submit(
                        _upload_one, quality, file_path,
                        season_folder_id, season_num, item_label
                    )
                    upload_futures.append((uf, quality))

            # Wait for all background uploads to finish (with statement handles shutdown)
            for uf, q in upload_futures:
                try:
                    quality, link = uf.result()
                except Exception as e:
                    logger.error(f"Upload thread failed for S{season_num} {item_label}: {e}")
                    continue
                if link:
                    uploaded_resolutions[quality] = link
                    # Use q from submit (source of truth) — matches size_staging keys
                    if size_staging.get(q):
                        file_sizes_map[(season_num, item_label, q)] = size_staging[q]

        if uploaded_resolutions:
            uploaded_count += 1

            # Update tvshow_data with drive links + sizes
            for season in tvshow_data.get("seasons", []):
                if season.get("season_number") == season_num:
                    for item in season.get("download_items", []):
                        if item.get("type") == item_type and item.get("label") == item_label:
                            item["resolutions"] = uploaded_resolutions
                            # Persist sizes into result JSON
                            if size_staging:
                                existing_sizes = item.get("sizes", {})
                                existing_sizes.update(size_staging)
                                item["sizes"] = existing_sizes
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

    # Final save
    save_task(media_task, status='completed', result=tvshow_data, error_message='')

    log_memory("Pipeline complete")
    logger.info(f"TV Show pipeline complete for: {title}. Uploaded {uploaded_count}/{total_items} items.")

    # Step 5: Publish to FlixBD
    _publish_to_flixbd_series(media_task, tvshow_data, file_sizes_map)

    return json.dumps({"status": "success", "type": "tvshow", "data": tvshow_data})


def _publish_to_flixbd_series(media_task, tvshow_data, file_sizes_map):
    """
    Add Drive links to FlixBD after upload completes.

    **Update path:** same as movie — ``site_content_id`` on the task (from DB row or
    pre-publish FlixBD reuse) triggers ``patch_series_title`` with latest
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
            logger.info(f"FlixBD: existing series id={cid} — PATCH title then add links")
            fx.patch_series_title(int(cid), tvshow_data)
            content_id = int(cid)
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
