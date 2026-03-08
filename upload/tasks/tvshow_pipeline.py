import os
import json
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from upload.service.info import get_structured_output
from upload.service.downloader import Downloader
from upload.service.uploader import DriveUploader
from upload.utils.subtitle_remove import process_downloaded_files
from llm.services import LLMService
from llm.tvshow_schema import TVSHOW_FILENAME_SYSTEM_PROMPT, tvshow_filename_schema
from django.conf import settings

from .helpers import save_task, is_drive_link, log_memory

logger = logging.getLogger(__name__)


def process_tvshow_pipeline(media_task, tvshow_data):
    """
    TV Show pipeline: Generate filenames → Download/Clean/Upload → Cleanup

    Parallelism strategy:
    - Items (combo/partial/single) processed SEQUENTIALLY
    - Downloads:  PARALLEL (all resolutions download at once)
    - FFmpeg:     SEQUENTIAL (one at a time, prevents OOM)
    - Uploads:    PARALLEL (runs in background alongside next ffmpeg)
    - Supports resume — skips items/resolutions already uploaded to Drive
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

    # Step 2: Generate filenames via LLM
    logger.info(f"Generating filenames for TV show: {title}")
    filename_response = LLMService.generate_completion(
        prompt=json.dumps(tvshow_data, indent=2),
        system_prompt=TVSHOW_FILENAME_SYSTEM_PROMPT
    )
    filenames = get_structured_output(filename_response, tvshow_filename_schema)
    if not isinstance(filenames, list):
        filenames = [filenames]

    # Step 3: Setup Drive
    service = DriveUploader._get_drive_service()

    from settings.models import UploadSettings
    upload_settings = UploadSettings.objects.filter(pk=1).first()
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
    for season in seasons:
        season_num = season.get("season_number")
        for item in season.get("download_items", []):
            item_type = item.get("type")
            item_label = item.get("label", "Unknown")
            resolutions = item.get("resolutions", {})

            # Find matching filename
            fname_item = next(
                (f for f in filenames
                 if f.get("season_number") == season_num
                 and f.get("type") == item_type
                 and f.get("label") == item_label),
                {}
            )
            fname_resolutions = fname_item.get("resolutions", {})

            all_items.append({
                "season_number": season_num,
                "type": item_type,
                "label": item_label,
                "resolutions": resolutions,
                "fname_resolutions": fname_resolutions,
                "season_folder_id": season_folders.get(season_num),
            })

    # ── Helper functions ──

    def _download_one(quality, urls, fname, season_num, item_label):
        """Download only — runs in PARALLEL thread."""
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
        return quality, file_path

    def _clean_one(quality, file_path):
        """FFmpeg subtitle clean — runs SEQUENTIALLY in main thread."""
        log_memory(f"Before ffmpeg {quality}")
        cleaned = process_downloaded_files({quality: file_path})
        file_path = cleaned.get(quality, file_path)
        log_memory(f"After ffmpeg {quality}")
        return file_path

    def _upload_one(quality, file_path, season_folder_id, season_num, item_label):
        """Upload to Drive + delete — runs in PARALLEL thread."""
        log_memory(f"Before upload {quality}")
        try:
            # Each thread gets its OWN service (google-api-python-client is NOT thread-safe)
            thread_service = DriveUploader._get_drive_service()
            link = DriveUploader._upload_file(thread_service, file_path, season_folder_id)
            logger.info(f"Uploaded S{season_num} {item_label} {quality}")
        except Exception as e:
            logger.error(f"Upload failed for S{season_num} {item_label} {quality}: {e}")
            link = f"UPLOAD_FAILED: {e}"

        # Delete local file after upload
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Removed local file: {file_path}")

        log_memory(f"After upload+delete {quality}")
        return quality, link

    # ── Step 4: Process items ──
    # Items: SEQUENTIAL | Downloads: PARALLEL | FFmpeg: SEQUENTIAL | Uploads: PARALLEL
    uploaded_count = 0
    total_items = len(all_items)

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
        for quality in ["480p", "720p", "1080p"]:
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
            else:
                logger.warning(f"No downloadable resolutions for S{season_num} {item_label}")
            continue

        logger.info(f"[{idx}/{total_items}] Processing S{season_num} {item_label}: {[q for q, _, _ in to_process]}")

        # ── Phase 1: Download ALL resolutions PARALLEL ──
        uploaded_resolutions = dict(already_uploaded)
        upload_executor = ThreadPoolExecutor(max_workers=3)
        upload_futures = []

        with ThreadPoolExecutor(max_workers=len(to_process)) as dl_executor:
            download_futures = {
                dl_executor.submit(
                    _download_one, q, u, f, season_num, item_label
                ): q
                for q, u, f in to_process
            }

            # ── Phase 2: As each download completes ──
            #   FFmpeg: SEQUENTIAL (blocks in main thread)
            #   Upload: PARALLEL (fire to background thread)
            for future in as_completed(download_futures):
                quality, file_path = future.result()
                if not file_path:
                    continue

                # FFmpeg — SEQUENTIAL (one at a time)
                file_path = _clean_one(quality, file_path)

                # Upload — PARALLEL (background thread)
                uf = upload_executor.submit(
                    _upload_one, quality, file_path,
                    season_folder_id, season_num, item_label
                )
                upload_futures.append(uf)

        # Wait for all background uploads to finish
        for uf in upload_futures:
            quality, link = uf.result()
            if link:
                uploaded_resolutions[quality] = link

        upload_executor.shutdown(wait=False)

        if uploaded_resolutions:
            uploaded_count += 1

            # Update tvshow_data with drive links
            for season in tvshow_data.get("seasons", []):
                if season.get("season_number") == season_num:
                    for item in season.get("download_items", []):
                        if item.get("type") == item_type and item.get("label") == item_label:
                            item["resolutions"] = uploaded_resolutions
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
    return json.dumps({"status": "success", "type": "tvshow", "data": tvshow_data})
