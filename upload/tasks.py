import os
import json
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from .models import MediaTask
from .service.info import get_content_info, get_structured_output
from .service.downloader import Downloader
from .service.uploader import DriveUploader
from .utils.subtitle_remove import process_downloaded_files
from llm.services import LLMService
from llm.schema import FILENAME_SYSTEM_PROMPT, filename_schema
from llm.tvshow_schema import TVSHOW_FILENAME_SYSTEM_PROMPT, tvshow_filename_schema
from django.conf import settings

logger = logging.getLogger(__name__)


def _save_task(media_task, **fields):
    """Helper: Update task fields and save to DB immediately."""
    for key, value in fields.items():
        setattr(media_task, key, value)
    media_task.save()


# ─────────────────────────────────────────────────────
# Movie Pipeline
# ─────────────────────────────────────────────────────

def _process_movie_pipeline(media_task, movie_data):
    """
    Movie pipeline: Generate filenames → Parallel Download+Upload → Cleanup
    Each quality downloads in parallel, and uploads as soon as download finishes.
    """
    title = movie_data.get("title", "Unknown")

    # Step 1: Check download links
    download_links = movie_data.get("download_links", {})
    if not download_links:
        logger.warning(f"No download links found for {title}")
        _save_task(media_task, status='failed', error_message='No download links found', result=movie_data)
        return json.dumps({"status": "error", "message": "No download links found"})

    # Save: LLM extraction complete + resolved links
    _save_task(media_task, result=movie_data)
    logger.info(f"Saved LLM extraction result for: {title}")

    # Step 2: Generate filenames via LLM
    logger.info(f"Generating filenames for movie: {title}")
    filename_response = LLMService.generate_completion(
        prompt=json.dumps(movie_data, indent=2),
        system_prompt=FILENAME_SYSTEM_PROMPT
    )
    filenames = get_structured_output(filename_response, filename_schema)

    # Step 3: Setup Drive
    service = DriveUploader._get_drive_service()

    from settings.models import UploadSettings
    upload_settings = UploadSettings.objects.filter(pk=1).first()
    if not upload_settings:
        raise Exception("UploadSettings not configured.")

    year = movie_data.get("year", "")
    folder_name = f"{title} {year}" if year else title
    movie_folder_id = DriveUploader._get_or_create_folder(
        service, folder_name, upload_settings.upload_folder_id
    )

    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in title).strip()
    drive_links = {}

    def _process_one_quality(quality, urls, fname):
        """Download → Clean → Upload → Delete one quality (runs in thread)."""
        url_list = urls if isinstance(urls, list) else [urls]

        # Download
        file_path = None
        for url in url_list:
            file_path = Downloader.download(url, fname, sub_folder=safe_title)
            if file_path:
                break

        if not file_path:
            logger.warning(f"Could not download {quality}")
            return quality, None

        # Clean subtitles
        logger.info(f"Cleaning subtitles for {quality}")
        cleaned = process_downloaded_files({quality: file_path})
        file_path = cleaned.get(quality, file_path)

        # Upload to Drive
        logger.info(f"Uploading {quality} to Drive")
        try:
            link = DriveUploader._upload_file(service, file_path, movie_folder_id)
        except Exception as e:
            logger.error(f"Upload failed for {quality}: {e}")
            link = f"UPLOAD_FAILED: {e}"

        # Delete local file
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Removed local file: {file_path}")

        return quality, link

    # Collect downloadable qualities
    to_process = []
    for quality in ["480p", "720p", "1080p"]:
        urls = download_links.get(quality)
        fname = filenames.get(quality)
        if urls and fname:
            to_process.append((quality, urls, fname))

    if not to_process:
        logger.warning(f"No valid download links found for: {title}")
        _save_task(media_task, status='failed', error_message='No valid download links found (after link resolution)')
        return json.dumps({"status": "error", "message": "No valid links found"})

    # Step 4: Parallel download + upload
    logger.info(f"Starting parallel processing: {[q for q, _, _ in to_process]}")
    with ThreadPoolExecutor(max_workers=len(to_process)) as executor:
        futures = {
            executor.submit(_process_one_quality, q, u, f): q
            for q, u, f in to_process
        }

        for future in as_completed(futures):
            quality, link = future.result()
            if link:
                drive_links[quality] = link
                # Save progress after each quality
                movie_data["download_links"] = drive_links
                _save_task(media_task, result=movie_data)
                logger.info(f"Saved Drive link for {quality}")

    # Clean empty folder
    movie_dir = os.path.join(settings.DOWNLOADS_DIR, safe_title)
    if os.path.isdir(movie_dir) and not os.listdir(movie_dir):
        shutil.rmtree(movie_dir, ignore_errors=True)

    # Final save
    if drive_links:
        movie_data["download_links"] = drive_links

    if not drive_links:
        _save_task(media_task, status='failed', error_message='No files could be downloaded or uploaded', result=movie_data)
        return json.dumps({"status": "error", "message": "Pipeline failed"})

    _save_task(media_task, status='completed', result=movie_data, error_message='')

    logger.info(f"Movie pipeline complete for: {title}")
    return json.dumps({"status": "success", "type": "movie", "data": movie_data})


# ─────────────────────────────────────────────────────
# TV Show Pipeline
# ─────────────────────────────────────────────────────

def _process_tvshow_pipeline(media_task, tvshow_data):
    """
    TV Show pipeline: Generate filenames → Parallel Download+Upload per item → Cleanup
    
    Parallelism strategy:
    - Each download_item (combo/partial/single) runs in parallel
    - Within each item, resolutions (480p/720p/1080p) download sequentially
    - As soon as an item finishes downloading → clean → upload → delete
    """
    title = tvshow_data.get("title", "Unknown")

    # Step 1: Check if seasons have download items
    seasons = tvshow_data.get("seasons", [])
    if not seasons:
        logger.warning(f"No seasons/download data found for {title}")
        _save_task(media_task, status='failed', error_message='No seasons or download links found', result=tvshow_data)
        return json.dumps({"status": "error", "message": "No seasons found"})

    has_download_items = any(
        item for s in seasons for item in s.get("download_items", [])
    )
    if not has_download_items:
        logger.warning(f"Seasons found but no download items for {title}")
        _save_task(media_task, status='failed', error_message='No download items found in seasons', result=tvshow_data)
        return json.dumps({"status": "error", "message": "No download items found"})

    # Save: LLM extraction complete
    _save_task(media_task, result=tvshow_data)
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

    # Pre-create season folders (to avoid race conditions in threads)
    season_folders = {}
    for season in seasons:
        season_num = season.get("season_number")
        if season_num not in season_folders:
            season_folder_name = f"Season {season_num}"
            season_folders[season_num] = DriveUploader._get_or_create_folder(
                service, season_folder_name, show_folder_id
            )

    # Build list of all download items to process
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

    def _process_one_resolution(quality, urls, fname, season_folder_id, season_num, item_label):
        """Download → Clean → Upload → Delete one resolution (runs in thread)."""
        url_list = urls if isinstance(urls, list) else [urls]

        # Download
        file_path = None
        for url in url_list:
            file_path = Downloader.download(url, fname, sub_folder=safe_title)
            if file_path:
                break

        if not file_path:
            logger.warning(f"Could not download {quality} for S{season_num} {item_label}")
            return quality, None

        # Clean subtitles
        cleaned = process_downloaded_files({quality: file_path})
        file_path = cleaned.get(quality, file_path)

        # Upload to Drive
        try:
            link = DriveUploader._upload_file(service, file_path, season_folder_id)
            logger.info(f"Uploaded S{season_num} {item_label} {quality}")
        except Exception as e:
            logger.error(f"Upload failed for S{season_num} {item_label} {quality}: {e}")
            link = f"UPLOAD_FAILED: {e}"

        # Delete local file
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Removed local file: {file_path}")

        return quality, link

    # Step 4: Process items SEQUENTIALLY, resolutions PARALLEL
    uploaded_count = 0
    total_items = len(all_items)

    logger.info(f"Processing {total_items} TV show item(s) sequentially (resolutions parallel)")

    for idx, item_info in enumerate(all_items, 1):
        season_num = item_info["season_number"]
        item_type = item_info["type"]
        item_label = item_info["label"]
        resolutions = item_info["resolutions"]
        fname_resolutions = item_info["fname_resolutions"]
        season_folder_id = item_info["season_folder_id"]

        # Collect resolutions to process
        to_process = []
        for quality in ["480p", "720p", "1080p"]:
            urls = resolutions.get(quality)
            fname = fname_resolutions.get(quality)
            if urls and fname:
                to_process.append((quality, urls, fname))

        if not to_process:
            logger.warning(f"No downloadable resolutions for S{season_num} {item_label}")
            continue

        logger.info(f"[{idx}/{total_items}] Processing S{season_num} {item_label}: {[q for q, _, _ in to_process]}")

        # Parallel download+upload for all resolutions of this item
        uploaded_resolutions = {}
        with ThreadPoolExecutor(max_workers=len(to_process)) as executor:
            futures = {
                executor.submit(
                    _process_one_resolution, q, u, f,
                    season_folder_id, season_num, item_label
                ): q
                for q, u, f in to_process
            }

            for future in as_completed(futures):
                quality, link = future.result()
                if link:
                    uploaded_resolutions[quality] = link

        if uploaded_resolutions:
            uploaded_count += 1

            # Update tvshow_data with drive links for this item
            for season in tvshow_data.get("seasons", []):
                if season.get("season_number") == season_num:
                    for item in season.get("download_items", []):
                        if item.get("type") == item_type and item.get("label") == item_label:
                            item["resolutions"] = uploaded_resolutions
                            break

            # Save progress after each item
            _save_task(media_task, result=tvshow_data)
            logger.info(f"[{idx}/{total_items}] Saved: S{season_num} {item_label}")

    # Clean empty folder
    show_dir = os.path.join(settings.DOWNLOADS_DIR, safe_title)
    if os.path.isdir(show_dir) and not os.listdir(show_dir):
        shutil.rmtree(show_dir, ignore_errors=True)

    if not uploaded_count:
        _save_task(media_task, status='failed', error_message='No files could be downloaded or uploaded', result=tvshow_data)
        return json.dumps({"status": "error", "message": "Pipeline failed"})

    # Final save
    _save_task(media_task, status='completed', result=tvshow_data, error_message='')

    logger.info(f"TV Show pipeline complete for: {title}. Uploaded {uploaded_count}/{total_items} items.")
    return json.dumps({"status": "success", "type": "tvshow", "data": tvshow_data})


# ─────────────────────────────────────────────────────
# Main Task Entry Point (Auto-Detect)
# ─────────────────────────────────────────────────────

def process_media_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    Auto-detects whether content is a Movie or TV Show and routes accordingly.
    Updates MediaTask status at each step.
    """
    media_task = MediaTask.objects.get(pk=task_pk)
    _save_task(media_task, status='processing')

    try:
        url = media_task.url
        logger.info(f"Task started for URL: {url}")

        # Step 1: Auto-detect content type and extract info
        content_type, data = get_content_info(url)
        title = data.get("title", "Unknown")

        # Save: Title + initial extraction result
        _save_task(media_task, title=title, result=data)
        logger.info(f"Detected content type: {content_type} — Title: {title}")

        # Step 2: Route to appropriate pipeline
        if content_type == "tvshow":
            return _process_tvshow_pipeline(media_task, data)
        else:
            return _process_movie_pipeline(media_task, data)

    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        _save_task(media_task, status='failed', error_message=str(e))
        return json.dumps({"status": "error", "message": str(e)})
