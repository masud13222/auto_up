import os
import json
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from .models import MovieTask
from .service.info import get_content_info, get_structured_output
from .service.downloader import Downloader
from .service.uploader import DriveUploader
from .utils.subtitle_remove import process_downloaded_files
from llm.services import LLMService
from llm.schema import FILENAME_SYSTEM_PROMPT, filename_schema
from llm.tvshow_schema import TVSHOW_FILENAME_SYSTEM_PROMPT, tvshow_filename_schema
from django.conf import settings

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────
# Movie Pipeline
# ─────────────────────────────────────────────────────

def _process_movie_pipeline(movie_task, movie_data):
    """
    Movie pipeline: Generate filenames → Download → Clean → Upload → Cleanup
    """
    title = movie_data.get("title", "Unknown")

    # Step 1: Check download links
    download_links = movie_data.get("download_links", {})
    if not download_links:
        logger.warning(f"No download links found for {title}")
        movie_task.status = 'failed'
        movie_task.error_message = 'No download links found'
        movie_task.result = movie_data
        movie_task.save()
        return json.dumps({"status": "error", "message": "No download links found"})

    # Step 2: Generate filenames via LLM
    logger.info(f"Generating filenames for movie: {title}")
    filename_response = LLMService.generate_completion(
        prompt=json.dumps(movie_data, indent=2),
        system_prompt=FILENAME_SYSTEM_PROMPT
    )
    filenames = get_structured_output(filename_response, filename_schema)

    # Step 3: Parallel download → sequential clean → upload → delete
    service = DriveUploader._get_drive_service()

    from settings.models import UploadSettings
    upload_settings = UploadSettings.objects.filter(pk=1).first()
    if not upload_settings:
        raise Exception("UploadSettings not configured.")

    year = movie_data.get("year", "")
    folder_name = f"{title} ({year})" if year else title
    movie_folder_id = DriveUploader._get_or_create_folder(
        service, folder_name, upload_settings.upload_folder_id
    )

    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in title).strip()
    drive_links = {}

    def _download_one(quality, urls, fname):
        """Download a single quality (runs in thread)."""
        url_list = urls if isinstance(urls, list) else [urls]
        for url in url_list:
            file_path = Downloader.download(url, fname, sub_folder=safe_title)
            if file_path:
                return quality, file_path
        return quality, None

    # Collect downloadable qualities
    to_download = []
    for quality in ["480p", "720p", "1080p"]:
        urls = download_links.get(quality)
        fname = filenames.get(quality)
        if urls and fname:
            to_download.append((quality, urls, fname))

    if not to_download:
        logger.warning(f"No valid download links found for: {title}")
        movie_task.status = 'failed'
        movie_task.error_message = 'No valid download links found (after link resolution)'
        movie_task.save()
        return json.dumps({"status": "error", "message": "No valid links found"})

    logger.info(f"Starting parallel downloads: {[q for q, _, _ in to_download]}")
    with ThreadPoolExecutor(max_workers=len(to_download)) as executor:
        futures = {
            executor.submit(_download_one, q, u, f): q
            for q, u, f in to_download
        }

        for future in as_completed(futures):
            quality, file_path = future.result()

            if not file_path:
                logger.warning(f"Could not download {quality}")
                continue

            # Clean subtitles
            logger.info(f"Cleaning subtitles for {quality}")
            cleaned = process_downloaded_files({quality: file_path})
            file_path = cleaned.get(quality, file_path)

            # Upload to Drive
            logger.info(f"Uploading {quality} to Drive")
            try:
                link = DriveUploader._upload_file(service, file_path, movie_folder_id)
                drive_links[quality] = link
            except Exception as e:
                logger.error(f"Upload failed for {quality}: {e}")
                drive_links[quality] = f"UPLOAD_FAILED: {e}"

            # Delete local file
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Removed local file: {file_path}")

    # Clean empty folder
    movie_dir = os.path.join(settings.DOWNLOADS_DIR, safe_title)
    if os.path.isdir(movie_dir) and not os.listdir(movie_dir):
        shutil.rmtree(movie_dir, ignore_errors=True)

    # Update movie_data with drive links
    if drive_links:
        movie_data["download_links"] = drive_links

    if not drive_links:
        movie_task.status = 'failed'
        movie_task.error_message = 'No files could be downloaded or uploaded'
        movie_task.result = movie_data
        movie_task.save()
        return json.dumps({"status": "error", "message": "Pipeline failed"})

    # Save result
    movie_task.status = 'completed'
    movie_task.result = movie_data
    movie_task.error_message = ''
    movie_task.save()

    logger.info(f"Movie pipeline complete for: {title}")
    return json.dumps({"status": "success", "type": "movie", "data": movie_data})


# ─────────────────────────────────────────────────────
# TV Show Pipeline
# ─────────────────────────────────────────────────────

def _process_tvshow_pipeline(movie_task, tvshow_data):
    """
    TV Show pipeline: Generate filenames → Download season-wise → Clean → Upload → Cleanup
    """
    title = tvshow_data.get("title", "Unknown")

    # Step 1: Check if seasons have download items
    seasons = tvshow_data.get("seasons", [])
    if not seasons:
        logger.warning(f"No seasons/download data found for {title}")
        movie_task.status = 'failed'
        movie_task.error_message = 'No seasons or download links found'
        movie_task.result = tvshow_data
        movie_task.save()
        return json.dumps({"status": "error", "message": "No seasons found"})

    # Step 2: Generate filenames via LLM
    logger.info(f"Generating filenames for TV show: {title}")
    filename_response = LLMService.generate_completion(
        prompt=json.dumps(tvshow_data, indent=2),
        system_prompt=TVSHOW_FILENAME_SYSTEM_PROMPT
    )
    filenames = get_structured_output(filename_response, tvshow_filename_schema)
    # filenames could be a list (JSON array)
    if not isinstance(filenames, list):
        filenames = [filenames]

    # Step 3: Download all items (season-wise)
    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in title).strip()
    downloaded_items = Downloader.download_all_tvshow(seasons, filenames, title)

    if not downloaded_items:
        logger.warning(f"No files downloaded for TV show: {title}")
        movie_task.status = 'failed'
        movie_task.error_message = 'No files could be downloaded'
        movie_task.result = tvshow_data
        movie_task.save()
        return json.dumps({"status": "error", "message": "Download failed"})

    # Step 4: Clean subtitles from all downloaded files
    for item in downloaded_items:
        resolutions = item.get("resolutions", {})
        cleaned = process_downloaded_files(resolutions)
        item["resolutions"] = {q: cleaned.get(q, p) for q, p in resolutions.items()}

    # Step 5: Upload to Drive (season-wise folder structure)
    logger.info(f"Uploading TV show '{title}' to Drive...")
    tvshow_data = DriveUploader.upload_tvshow(tvshow_data, downloaded_items)

    # Step 6: Delete local files
    for item in downloaded_items:
        for quality, file_path in item.get("resolutions", {}).items():
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Removed local file: {file_path}")

    # Clean empty folder
    show_dir = os.path.join(settings.DOWNLOADS_DIR, safe_title)
    if os.path.isdir(show_dir) and not os.listdir(show_dir):
        shutil.rmtree(show_dir, ignore_errors=True)

    # Save result
    movie_task.status = 'completed'
    movie_task.result = tvshow_data
    movie_task.error_message = ''
    movie_task.save()

    logger.info(f"TV Show pipeline complete for: {title}")
    return json.dumps({"status": "success", "type": "tvshow", "data": tvshow_data})


# ─────────────────────────────────────────────────────
# Main Task Entry Point (Auto-Detect)
# ─────────────────────────────────────────────────────

def process_movie_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    Auto-detects whether content is a Movie or TV Show and routes accordingly.
    Updates MovieTask status at each step.
    """
    movie_task = MovieTask.objects.get(pk=task_pk)
    movie_task.status = 'processing'
    movie_task.save(update_fields=['status', 'updated_at'])

    try:
        url = movie_task.url
        logger.info(f"Task started for URL: {url}")

        # Step 1: Auto-detect content type and extract info
        content_type, data = get_content_info(url)
        title = data.get("title", "Unknown")
        movie_task.title = title
        movie_task.save(update_fields=['title', 'updated_at'])

        logger.info(f"Detected content type: {content_type} — Title: {title}")

        # Step 2: Route to appropriate pipeline
        if content_type == "tvshow":
            return _process_tvshow_pipeline(movie_task, data)
        else:
            return _process_movie_pipeline(movie_task, data)

    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        movie_task.status = 'failed'
        movie_task.error_message = str(e)
        movie_task.save()
        return json.dumps({"status": "error", "message": str(e)})
