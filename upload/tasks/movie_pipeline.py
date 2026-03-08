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
from llm.schema import FILENAME_SYSTEM_PROMPT, filename_schema
from django.conf import settings

from .helpers import save_task, is_drive_link

logger = logging.getLogger(__name__)


def process_movie_pipeline(media_task, movie_data):
    """
    Movie pipeline: Generate filenames → Parallel Download+Upload → Cleanup
    Each quality downloads in parallel, and uploads as soon as download finishes.
    Supports resume — skips qualities already uploaded to Drive.
    """
    title = movie_data.get("title", "Unknown")

    # Step 1: Check download links
    download_links = movie_data.get("download_links", {})
    if not download_links:
        logger.warning(f"No download links found for {title}")
        save_task(media_task, status='failed', error_message='No download links found', result=movie_data)
        return json.dumps({"status": "error", "message": "No download links found"})

    # Save: LLM extraction complete + resolved links
    save_task(media_task, result=movie_data)
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

    # Collect downloadable qualities (skip already-uploaded ones)
    to_process = []
    for quality in ["480p", "720p", "1080p"]:
        urls = download_links.get(quality)
        fname = filenames.get(quality)

        # Skip if already uploaded to Drive (resume support)
        if is_drive_link(urls):
            logger.info(f"Skipping {quality}: already uploaded to Drive")
            drive_links[quality] = urls
            continue

        if urls and fname:
            to_process.append((quality, urls, fname))

    if not to_process:
        if drive_links:
            # All already uploaded — mark as complete
            movie_data["download_links"] = drive_links
            save_task(media_task, status='completed', result=movie_data, error_message='')
            logger.info(f"Movie already fully uploaded: {title}")
            return json.dumps({"status": "success", "type": "movie", "data": movie_data})

        logger.warning(f"No valid download links found for: {title}")
        save_task(media_task, status='failed', error_message='No valid download links found (after link resolution)')
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
                save_task(media_task, result=movie_data)
                logger.info(f"Saved Drive link for {quality}")

    # Clean empty folder
    movie_dir = os.path.join(settings.DOWNLOADS_DIR, safe_title)
    if os.path.isdir(movie_dir) and not os.listdir(movie_dir):
        shutil.rmtree(movie_dir, ignore_errors=True)

    # Final save
    if drive_links:
        movie_data["download_links"] = drive_links

    if not drive_links:
        save_task(media_task, status='failed', error_message='No files could be downloaded or uploaded', result=movie_data)
        return json.dumps({"status": "error", "message": "Pipeline failed"})

    save_task(media_task, status='completed', result=movie_data, error_message='')

    logger.info(f"Movie pipeline complete for: {title}")
    return json.dumps({"status": "success", "type": "movie", "data": movie_data})
