import os
import json
import shutil
import logging
from .models import MovieTask
from .service.info import get_movie_info, get_structured_output
from .service.downloader import Downloader
from .service.uploader import DriveUploader
from .utils.subtitle_remove import process_downloaded_files
from llm.services import LLMService
from llm.schema import FILENAME_SYSTEM_PROMPT, filename_schema
from django.conf import settings

logger = logging.getLogger(__name__)


def process_movie_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    Updates MovieTask status at each step.
    """
    movie_task = MovieTask.objects.get(pk=task_pk)
    movie_task.status = 'processing'
    movie_task.save(update_fields=['status', 'updated_at'])

    try:
        url = movie_task.url
        logger.info(f"Task started for URL: {url}")

        # Step 1: Get movie info
        movie_data = get_movie_info(url)
        title = movie_data.get("title", "Unknown")
        movie_task.title = title
        movie_task.save(update_fields=['title', 'updated_at'])

        # Step 2: Generate filenames from LLM
        download_links = movie_data.get("download_links", {})
        if not download_links:
            logger.warning(f"No download links found for {title}")
            movie_task.status = 'failed'
            movie_task.error_message = 'No download links found'
            movie_task.result = movie_data
            movie_task.save()
            return json.dumps({"status": "error", "message": "No download links found"})

        logger.info(f"Generating filenames for: {title}")
        filename_response = LLMService.generate_completion(
            prompt=json.dumps(movie_data, indent=2),
            system_prompt=FILENAME_SYSTEM_PROMPT
        )
        filenames = get_structured_output(filename_response, filename_schema)

        # Step 3: Parallel downloads, sequential upload
        # All qualities download at the same time.
        # Whichever finishes first → clean → upload → delete → next finished one
        from concurrent.futures import ThreadPoolExecutor, as_completed

        service = DriveUploader._get_drive_service()
        
        # Get upload folder
        from settings.models import UploadSettings
        upload_settings = UploadSettings.objects.filter(pk=1).first()
        if not upload_settings:
            raise Exception("UploadSettings not configured.")
        
        year = movie_data.get("year", "")
        folder_name = f"{title} ({year})" if year else title
        movie_folder_id = DriveUploader._get_or_create_folder(service, folder_name, upload_settings.upload_folder_id)

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

        # Start all downloads in parallel
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

            # Process each as it completes (first finished = first uploaded)
            for future in as_completed(futures):
                quality, file_path = future.result()

                if not file_path:
                    logger.warning(f"Could not download {quality}")
                    continue

                # Clean subtitles
                logger.info(f"Cleaning subtitles for {quality}")
                cleaned = process_downloaded_files({quality: file_path})
                file_path = cleaned.get(quality, file_path)

                # Upload to Drive (sequential — one at a time)
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

        logger.info(f"Pipeline complete for: {title}")
        return json.dumps({"status": "success", "movie": movie_data})

    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        movie_task.status = 'failed'
        movie_task.error_message = str(e)
        movie_task.save()
        return json.dumps({"status": "error", "message": str(e)})


