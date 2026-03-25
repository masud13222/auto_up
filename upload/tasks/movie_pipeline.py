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
from llm.schema import FILENAME_SYSTEM_PROMPT
from django.conf import settings

from .helpers import save_task, is_drive_link

logger = logging.getLogger(__name__)


def process_movie_pipeline(media_task, movie_data, dup_info=None):
    """
    Movie pipeline: Generate filenames -> Parallel Download+Upload -> FlixBD Publish -> Cleanup
    Each quality downloads in parallel, and uploads as soon as download finishes.
    Supports resume -- skips qualities already uploaded to Drive.
    File size is captured from the local file after download (before deletion).
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
        system_prompt=FILENAME_SYSTEM_PROMPT,
        purpose='movie_filename',
    )
    filenames = get_structured_output(filename_response)

    # Step 3: Setup Drive
    service = DriveUploader._get_drive_service()

    from settings.models import UploadSettings
    upload_settings = UploadSettings.objects.first()
    if not upload_settings:
        raise Exception("UploadSettings not configured.")

    year = movie_data.get("year", "")
    folder_name = f"{title} {year}" if year else title
    movie_folder_id = DriveUploader._get_or_create_folder(
        service, folder_name, upload_settings.upload_folder_id
    )

    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in title).strip()
    drive_links = {}
    # {quality: "2.15 GB"} -- populated from actual local file before deletion
    file_sizes = {}

    # Duplicate "update": LLM lists only qualities missing on FlixBD/DB — skip re-downloading the rest.
    missing_only: set[str] | None = None
    if dup_info and dup_info.get("action") == "update":
        mr = dup_info.get("missing_resolutions")
        if isinstance(mr, list) and mr:
            missing_only = {str(x).strip().lower() for x in mr if x is not None and str(x).strip()}
            logger.info(
                f"Duplicate update mode: will only download/upload resolutions {sorted(missing_only)} "
                f"(others already on target site)"
            )

    def _process_one_quality(quality, urls, fname):
        """Download -> Capture Size -> Clean -> Upload -> Delete one quality (runs in thread)."""
        from upload.service.flixbd_client import format_file_size
        url_list = urls if isinstance(urls, list) else [urls]

        # Download
        file_path = None
        for url in url_list:
            file_path = Downloader.download(url, fname, sub_folder=safe_title)
            if file_path:
                break

        if not file_path:
            logger.warning(f"Could not download {quality}")
            return quality, None, None

        # Capture file size BEFORE any processing/deletion
        try:
            raw_size = os.path.getsize(file_path)
            size_str = format_file_size(raw_size)
            logger.debug(f"File size for {quality}: {size_str}")
        except OSError:
            size_str = None

        # Clean subtitles
        logger.info(f"Cleaning subtitles for {quality}")
        cleaned = process_downloaded_files({quality: file_path})
        file_path = cleaned.get(quality, file_path)

        # Upload to Drive (each thread gets its OWN service -- NOT thread-safe)
        logger.info(f"Uploading {quality} to Drive")
        try:
            thread_service = DriveUploader._get_drive_service()
            link = DriveUploader._upload_file(thread_service, file_path, movie_folder_id)
        except Exception as e:
            logger.error(f"Upload failed for {quality}: {e}")
            link = None

        # Delete local file
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Removed local file: {file_path}")

        return quality, link, size_str

    # Collect downloadable qualities (skip already-uploaded ones)
    to_process = []
    for quality in download_links:
        urls = download_links.get(quality)
        fname = filenames.get(quality)
        qkey = str(quality)

        # Skip if already uploaded to Drive (resume support)
        if is_drive_link(urls):
            logger.info(f"Skipping {quality}: already uploaded to Drive")
            drive_links[quality] = urls
            continue

        if missing_only is not None and qkey.lower() not in missing_only:
            logger.info(
                f"Skipping {quality}: duplicate update — not in missing_resolutions "
                f"(already published on target site)"
            )
            continue

        if urls and fname:
            to_process.append((quality, urls, fname))

    if not to_process:
        if drive_links:
            # All already uploaded -- mark as complete and try FlixBD publish
            movie_data["download_links"] = drive_links
            save_task(media_task, status='completed', result=movie_data, error_message='')
            logger.info(f"Movie already fully uploaded: {title}")
            _publish_to_flixbd_movie(media_task, movie_data, drive_links, file_sizes)
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
            quality, link, size_str = future.result()
            if link:
                drive_links[quality] = link
                if size_str:
                    file_sizes[quality] = size_str
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

    # Step 5: Publish to FlixBD
    _publish_to_flixbd_movie(media_task, movie_data, drive_links, file_sizes)

    return json.dumps({"status": "success", "type": "movie", "data": movie_data})


def _publish_to_flixbd_movie(media_task, movie_data, drive_links, file_sizes):
    """
    Add Drive links to FlixBD after upload completes.
    - If site_content_id already set (from dup check): just add download links
    - If not set: create movie on FlixBD first, then add download links
    Never raises -- errors are logged only.
    """
    from upload.service import flixbd_client as fx

    title = movie_data.get("title", "Unknown")

    try:
        fx._get_config()
    except RuntimeError as e:
        logger.info(f"FlixBD publish skipped: {e}")
        return

    try:
        if media_task.site_content_id:
            # Already found during dup check — skip search+create
            content_id = media_task.site_content_id
            logger.info(f"FlixBD: using pre-found id={content_id} for '{title}' (from dup check)")
        else:
            # Not found yet — create new
            content_id = fx.create_movie(movie_data)

        # Add download links with actual file sizes
        fx.add_movie_download_links(
            content_id=content_id,
            drive_links=drive_links,
            file_sizes=file_sizes,
            movie_data=movie_data,
        )

        # Save site_content_id if it wasn't already
        if not media_task.site_content_id:
            media_task.site_content_id = content_id
            media_task.save(update_fields=["site_content_id", "updated_at"])
        logger.info(f"FlixBD: movie published -- site_content_id={content_id} title='{title}'")

    except Exception as e:
        logger.error(f"FlixBD publish failed for movie '{title}': {e}", exc_info=True)

