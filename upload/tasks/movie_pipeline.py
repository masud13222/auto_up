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

from .helpers import save_task, is_drive_link, validate_llm_download_basename

logger = logging.getLogger(__name__)


def _movie_download_filenames_from_llm(movie_data: dict) -> dict:
    """Require LLM `download_filenames`; validate keys match download_links and basenames are safe."""
    download_links = movie_data.get("download_links") or {}
    raw = movie_data.get("download_filenames")
    if not isinstance(raw, dict):
        raise ValueError(
            "Movie `download_filenames` must be a JSON object with the same keys as `download_links`"
        )
    out = {}
    for q in download_links:
        ctx = f"Movie download_filenames[{q!r}]"
        if q not in raw:
            raise ValueError(
                f"{ctx}: missing key — must supply one basename per `download_links` key "
                f"(expected keys: {list(download_links.keys())!r})"
            )
        out[q] = validate_llm_download_basename(raw.get(q), context=ctx)
    extra = set(raw.keys()) - set(download_links.keys())
    if extra:
        logger.warning("Movie download_filenames: ignoring extra keys not in download_links: %s", sorted(extra))
    movie_data["download_filenames"] = out
    return out


def process_movie_pipeline(media_task, movie_data, dup_info=None):
    """
    Movie pipeline: LLM `download_filenames` (validated) -> parallel download + subtitle clean ->
    optional keyframe screenshots -> parallel Drive upload -> FlixBD publish.
    Skips qualities already on Drive; duplicate update only downloads missing_resolutions.
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

    # Screenshots: keyframes on first publish; duplicate **update** keeps merged URLs (see _merge_drive_links)
    is_dup_update = bool(dup_info and dup_info.get("action") == "update")
    if not is_dup_update:
        movie_data.pop("screen_shots_url", None)

    # Step 2: Require download_filenames from main extract (no server-side synthesis)
    logger.info(f"Validating download_filenames for movie: {title}")
    try:
        filenames = _movie_download_filenames_from_llm(movie_data)
    except ValueError as e:
        msg = str(e)
        logger.error("Movie pipeline: %s", msg)
        save_task(media_task, status="failed", error_message=msg, result=movie_data)
        return json.dumps({"status": "error", "message": msg})

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

    def _download_and_clean(quality, urls, fname):
        """Download + subtitle clean only (runs in thread)."""
        from upload.service.flixbd_client import format_file_size
        url_list = urls if isinstance(urls, list) else [urls]

        file_path = None
        for url in url_list:
            file_path = Downloader.download(url, fname, sub_folder=safe_title)
            if file_path:
                break

        if not file_path:
            logger.warning(f"Could not download {quality}")
            return quality, None, None

        try:
            raw_size = os.path.getsize(file_path)
            size_str = format_file_size(raw_size)
            logger.debug(f"File size for {quality}: {size_str}")
        except OSError:
            size_str = None

        logger.info(f"Cleaning subtitles for {quality}")
        cleaned = process_downloaded_files({quality: file_path})
        file_path = cleaned.get(quality, file_path)
        return quality, file_path, size_str

    def _upload_and_delete(quality, file_path):
        """Upload to Drive + delete local (runs in thread)."""
        if not file_path or not os.path.exists(file_path):
            return quality, None
        logger.info(f"Uploading {quality} to Drive")
        try:
            thread_service = DriveUploader._get_drive_service()
            link = DriveUploader._upload_file(thread_service, file_path, movie_folder_id)
        except Exception as e:
            logger.error(f"Upload failed for {quality}: {e}")
            link = None
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Removed local file: {file_path}")
        return quality, link

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
            _publish_to_flixbd_movie(
                media_task,
                movie_data,
                drive_links,
                file_sizes,
                dup_info=dup_info,
            )
            return json.dumps({"status": "success", "type": "movie", "data": movie_data})

        logger.warning(f"No valid download links found for: {title}")
        save_task(media_task, status='failed', error_message='No valid download links found (after link resolution)')
        return json.dumps({"status": "error", "message": "No valid links found"})

    # Step 4a: Parallel download + subtitle clean
    logger.info(f"Starting parallel download+clean: {[q for q, _, _ in to_process]}")
    with ThreadPoolExecutor(max_workers=len(to_process)) as executor:
        futures = {
            executor.submit(_download_and_clean, q, u, f): q
            for q, u, f in to_process
        }
        results = []
        for future in as_completed(futures):
            results.append(future.result())

    paths_ok = [(q, p, s) for q, p, s in results if p]
    if paths_ok and not is_dup_update:
        _, best_path, _ = max(paths_ok, key=lambda x: os.path.getsize(x[1]))
        ss_urls = capture_screenshots_for_publish(best_path, f"{safe_title}-ss")
        if ss_urls:
            movie_data["screen_shots_url"] = ss_urls
            save_task(media_task, result=movie_data)
            logger.info(f"Set {len(ss_urls)} screenshot URL(s) from largest local file")
        else:
            logger.warning(
                "No screen_shots_url for %s — keyframes failed, screenshots disabled, "
                "or Telegram/Worker settings incomplete (check logs above).",
                title,
            )
    elif paths_ok and is_dup_update:
        logger.info("Duplicate update: skipping screenshot capture (keeping existing screen_shots_url)")

    # Step 4b: Parallel upload + delete
    logger.info(f"Starting parallel upload: {[q for q, _, _ in to_process]}")
    with ThreadPoolExecutor(max_workers=len(to_process)) as executor:
        futures = {
            executor.submit(_upload_and_delete, q, p): q
            for q, p, s in results
            if p
        }

        for future in as_completed(futures):
            quality, link = future.result()
            size_str = next((s for q, p, s in results if q == quality), None)
            if link:
                drive_links[quality] = link
                if size_str:
                    file_sizes[quality] = size_str
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
    _publish_to_flixbd_movie(media_task, movie_data, drive_links, file_sizes, dup_info=dup_info)

    return json.dumps({"status": "success", "type": "movie", "data": movie_data})


def _publish_to_flixbd_movie(media_task, movie_data, drive_links, file_sizes, dup_info=None):
    """
    Add Drive links to FlixBD after upload completes.

    **Duplicate / update path:** ``process_media_task`` sets ``site_content_id`` from the
    LLM duplicate_check target site row id or an existing DB row. Then this function runs ``patch_movie_title`` so
    ``website_movie_title`` from the **latest** LLM merge hits FlixBD. If
    ``site_content_id`` is still null, we **create** a new movie (no PATCH) — title
    will not update an existing row.

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

        if cid:
            logger.info(f"FlixBD: existing row id={cid} — PATCH title then add links")
            fx.patch_movie_title(int(cid), movie_data)
            content_id = int(cid)
            if dup_info and dup_info.get("clear_flixbd_links"):
                n = fx.clear_movie_download_links(content_id)
                logger.info(
                    "FlixBD: replace — cleared %s existing download row(s) for movie id=%s",
                    n,
                    content_id,
                )
        else:
            logger.warning(
                "FlixBD: no site_content_id on task pk=%s — POST create_movie (title will be set on new row only; "
                "existing FlixBD row will NOT get a title update)",
                getattr(media_task, "pk", None),
            )
            content_id = fx.create_movie(movie_data)

        # Add download links with actual file sizes
        fx.add_movie_download_links(
            content_id=content_id,
            drive_links=drive_links,
            file_sizes=file_sizes,
            movie_data=movie_data,
        )

        if not media_task.site_content_id:
            media_task.site_content_id = content_id
            media_task.save(update_fields=["site_content_id", "updated_at"])
        logger.info(f"FlixBD: movie done -- site_content_id={content_id} clean_title='{title}'")

    except Exception as e:
        logger.error(f"FlixBD publish failed for movie '{title}': {e}", exc_info=True)

