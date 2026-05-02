"""
Utility to delete files from Google Drive by their drive links.
Used during 'replace' action to clean up old files before re-downloading.
"""
import re
import logging
from upload.service.uploader import DriveUploader

logger = logging.getLogger(__name__)


def extract_file_id(drive_link: str) -> str | None:
    """
    Extract Google Drive file ID from various link formats.
    Supports:
      - https://drive.google.com/file/d/FILE_ID/view
      - https://drive.google.com/open?id=FILE_ID
      - https://drive.google.com/uc?id=FILE_ID
    """
    if not drive_link or not isinstance(drive_link, str):
        return None

    # /file/d/FILE_ID/
    match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', drive_link)
    if match:
        return match.group(1)

    # ?id=FILE_ID
    match = re.search(r'[?&]id=([a-zA-Z0-9_-]+)', drive_link)
    if match:
        return match.group(1)

    return None


def delete_drive_file(service, file_id: str) -> bool:
    """Delete a single file from Google Drive. Returns True on success."""
    try:
        service.files().delete(
            fileId=file_id,
            supportsAllDrives=True,
        ).execute()
        logger.info(f"Deleted Drive file: {file_id}")
        return True
    except Exception as e:
        logger.warning(f"Failed to delete Drive file {file_id}: {e}")
        return False


def cleanup_old_drive_files(existing_result: dict) -> int:
    """
    Delete all Drive files from an existing task result.
    Used during 'replace' action to remove old quality files.

    Handles both movie and tvshow formats:
      - Movie: result.download_links = {res: [{u, l, f}, ...]}
      - TV Show: result.seasons[].download_items[].resolutions = {res: [{u, l, f}, ...]}

    Returns count of deleted files.
    """
    from upload.utils.media_entry_helpers import is_drive_link

    # Collect all drive file IDs
    file_ids = set()

    # Movie: download_links
    for res, entries in existing_result.get("download_links", {}).items():
        for entry in entries if isinstance(entries, list) else []:
            drive_link = str((entry or {}).get("u") or "").strip()
            if is_drive_link(drive_link):
                fid = extract_file_id(drive_link)
                if fid:
                    file_ids.add(fid)
                    logger.debug(f"Found old movie file: {res} [{entry.get('language')}] → {fid}")

    # TV Show: seasons → download_items → resolutions
    for season in existing_result.get("seasons", []):
        snum = season.get("season_number", "?")
        for item in season.get("download_items", []):
            label = item.get("label", "")
            for res, entries in item.get("resolutions", {}).items():
                for entry in entries if isinstance(entries, list) else []:
                    drive_link = str((entry or {}).get("u") or "").strip()
                    if is_drive_link(drive_link):
                        fid = extract_file_id(drive_link)
                        if fid:
                            file_ids.add(fid)
                            logger.debug(
                                f"Found old tvshow file: S{snum} {label} {res} [{entry.get('language')}] → {fid}"
                            )

    if not file_ids:
        logger.info("No old Drive files to clean up.")
        return 0

    logger.info(f"Cleaning up {len(file_ids)} old Drive file(s)...")

    # Delete files
    service = DriveUploader._get_drive_service()
    deleted = 0
    for fid in file_ids:
        if delete_drive_file(service, fid):
            deleted += 1

    logger.info(f"Drive cleanup complete: {deleted}/{len(file_ids)} files deleted.")
    return deleted
