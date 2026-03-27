import os
import json
import random
import logging
import threading
import time
from datetime import datetime, timezone
from django.core.files.base import ContentFile
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

from settings.models import GoogleConfig, UploadSettings
from credentials.services import GoogleAuthService

logger = logging.getLogger(__name__)

# Retry on these HTTP status codes during resumable upload
RETRYABLE_STATUS_CODES = {308, 429, 500, 502, 503, 504}

# Thread-safe token refresh locks (per config ID)
_refresh_locks = {}
_refresh_locks_lock = threading.Lock()


def _get_refresh_lock(config_id):
    """Get or create a per-config threading lock for token refresh."""
    with _refresh_locks_lock:
        if config_id not in _refresh_locks:
            _refresh_locks[config_id] = threading.Lock()
        return _refresh_locks[config_id]


class DriveUploader:

    @staticmethod
    def _get_random_config_id():
        pk_list = list(GoogleConfig.objects.values_list('pk', flat=True))
        if not pk_list:
            raise Exception("No GoogleConfig found. Please add a token.json in admin.")
        return random.choice(pk_list)

    @staticmethod
    def _load_token_data(config):
        """Load token JSON from config file (with seek to start)."""
        config.config_file.seek(0)  # Always read from beginning
        raw = config.config_file.read()
        config.config_file.close()
        return json.loads(raw.decode('utf-8'))

    @staticmethod
    def _is_token_expired(token_data: dict) -> bool:
        """Check if token has expired."""
        expiry_str = token_data.get('expiry')
        if not expiry_str:
            return True
        try:
            expiry = datetime.fromisoformat(expiry_str)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) >= expiry
        except Exception:
            return True

    @staticmethod
    def _refresh_and_save(config, token_data: dict) -> dict:
        """Refresh OAuth token and save back to DB."""
        try:
            refreshed = GoogleAuthService.refresh_access_token(
                refresh_token=token_data['refresh_token'],
                client_id=token_data['client_id'],
                client_secret=token_data['client_secret'],
                token_uri=token_data.get('token_uri', 'https://oauth2.googleapis.com/token')
            )
            token_data['token'] = refreshed['token']
            token_data['expiry'] = refreshed['expiry']

            updated_bytes = json.dumps(token_data, indent=2).encode('utf-8')
            config.config_file.save(
                os.path.basename(config.config_file.name),
                ContentFile(updated_bytes),
                save=True
            )
            logger.info(f"Token refreshed for config '{config.name}'.")
            return token_data

        except Exception as e:
            logger.error(f"Token refresh failed for '{config.name}': {e}", exc_info=True)
            raise Exception(f"Token refresh failed: {e}")

    @staticmethod
    def _build_credentials(token_data: dict) -> Credentials:
        return Credentials(
            token=token_data.get('token'),
            refresh_token=token_data.get('refresh_token'),
            token_uri=token_data.get('token_uri', 'https://oauth2.googleapis.com/token'),
            client_id=token_data.get('client_id'),
            client_secret=token_data.get('client_secret'),
        )

    @staticmethod
    def _get_credentials() -> Credentials:
        """
        Random config pick → token load → thread-safe refresh if needed → Credentials.
        Uses per-config locks to prevent concurrent refresh of the same token.
        """
        config_id = DriveUploader._get_random_config_id()
        # Thread-safe token refresh with per-config lock
        with _get_refresh_lock(config_id):
            # Re-fetch inside the lock so concurrent refresh/save cannot leave us
            # with a stale FileField path for the token JSON.
            config = GoogleConfig.objects.get(pk=config_id)
            logger.info(f"Using Google Config: {config.name} (ID: {config.pk})")
            token_data = DriveUploader._load_token_data(config)

            if DriveUploader._is_token_expired(token_data) or not token_data.get('token'):
                logger.info("Token expired or missing. Refreshing...")
                token_data = DriveUploader._refresh_and_save(config, token_data)

        return DriveUploader._build_credentials(token_data)

    @staticmethod
    def _get_drive_service():
        """Build a NEW Drive service (call per-thread for thread safety)."""
        creds = DriveUploader._get_credentials()
        return build('drive', 'v3', credentials=creds)

    @staticmethod
    def _get_or_create_folder(service, folder_name: str, parent_id: str) -> str:
        """
        Get existing folder or create new one.
        Handles race condition: if two threads try to create the same folder,
        the second one catches the conflict and returns the existing folder.
        """
        # Escape special chars for Drive query
        safe_name = folder_name.replace("\\", "\\\\").replace("'", "\\'")
        query = (
            f"name='{safe_name}' and '{parent_id}' in parents "
            f"and mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        results = service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1
        ).execute()

        existing = results.get('files', [])
        if existing:
            folder_id = existing[0]['id']
            logger.info(f"Folder exists: '{folder_name}' ({folder_id})")
            return folder_id

        try:
            folder = service.files().create(
                body={
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [parent_id]
                },
                fields='id',
                supportsAllDrives=True
            ).execute()

            folder_id = folder['id']
            logger.info(f"Created folder: '{folder_name}' ({folder_id})")
            return folder_id

        except HttpError as e:
            # Race condition: another thread already created this folder
            if e.resp.status in (409, 400):
                logger.warning(f"Folder creation conflict for '{folder_name}', re-querying...")
                results = service.files().list(
                    q=query,
                    fields="files(id)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    pageSize=1
                ).execute()
                existing = results.get('files', [])
                if existing:
                    return existing[0]['id']
            raise

    @staticmethod
    def _upload_file(service, file_path: str, folder_id: str, max_retries: int = 5) -> str:
        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        logger.info(f"Uploading: {filename} ({file_size / (1024 * 1024):.1f} MB)")

        media = MediaFileUpload(
            file_path,
            resumable=True,
            chunksize=20 * 1024 * 1024
        )

        request = service.files().create(
            body={'name': filename, 'parents': [folder_id]},
            media_body=media,
            fields='id, webViewLink',
            supportsAllDrives=True
        )

        response = None
        retry_count = 0
        last_logged_progress = -1

        while response is None:
            try:
                status, response = request.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    # Log only every 20% to reduce log noise
                    if progress // 20 > last_logged_progress // 20:
                        logger.info(f"  {filename}: {progress}%")
                        last_logged_progress = progress
                retry_count = 0  # Successful chunk → retry counter reset

            except HttpError as e:
                if e.resp.status in (401, 403):
                    logger.error(f"Auth/permission error uploading '{filename}': {e}")
                    raise  # Caller retries with fresh service
                if e.resp.status not in RETRYABLE_STATUS_CODES:
                    raise

                retry_count += 1
                if retry_count > max_retries:
                    raise Exception(f"Upload failed after {max_retries} retries: {filename}")

                wait = min(2 ** retry_count, 60)
                logger.warning(f"HTTP {e.resp.status} — retry {retry_count}/{max_retries} in {wait}s")
                time.sleep(wait)

            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    raise Exception(f"Upload failed after {max_retries} retries: {filename}")
                wait = min(2 ** retry_count, 60)
                logger.warning(f"Chunk error (retry {retry_count}/{max_retries}) in {wait}s: {e}")
                time.sleep(wait)

        file_id = response.get('id')
        web_link = response.get('webViewLink') or f"https://drive.google.com/file/d/{file_id}/view"

        # Public read permission (with retry — 503 transient failures are common)
        perm_retries = 3
        for perm_attempt in range(perm_retries + 1):
            try:
                service.permissions().create(
                    fileId=file_id,
                    body={'type': 'anyone', 'role': 'reader'},
                    supportsAllDrives=True
                ).execute()
                break
            except HttpError as e:
                if e.resp.status in RETRYABLE_STATUS_CODES and perm_attempt < perm_retries:
                    wait = min(2 ** (perm_attempt + 1), 30)
                    logger.warning(
                        f"Permission set failed (HTTP {e.resp.status}), "
                        f"retry {perm_attempt + 1}/{perm_retries} in {wait}s: {filename}"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"Permission set failed for {filename} (file_id={file_id}): {e}")
                    raise

        logger.info(f"Done: {filename} → {web_link}")
        return web_link

    @staticmethod
    def upload_file_with_retry(file_path: str, folder_id: str, max_attempts: int = 3) -> str:
        """
        Full upload retry wrapper:
        - rebuild Drive service on each attempt
        - retry credential/service creation failures
        - retry upload failures that occur before or during resumable upload
        """
        filename = os.path.basename(file_path)
        last_error = None

        for attempt in range(1, max_attempts + 1):
            try:
                service = DriveUploader._get_drive_service()
                return DriveUploader._upload_file(service, file_path, folder_id)
            except Exception as e:
                last_error = e
                if attempt >= max_attempts:
                    break
                wait = min(2 ** attempt, 15)
                logger.warning(
                    "Upload attempt %s/%s failed for %s; retrying in %ss: %s",
                    attempt,
                    max_attempts,
                    filename,
                    wait,
                    e,
                )
                time.sleep(wait)

        raise Exception(f"Upload failed after {max_attempts} attempts: {filename}: {last_error}")

    @staticmethod
    def upload_movie(movie_data: dict, downloaded_files: dict) -> dict:
        """
        Upload movie files to Google Drive.
        
        Args:
            movie_data: Movie info dict
            downloaded_files: {quality: file_path, ...}
        """
        upload_settings = UploadSettings.objects.first()
        if not upload_settings:
            raise Exception("UploadSettings not configured. Please set upload_folder_id in admin.")

        parent_folder_id = upload_settings.upload_folder_id
        title = movie_data.get("title", "Unknown")
        year = movie_data.get("year", "")
        folder_name = f"{title} {year}" if year else title

        service = DriveUploader._get_drive_service()
        movie_folder_id = DriveUploader._get_or_create_folder(service, folder_name, parent_folder_id)

        drive_links = {}
        for quality, file_path in downloaded_files.items():
            if not file_path or not os.path.exists(file_path):
                logger.warning(f"Skipping '{quality}': file not found at {file_path}")
                continue
            try:
                drive_links[quality] = DriveUploader._upload_file(service, file_path, movie_folder_id)
            except Exception as e:
                logger.error(f"Upload failed for quality '{quality}': {e}", exc_info=True)
                drive_links[quality] = None  # Failed — caller decides retry

        if drive_links:
            movie_data["download_links"] = drive_links

        logger.info(f"Upload complete for '{folder_name}'. Qualities: {list(drive_links.keys())}")
        return movie_data

    @staticmethod
    def upload_tvshow(tvshow_data: dict, downloaded_items: list) -> dict:
        """
        Upload TV show files to Google Drive with season-wise folder structure.
        
        Args:
            tvshow_data: TV show info dict
            downloaded_items: List of dicts with season_number, type, label, resolutions
                [
                    {
                        "season_number": 1,
                        "type": "combo_pack",
                        "label": "Season 1 Combo Pack",
                        "resolutions": {"720p": "/path/to/file.mkv", ...}
                    },
                    ...
                ]
        """
        upload_settings = UploadSettings.objects.first()
        if not upload_settings:
            raise Exception("UploadSettings not configured. Please set upload_folder_id in admin.")

        parent_folder_id = upload_settings.upload_folder_id
        title = tvshow_data.get("title", "Unknown")
        year = tvshow_data.get("year", "")
        folder_name = f"{title} {year}" if year else title

        service = DriveUploader._get_drive_service()
        show_folder_id = DriveUploader._get_or_create_folder(service, folder_name, parent_folder_id)

        # Group downloaded items by season for organized upload
        season_folders = {}  # season_number -> folder_id

        uploaded_items = []
        for item in downloaded_items:
            season_num = item.get("season_number")
            item_type = item.get("type")
            item_label = item.get("label", "Unknown")
            resolutions = item.get("resolutions", {})

            # Create season sub-folder if not exists
            if season_num not in season_folders:
                season_folder_name = f"Season {season_num}"
                season_folders[season_num] = DriveUploader._get_or_create_folder(
                    service, season_folder_name, show_folder_id
                )

            season_folder_id = season_folders[season_num]

            uploaded_resolutions = {}
            for quality, file_path in resolutions.items():
                if not file_path or not os.path.exists(file_path):
                    logger.warning(f"Skipping '{quality}' for '{item_label}': file not found at {file_path}")
                    continue
                try:
                    uploaded_resolutions[quality] = DriveUploader._upload_file(
                        service, file_path, season_folder_id
                    )
                except Exception as e:
                    logger.error(f"Upload failed for '{item_label}' quality '{quality}': {e}", exc_info=True)
                    uploaded_resolutions[quality] = None  # Failed — caller decides retry

            if uploaded_resolutions:
                uploaded_items.append({
                    "season_number": season_num,
                    "type": item_type,
                    "label": item_label,
                    "resolutions": uploaded_resolutions
                })

        # Update tvshow_data seasons with drive links
        if uploaded_items:
            for season in tvshow_data.get("seasons", []):
                season_num = season.get("season_number")
                for item in season.get("download_items", []):
                    # Find matching uploaded item
                    uploaded = next(
                        (u for u in uploaded_items
                         if u.get("season_number") == season_num
                         and u.get("type") == item.get("type")
                         and u.get("label") == item.get("label")),
                        None
                    )
                    if uploaded:
                        item["resolutions"] = uploaded["resolutions"]

        logger.info(f"Upload complete for TV Show '{folder_name}'. Uploaded {len(uploaded_items)} item(s).")
        return tvshow_data