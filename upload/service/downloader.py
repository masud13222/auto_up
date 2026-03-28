import os
import subprocess
import logging
import threading
from typing import Optional
from django.conf import settings
from .aria2c_config import build_aria2c_command, DOWNLOAD_TIMEOUT

logger = logging.getLogger(__name__)
_DOWNLOAD_NAME_LOCK = threading.Lock()
_RESERVED_DOWNLOAD_PATHS: set[str] = set()


class Downloader:
    """
    Downloads files using aria2c with parallel connections.
    """

    @staticmethod
    def download(url: str, filename: str, sub_folder: str = "") -> Optional[str]:
        """
        Download a single file using aria2c.
        Returns the file path if successful, None otherwise.
        """
        # Build download directory
        download_dir = os.path.join(settings.DOWNLOADS_DIR, sub_folder) if sub_folder else str(settings.DOWNLOADS_DIR)
        os.makedirs(download_dir, exist_ok=True)

        expected_file_path = Downloader._reserve_download_path(download_dir, filename)

        try:
            logger.info(f"Starting download: {os.path.basename(expected_file_path)}")
            
            # Build command from centralized config
            cmd = build_aria2c_command(url, download_dir, os.path.basename(expected_file_path))

            # Run aria2c
            result = subprocess.run(
                cmd, 
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.PIPE, 
                text=True, 
                timeout=DOWNLOAD_TIMEOUT
            )

            if result.returncode != 0:
                logger.error(f"aria2c error for {os.path.basename(expected_file_path)}: {result.stderr}")
            
            # Verify download
            if os.path.exists(expected_file_path) and os.path.getsize(expected_file_path) > 0:
                logger.info(
                    f"Download finished: {os.path.basename(expected_file_path)} "
                    f"({os.path.getsize(expected_file_path) / (1024*1024):.1f} MB)"
                )
                return expected_file_path
            else:
                logger.error(f"Download failed or empty file: {os.path.basename(expected_file_path)}")
                Downloader._cleanup(expected_file_path)
                return None

        except subprocess.TimeoutExpired:
            logger.error(f"Download timed out: {os.path.basename(expected_file_path)}")
            Downloader._cleanup(expected_file_path)
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading {os.path.basename(expected_file_path)}: {e}")
            Downloader._cleanup(expected_file_path)
            return None
        finally:
            Downloader._release_reserved_path(expected_file_path)

    @staticmethod
    def download_all_movie(download_links: dict, filenames: dict, main_title: str) -> dict:
        """
        Download all available qualities for a movie.
        Returns: {quality: file_path, ...}
        """
        safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in main_title).strip()

        results = {}
        for quality, urls in download_links.items():
            fname = filenames.get(quality)

            if not urls or not fname:
                continue

            url_list = urls if isinstance(urls, list) else [urls]

            for url in url_list:
                file_path = Downloader.download(url, fname, sub_folder=safe_title)
                if file_path:
                    results[quality] = file_path
                    break

            if quality not in results:
                logger.warning(f"Could not download quality: {quality}")

        return results

    @staticmethod
    def download_all_tvshow(seasons: list, filenames: list, main_title: str) -> list:
        """
        Download all available qualities for a TV show (season-wise).
        
        Args:
            seasons: List of season dicts with download_items (from tvshow_schema)
            filenames: List of filename dicts with season_number, type, label, resolutions
            main_title: Show title for sub-folder naming
            
        Returns: List of dicts:
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
        safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in main_title).strip()

        results = []
        for season in seasons:
            season_num = season.get("season_number")
            download_items = season.get("download_items", [])

            for item in download_items:
                item_type = item.get("type")
                item_label = item.get("label", "Unknown")
                resolutions = item.get("resolutions", {})

                # Find matching filename entry by season_number + type + label
                fname_item = next(
                    (f for f in filenames
                     if f.get("season_number") == season_num
                     and f.get("type") == item_type
                     and f.get("label") == item_label),
                    {}
                )
                fname_resolutions = fname_item.get("resolutions", {})

                downloaded_resolutions = {}
                for quality, urls in resolutions.items():
                    fname = fname_resolutions.get(quality)

                    if not urls or not fname:
                        continue

                    url_list = urls if isinstance(urls, list) else [urls]
                    for url in url_list:
                        file_path = Downloader.download(url, fname, sub_folder=safe_title)
                        if file_path:
                            downloaded_resolutions[quality] = file_path
                            break

                    if quality not in downloaded_resolutions:
                        logger.warning(f"Could not download {quality} for S{season_num} {item_label}")

                if downloaded_resolutions:
                    results.append({
                        "season_number": season_num,
                        "type": item_type,
                        "label": item_label,
                        "resolutions": downloaded_resolutions
                    })

        return results

    @staticmethod
    def _cleanup(file_path: str):
        """Remove partial/empty files."""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Cleaned up: {file_path}")
        except Exception:
            pass

    @staticmethod
    def _reserve_download_path(download_dir: str, filename: str) -> str:
        """
        Reserve a collision-free local path for a download.
        Uses `name (1).ext` style to prevent parallel overwrite in the same folder.
        """
        stem, ext = os.path.splitext(filename)
        attempt = 0
        with _DOWNLOAD_NAME_LOCK:
            while True:
                candidate_name = filename if attempt == 0 else f"{stem} ({attempt}){ext}"
                candidate_path = os.path.join(download_dir, candidate_name)
                reservation_key = os.path.normcase(os.path.abspath(candidate_path))
                if reservation_key not in _RESERVED_DOWNLOAD_PATHS and not os.path.exists(candidate_path):
                    _RESERVED_DOWNLOAD_PATHS.add(reservation_key)
                    return candidate_path
                attempt += 1

    @staticmethod
    def _release_reserved_path(file_path: str) -> None:
        with _DOWNLOAD_NAME_LOCK:
            _RESERVED_DOWNLOAD_PATHS.discard(os.path.normcase(os.path.abspath(file_path)))
