import os
import subprocess
import logging
from typing import Optional
from django.conf import settings
from .aria2c_config import build_aria2c_command, DOWNLOAD_TIMEOUT

logger = logging.getLogger(__name__)


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

        expected_file_path = os.path.join(download_dir, filename)

        try:
            logger.info(f"Starting download: {filename}")
            
            # Build command from centralized config
            cmd = build_aria2c_command(url, download_dir, filename)

            # Run aria2c
            result = subprocess.run(
                cmd, 
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.PIPE, 
                text=True, 
                timeout=DOWNLOAD_TIMEOUT
            )

            if result.returncode != 0:
                logger.error(f"aria2c error for {filename}: {result.stderr}")
            
            # Verify download
            if os.path.exists(expected_file_path) and os.path.getsize(expected_file_path) > 0:
                logger.info(f"Download finished: {filename} ({os.path.getsize(expected_file_path) / (1024*1024):.1f} MB)")
                return expected_file_path
            else:
                logger.error(f"Download failed or empty file: {filename}")
                Downloader._cleanup(expected_file_path)
                return None

        except subprocess.TimeoutExpired:
            logger.error(f"Download timed out: {filename}")
            Downloader._cleanup(expected_file_path)
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading {filename}: {e}")
            Downloader._cleanup(expected_file_path)
            return None

    @staticmethod
    def download_all_movie(download_links: dict, filenames: dict, main_title: str) -> dict:
        """
        Download all available qualities for a movie.
        Returns: {quality: file_path, ...}
        """
        safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in main_title).strip()

        results = {}
        for quality in ["480p", "720p", "1080p"]:
            urls = download_links.get(quality)
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
                for quality in ["480p", "720p", "1080p"]:
                    urls = resolutions.get(quality)
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
