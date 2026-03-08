"""
Subtitle track remover using FFmpeg (subprocess).
Removes subtitle tracks that contain specific site names.
Works with MKV, MP4, AVI, and all FFmpeg-supported formats.
Uses stream copy (no re-encoding) for maximum speed.
"""

import os
import json
import subprocess
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Site names to filter out from subtitle track names
BLOCKED_SUBTITLE_NAMES = [
    "cinefreak",
    "mlsbd",
    "cinemaza",
    "mkvking",
    "hdmovie99",
    "moviesmod",
    "vegamovies",
    "katmoviehd",
    "extramovies",
    "filmyzilla",
    "bolly4u",
    "themoviesflix",
    "movieverse",
    "torrent",
    "yts",
    "rarbg",
    "yify",
]


def _get_stream_info(input_path: str) -> dict:
    """
    Uses ffprobe to get all stream info as JSON.
    """
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        input_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        logger.error(f"ffprobe failed: {result.stderr}")
        return {}
    return json.loads(result.stdout)


def remove_subtitles(input_path: str) -> Optional[str]:
    """
    Removes subtitle tracks from a video file whose track title contains
    any of the BLOCKED_SUBTITLE_NAMES (case-insensitive).

    Works with MKV, MP4, and any FFmpeg-supported format.
    Uses stream copy (no re-encoding) for maximum performance.
    Overwrites the original file with the cleaned version.
    Returns the output path if successful, None otherwise.
    """
    if not os.path.exists(input_path):
        logger.error(f"File not found: {input_path}")
        return None

    try:
        # Step 1: Get stream info via ffprobe
        logger.info(f"Analyzing streams: {os.path.basename(input_path)}")
        probe_data = _get_stream_info(input_path)
        streams = probe_data.get("streams", [])

        if not streams:
            logger.warning("No streams found in file.")
            return input_path

        # Step 2: Find subtitle streams to remove
        subtitle_streams = [s for s in streams if s.get("codec_type") == "subtitle"]

        if not subtitle_streams:
            logger.info("No subtitle tracks found in file.")
            return input_path

        streams_to_remove = []
        for stream in subtitle_streams:
            stream_index = stream.get("index")
            # Check title tag
            title = stream.get("tags", {}).get("title", "").lower()
            # Also check handler_name for MP4
            handler = stream.get("tags", {}).get("handler_name", "").lower()

            check_text = f"{title} {handler}"

            for blocked in BLOCKED_SUBTITLE_NAMES:
                if blocked in check_text:
                    streams_to_remove.append(stream_index)
                    logger.info(f"Marked for removal: Stream #{stream_index} (title: '{title}')")
                    break

        if not streams_to_remove:
            logger.info("No blocked subtitle tracks found. File is clean.")
            return input_path

        # Step 3: Build FFmpeg command to remux without blocked streams
        # Get file extension
        _, ext = os.path.splitext(input_path)
        output_path = input_path + f".clean{ext}"

        cmd = [
            "ffmpeg",
            "-i", input_path,
            "-map", "0",           # Map all streams first
        ]

        # Add negative mappings for blocked subtitle streams
        for idx in streams_to_remove:
            cmd.extend(["-map", f"-0:{idx}"])

        cmd.extend([
            "-c", "copy",          # No re-encoding
            "-y",                  # Overwrite output
            output_path
        ])

        logger.info(f"Removing {len(streams_to_remove)} subtitle stream(s)...")
        result = subprocess.run(
            cmd, 
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.PIPE, 
            text=True, 
            timeout=600
        )

        if result.returncode != 0:
            logger.error(f"FFmpeg remux failed: {result.stderr[:500]}")
            if os.path.exists(output_path):
                os.remove(output_path)
            return None

        # Step 4: Replace original with cleaned version
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            os.remove(input_path)
            os.rename(output_path, input_path)
            logger.info(f"Successfully cleaned subtitles from: {os.path.basename(input_path)}")
            return input_path
        else:
            logger.error("Remuxed file is empty or not created.")
            if os.path.exists(output_path):
                os.remove(output_path)
            return None

    except FileNotFoundError:
        logger.error("FFmpeg not found! Please install FFmpeg and add it to PATH.")
        return None
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg timed out.")
        return None
    except Exception as e:
        logger.error(f"Error removing subtitles: {e}", exc_info=True)
        return None


def process_downloaded_files(downloaded_files: dict) -> dict:
    """
    Processes all downloaded files and removes blocked subtitles.
    Returns dict of quality -> cleaned file path.
    """
    cleaned = {}
    for quality, file_path in downloaded_files.items():
        if file_path and os.path.exists(file_path):
            logger.info(f"Processing {quality}: {os.path.basename(file_path)}")
            result = remove_subtitles(file_path)
            if result:
                cleaned[quality] = result
            else:
                cleaned[quality] = file_path  # Keep original if cleaning fails
    return cleaned
