"""
Subtitle track remover using FFmpeg (subprocess).
Removes subtitle tracks that contain specific site names.
Works with MKV, MP4, AVI, and all FFmpeg-supported formats.
Uses stream copy (no re-encoding) for maximum speed.
"""

import os
import re
import json
import subprocess
import logging
from typing import Optional

from llm.schema.blocked_names import BLOCKED_SITE_NAMES, SITE_NAME

logger = logging.getLogger(__name__)

_EXTRA_BLOCKED_MEDIA_NAMES = ["torrent", "yts", "rarbg", "yify"]
BLOCKED_SUBTITLE_NAMES = sorted(
    {*(name.lower() for name in BLOCKED_SITE_NAMES), *_EXTRA_BLOCKED_MEDIA_NAMES},
    key=len,
    reverse=True,
)
_BLOCKED_METADATA_NAMES = sorted(
    {*(name.lower() for name in BLOCKED_SITE_NAMES), *_EXTRA_BLOCKED_MEDIA_NAMES, SITE_NAME.lower()},
    key=len,
    reverse=True,
)


def _name_pattern(name: str) -> re.Pattern[str]:
    parts = re.findall(r"[a-z0-9]+", (name or "").lower())
    if not parts:
        return re.compile(r"$^")
    body = r"[\W_]*".join(re.escape(part) for part in parts)
    return re.compile(rf"(?<![a-z0-9]){body}(?![a-z0-9])", re.IGNORECASE)


_BLOCKED_PATTERNS = [_name_pattern(name) for name in _BLOCKED_METADATA_NAMES]


def _contains_blocked_name(text: str) -> bool:
    value = (text or "").strip()
    if not value:
        return False
    return any(pattern.search(value) for pattern in _BLOCKED_PATTERNS)


def _strip_site_names(text: str) -> str:
    value = text or ""
    for pattern in _BLOCKED_PATTERNS:
        value = pattern.sub(" ", value)
    value = re.sub(r"[\[\]\(\)\{\}_|]+", " ", value)
    value = re.sub(r"\s*[-:]+\s*", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -_.")


def _prettify_filename_title(input_path: str) -> str:
    stem = os.path.splitext(os.path.basename(input_path))[0]
    base = stem.replace(".", " ").replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()
    base = _strip_site_names(base)
    return base or stem


def _format_language_label(stream: dict) -> str:
    raw = (
        stream.get("tags", {}).get("language")
        or stream.get("tags", {}).get("LANGUAGE")
        or ""
    ).strip().lower()
    if not raw:
        return ""
    known = {
        "eng": "English",
        "en": "English",
        "hin": "Hindi",
        "hi": "Hindi",
        "ben": "Bangla",
        "bn": "Bangla",
        "bengali": "Bangla",
        "tam": "Tamil",
        "ta": "Tamil",
        "tel": "Telugu",
        "te": "Telugu",
    }
    return known.get(raw, raw.upper() if len(raw) <= 3 else raw.title())


def _is_attachment_stream(stream: dict) -> bool:
    disposition = stream.get("disposition", {}) or {}
    tags = stream.get("tags", {}) or {}
    return bool(
        disposition.get("attached_pic")
        or tags.get("filename")
        or tags.get("mimetype")
    )


def _desired_container_title(input_path: str) -> str:
    return f"{_prettify_filename_title(input_path)} - {SITE_NAME}"


def _desired_stream_title(stream: dict) -> str:
    codec_type = (stream.get("codec_type") or "").lower()
    if _is_attachment_stream(stream):
        return f"{SITE_NAME} - Cover"
    if codec_type == "video":
        return f"{SITE_NAME} - Video"
    if codec_type == "audio":
        lang = _format_language_label(stream)
        return f"{SITE_NAME} - Audio ({lang})" if lang else f"{SITE_NAME} - Audio"
    if codec_type == "subtitle":
        lang = _format_language_label(stream)
        return f"{SITE_NAME} - Subtitle ({lang})" if lang else f"{SITE_NAME} - Subtitle"
    return f"{SITE_NAME} - Stream"


def _needs_metadata_cleanup(input_path: str, probe_data: dict, kept_streams: list[dict]) -> bool:
    format_title = (probe_data.get("format", {}) or {}).get("tags", {}).get("title", "")
    if format_title != _desired_container_title(input_path):
        return True
    for stream in kept_streams:
        tags = stream.get("tags", {}) or {}
        desired = _desired_stream_title(stream)
        current_title = tags.get("title", "") or ""
        current_handler = tags.get("handler_name", "") or ""
        if _contains_blocked_name(current_title) or _contains_blocked_name(current_handler):
            return True
        if SITE_NAME.lower() not in current_title.lower():
            return True
        if current_title != desired:
            return True
    return False


def _get_stream_info(input_path: str) -> dict:
    """
    Uses ffprobe to get all stream info as JSON.
    """
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
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

        kept_streams = [s for s in streams if s.get("index") not in set(streams_to_remove)]
        metadata_cleanup_needed = _needs_metadata_cleanup(input_path, probe_data, kept_streams)

        if not streams_to_remove and not metadata_cleanup_needed:
            logger.info("No blocked subtitle tracks or metadata cleanup needed. File is clean.")
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

        cmd.extend(["-metadata", f"title={_desired_container_title(input_path)}"])

        type_counts = {"video": 0, "audio": 0, "subtitle": 0}
        for stream in kept_streams:
            codec_type = (stream.get("codec_type") or "").lower()
            if codec_type not in type_counts:
                continue
            if codec_type == "video":
                stream_spec = f"s:v:{type_counts['video']}"
                type_counts["video"] += 1
            elif codec_type == "audio":
                stream_spec = f"s:a:{type_counts['audio']}"
                type_counts["audio"] += 1
            else:
                stream_spec = f"s:s:{type_counts['subtitle']}"
                type_counts["subtitle"] += 1

            clean_title = _desired_stream_title(stream)
            cmd.extend(["-metadata:" + stream_spec, f"title={clean_title}"])
            cmd.extend(["-metadata:" + stream_spec, f"handler_name={clean_title}"])

        cmd.extend([
            "-c", "copy",          # No re-encoding
            "-y",                  # Overwrite output
            output_path
        ])

        logger.info(
            "Remuxing file: remove %s subtitle stream(s), metadata cleanup=%s",
            len(streams_to_remove),
            metadata_cleanup_needed,
        )
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
