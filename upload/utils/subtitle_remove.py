"""
Subtitle track remover using FFmpeg (subprocess).
Removes tracks when metadata matches blocklists on any subtitle, or when dialogue
in the playback-default (else first) subtitle matches. Uses stream copy where possible.
"""

import os
import re
import json
import tempfile
import subprocess
import logging
from typing import Optional

import pysubs2

from llm.schema.blocked_names import BLOCKED_SITE_NAMES, SITE_NAME

from .ffmpeg_low_priority import low_priority_cmd

logger = logging.getLogger(__name__)

# First N subtitle events (per stream) scanned for blocklisted names in dialogue text.
SUBTITLE_CONTENT_SCAN_MAX_EVENTS = 30

# Bitmap / image-based subs: FFmpeg cannot emit text for cheap scan; rely on metadata only.
_SUBTITLE_CODECS_SKIP_TEXT_SCAN = frozenset(
    {
        "hdmv_pgs_subtitle",
        "dvd_subtitle",
        "dvb_subtitle",
        "xsub",
    }
)

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


# Only fields used by remux / metadata logic (smaller probe than full format+streams).
_FFPROBE_SUBTITLE_ENTRIES = (
    "format_tags=title:stream=index,codec_type,codec_name,disposition,tags"
)


def _get_stream_info(input_path: str) -> dict:
    """
    Uses ffprobe to fetch stream and container title metadata as JSON.
    """
    cmd = low_priority_cmd(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_entries",
            _FFPROBE_SUBTITLE_ENTRIES,
            input_path,
        ]
    )
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            logger.error("ffprobe failed: %s", (result.stderr or "")[:800])
            return {}
        data = json.loads(result.stdout)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError, subprocess.TimeoutExpired) as e:
        logger.error("ffprobe error: %s", e)
        return {}


def _extract_subtitle_to_tempfile(input_path: str, stream_index: int) -> Optional[str]:
    """
    Demux one subtitle stream to a temporary text-based file for parsing.
    Returns path to delete on success, or None if demux/conversion failed (e.g. bitmap subs).
    """
    for suffix, extra in (
        (".srt", ["-c:s", "srt", "-f", "srt"]),
        (".ass", ["-c:s", "ass", "-f", "ass"]),
    ):
        fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        cmd = low_priority_cmd(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                input_path,
                "-map",
                f"0:{stream_index}",
                *extra,
                tmp_path,
            ]
        )
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120
            )
            if result.returncode == 0 and os.path.getsize(tmp_path) > 0:
                return tmp_path
        except (subprocess.TimeoutExpired, OSError) as e:
            logger.debug(
                "Subtitle demux attempt failed stream=%s suffix=%s: %s",
                stream_index,
                suffix,
                e,
            )
        try:
            os.remove(tmp_path)
        except OSError:
            pass
    return None


def _subtitle_preview_lower_blob(temp_path: str, max_events: int) -> str:
    """Lowercase text blob from the first ``max_events`` subtitle cues (plaintext)."""
    try:
        try:
            subs = pysubs2.load(temp_path, encoding="utf-8")
        except UnicodeDecodeError:
            subs = pysubs2.load(temp_path, encoding="latin-1")
    except Exception as e:
        logger.debug("pysubs2 parse failed for %s: %s", os.path.basename(temp_path), e)
        return ""

    parts: list[str] = []
    for i, line in enumerate(subs):
        if i >= max_events:
            break
        plain = getattr(line, "plaintext", None)
        if plain is None:
            plain = (line.text or "").replace("\\N", " ").replace("\\n", " ")
        t = (plain or "").strip()
        if t:
            parts.append(t)
    return " ".join(parts).casefold()


def _subtitle_body_has_blocked_substring(blob_lower: str) -> bool:
    """Fast substring scan (``blob_lower`` must already be casefolded)."""
    if not blob_lower:
        return False
    return any(blocked in blob_lower for blocked in BLOCKED_SUBTITLE_NAMES)


def _subtitle_stream_dialogue_suggests_removal(
    input_path: str, stream_index: int, max_events: int
) -> bool:
    tmp = _extract_subtitle_to_tempfile(input_path, stream_index)
    if not tmp:
        return False
    try:
        blob = _subtitle_preview_lower_blob(tmp, max_events)
        return _subtitle_body_has_blocked_substring(blob)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def _select_playback_subtitle_stream(subtitle_streams: list[dict]) -> dict | None:
    """
    Subtitle stream most players enable first: ``disposition.default``, else the
    first subtitle in ffprobe order (same idea as browser "subtitle #1").
    """
    if not subtitle_streams:
        return None
    for s in subtitle_streams:
        disp = s.get("disposition") or {}
        if disp.get("default") in (1, True, "1"):
            return s
    return subtitle_streams[0]


def remove_subtitles(input_path: str) -> Optional[str]:
    """
    Removes subtitle tracks when any track's metadata matches the blocklist, or when
    the **playback-default** subtitle (else first subtitle) has blocklisted text in
    the first ``SUBTITLE_CONTENT_SCAN_MAX_EVENTS`` dialogue cues.

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

        # Step 2: Subtitle removal — metadata on every track; dialogue scan on playback default only
        subtitle_streams = [s for s in streams if s.get("codec_type") == "subtitle"]

        streams_to_remove: set[int] = set()
        for stream in subtitle_streams:
            stream_index = stream.get("index")
            if stream_index is None:
                continue
            title = stream.get("tags", {}).get("title", "").lower()
            handler = stream.get("tags", {}).get("handler_name", "").lower()
            check_text = f"{title} {handler}"

            for blocked in BLOCKED_SUBTITLE_NAMES:
                if blocked in check_text:
                    streams_to_remove.add(stream_index)
                    logger.info(
                        "Marked for removal (metadata): Stream #%s title=%r",
                        stream_index,
                        (title or "")[:120],
                    )
                    break

        playback_sub = _select_playback_subtitle_stream(subtitle_streams)
        if playback_sub:
            pidx = playback_sub.get("index")
            if pidx is not None and pidx not in streams_to_remove:
                codec_name = (playback_sub.get("codec_name") or "").strip().lower()
                if codec_name in _SUBTITLE_CODECS_SKIP_TEXT_SCAN:
                    logger.debug(
                        "Skipping dialogue scan for playback subtitle stream #%s (codec=%s)",
                        pidx,
                        codec_name or "?",
                    )
                elif _subtitle_stream_dialogue_suggests_removal(
                    input_path, pidx, SUBTITLE_CONTENT_SCAN_MAX_EVENTS
                ):
                    streams_to_remove.add(pidx)
                    logger.info(
                        "Marked for removal (dialogue on playback subtitle, first %s cues): Stream #%s",
                        SUBTITLE_CONTENT_SCAN_MAX_EVENTS,
                        pidx,
                    )

        kept_streams = [s for s in streams if s.get("index") not in streams_to_remove]
        metadata_cleanup_needed = _needs_metadata_cleanup(input_path, probe_data, kept_streams)

        if not streams_to_remove and not metadata_cleanup_needed:
            logger.info("No blocked subtitle tracks or metadata cleanup needed. File is clean.")
            return input_path

        # Step 3: Build FFmpeg command to remux without blocked streams
        # Get file extension
        _, ext = os.path.splitext(input_path)
        output_path = input_path + f".clean{ext}"

        cmd = low_priority_cmd(
            [
                "ffmpeg",
                "-i",
                input_path,
                "-map",
                "0",  # Map all streams first
            ]
        )

        # Add negative mappings for blocked subtitle streams
        for idx in sorted(streams_to_remove):
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
