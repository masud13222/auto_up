"""
Extract N evenly spaced WebP keyframes from a video; total size capped (default 5 MiB).
Uses ffmpeg / ffprobe (same as subtitle pipeline).
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

MAX_TOTAL_BYTES_DEFAULT = 5 * 1024 * 1024
NUM_FRAMES_DEFAULT = 6


def _ffprobe_duration(path: str) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        logger.error("ffprobe failed: %s", r.stderr[:500])
        return 0.0
    try:
        fmt = json.loads(r.stdout).get("format", {})
        return float(fmt.get("duration", 0) or 0)
    except (TypeError, ValueError, json.JSONDecodeError):
        return 0.0


def _safe_prefix(name: str) -> str:
    return re.sub(r"[^\w.\-]+", "_", name)[:80] or "frame"


def _extract_one_webp(
    video_path: str,
    t_sec: float,
    out_path: str,
    quality: int,
) -> bool:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{t_sec:.3f}",
        "-i",
        video_path,
        "-an",
        "-sn",
        "-dn",
        "-vframes",
        "1",
        "-vf",
        "scale='min(1280,iw)':-1",
        "-c:v",
        "libwebp",
        "-quality",
        str(quality),
        "-y",
        out_path,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if r.returncode != 0:
            logger.warning("ffmpeg webp frame failed: %s", r.stderr[:300])
            return False
        return os.path.isfile(out_path) and os.path.getsize(out_path) > 0
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        logger.error("ffmpeg error: %s", e)
        return False


def extract_keyframes_webp(
    video_path: str,
    *,
    name_prefix: str = "kf",
    num_frames: int = NUM_FRAMES_DEFAULT,
    max_total_bytes: int = MAX_TOTAL_BYTES_DEFAULT,
) -> list[Path]:
    """
    Write num_frames WebP files into a temp directory; return paths sorted by index.
    Tries lowering WebP quality until total size <= max_total_bytes (or quality floor).
    """
    if not video_path or not os.path.isfile(video_path):
        return []

    duration = _ffprobe_duration(video_path)
    if duration <= 0.5:
        logger.warning("Duration too short or unknown: %s", video_path)
        return []

    prefix = _safe_prefix(name_prefix)
    tmp = tempfile.mkdtemp(prefix="ss_kf_")
    try:
        times = [duration * (i + 0.5) / num_frames for i in range(num_frames)]
        quality = 82
        floor = 28
        paths: list[Path] = []

        while quality >= floor:
            paths.clear()
            ok_all = True
            for i, t in enumerate(times):
                outp = os.path.join(tmp, f"{prefix}_{i:02d}.webp")
                if not _extract_one_webp(video_path, t, outp, quality):
                    ok_all = False
                    break
                paths.append(Path(outp))

            if not ok_all:
                quality -= 8
                continue

            total = sum(p.stat().st_size for p in paths)
            if total <= max_total_bytes:
                # Copy to new temp dir so caller can delete our work dir but keep files
                keep = tempfile.mkdtemp(prefix="ss_kf_keep_")
                kept: list[Path] = []
                for p in paths:
                    dest = Path(keep) / p.name
                    shutil.copy2(p, dest)
                    kept.append(dest)
                shutil.rmtree(tmp, ignore_errors=True)
                return kept

            logger.debug("Keyframe bundle %d bytes > cap at q=%d, lowering", total, quality)
            quality -= 7

        logger.warning("Could not fit keyframes under %s bytes", max_total_bytes)
        return []
    finally:
        if os.path.isdir(tmp):
            shutil.rmtree(tmp, ignore_errors=True)
