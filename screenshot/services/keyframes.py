"""
Movie / video screenshots: ffmpeg extracts JPEG frames; Pillow re-compresses so the **bundle
total stays ≤ 5 MiB** (simple quality + optional resize loop).

FFmpeg: fast seek with ``-ss`` before ``-i``, ``-vframes 1``, ``scale`` + ``-q:v`` (mjpeg).
Pillow: ``JPEG`` save with ``quality`` + ``optimize=True`` until under cap.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from io import BytesIO
from pathlib import Path

from PIL import Image

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


def _ffmpeg_jpeg_frame(
    video_path: str,
    t_sec: float,
    out_path: str,
    *,
    max_width: int = 1280,
    q_v: int = 6,
) -> bool:
    """One JPEG still; ``-ss`` before ``-i`` for faster seek."""
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
        f"scale='min({max_width},iw)':-1",
        "-q:v",
        str(max(2, min(q_v, 31))),
        "-y",
        out_path,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if r.returncode != 0:
            logger.warning("ffmpeg jpeg frame failed: %s", r.stderr[:300])
            return False
        return os.path.isfile(out_path) and os.path.getsize(out_path) > 0
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        logger.error("ffmpeg error: %s", e)
        return False


def _bundle_bytes(images_rgb: list[Image.Image], quality: int, scale: float) -> tuple[int, list[bytes]]:
    """Encode all frames at given JPEG quality and uniform scale; return (total_size, blobs)."""
    chunks: list[bytes] = []
    total = 0
    q = max(5, min(int(quality), 95))

    for im in images_rgb:
        w, h = im.size
        if scale < 0.999:
            nw = max(320, int(w * scale))
            nh = max(180, int(h * scale))
            rim = im.resize((nw, nh), Image.Resampling.LANCZOS)
        else:
            rim = im

        buf = BytesIO()
        rim.save(buf, format="JPEG", quality=q, optimize=True)
        b = buf.getvalue()
        chunks.append(b)
        total += len(b)

    return total, chunks


def _pil_shrink_bundle_under_cap(
    images_rgb: list[Image.Image],
    prefix: str,
    max_total_bytes: int,
) -> list[Path]:
    """
    Re-encode JPEGs until sum(sizes) <= max_total_bytes.
    Lowers ``quality`` first, then scales down (Pillow), same as typical image-compression flow.
    """
    cap = min(max_total_bytes, MAX_TOTAL_BYTES_DEFAULT)
    quality = 82
    scale = 1.0

    while True:
        total, blobs = _bundle_bytes(images_rgb, quality, scale)
        if total <= cap:
            keep = tempfile.mkdtemp(prefix="ss_jpg_")
            out_paths: list[Path] = []
            for i, blob in enumerate(blobs):
                p = Path(keep) / f"{prefix}_{i:02d}.jpg"
                p.write_bytes(blob)
                out_paths.append(p)
            logger.debug(
                "Screenshot JPEG bundle: %d bytes (cap %d) q=%d scale=%.2f",
                total,
                cap,
                quality,
                scale,
            )
            return out_paths

        quality -= 8
        if quality < 28:
            quality = 78
            scale *= 0.88
        if scale < 0.28:
            # Last resort: smallest scale + low quality (still JPEG)
            scale = 0.25
            quality = 22
            total, blobs = _bundle_bytes(images_rgb, quality, scale)
            keep = tempfile.mkdtemp(prefix="ss_jpg_")
            for i, blob in enumerate(blobs):
                p = Path(keep) / f"{prefix}_{i:02d}.jpg"
                p.write_bytes(blob)
            logger.warning(
                "Screenshot bundle still %d bytes (cap %d); using aggressive q=%d scale=%.2f",
                total,
                cap,
                quality,
                scale,
            )
            return [Path(keep) / f"{prefix}_{i:02d}.jpg" for i in range(len(blobs))]


def extract_keyframes_webp(
    video_path: str,
    *,
    name_prefix: str = "kf",
    num_frames: int = NUM_FRAMES_DEFAULT,
    max_total_bytes: int = MAX_TOTAL_BYTES_DEFAULT,
) -> list[Path]:
    """
    Extract ``num_frames`` JPEG screenshots, then Pillow-compress so **total size ≤ 5 MiB**.

    Name kept for callers; output files are ``.jpg`` (not WebP).
    """
    if not video_path or not os.path.isfile(video_path):
        return []

    duration = _ffprobe_duration(video_path)
    if duration <= 0.5:
        logger.warning("Duration too short or unknown: %s", video_path)
        return []

    prefix = _safe_prefix(name_prefix)
    tmp = tempfile.mkdtemp(prefix="ss_raw_")
    try:
        times = [duration * (i + 0.5) / num_frames for i in range(num_frames)]
        raw_paths: list[Path] = []
        for i, t in enumerate(times):
            outp = os.path.join(tmp, f"{prefix}_{i:02d}.jpg")
            if not _ffmpeg_jpeg_frame(video_path, t, outp):
                logger.warning("Failed extracting frame at t=%.2fs", t)
                return []
            raw_paths.append(Path(outp))

        images: list[Image.Image] = []
        try:
            for p in raw_paths:
                im = Image.open(p).convert("RGB")
                im.load()
                images.append(im)

            # Fast path: ffmpeg output already under cap
            raw_total = sum(p.stat().st_size for p in raw_paths)
            cap = min(max_total_bytes, MAX_TOTAL_BYTES_DEFAULT)
            if raw_total <= cap:
                keep = tempfile.mkdtemp(prefix="ss_jpg_")
                kept: list[Path] = []
                for p in raw_paths:
                    dest = Path(keep) / p.name
                    shutil.copy2(p, dest)
                    kept.append(dest)
                shutil.rmtree(tmp, ignore_errors=True)
                tmp = None  # outer finally skips cleanup
                return kept

            return _pil_shrink_bundle_under_cap(images, prefix, cap)
        finally:
            for im in images:
                try:
                    im.close()
                except Exception:
                    pass
    finally:
        if tmp is not None and os.path.isdir(tmp):
            shutil.rmtree(tmp, ignore_errors=True)

    return []
