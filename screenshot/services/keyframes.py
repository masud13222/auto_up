"""
Extract N evenly-spaced JPEG screenshots from a video, total ≤ 5 MiB.

Strategy (simple, works on x264 AND HEVC):
  1. ffmpeg: ``-i video -ss t`` (accurate seek AFTER input — reliable on all codecs)
     → single PNG frame per timestamp (no mjpeg encoder headaches)
  2. Pillow: open PNG → save as JPEG, then shrink bundle until total ≤ 5 MiB.
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
EXTRACT_MAX_WIDTH = 854


def _ffprobe_duration(path: str) -> float:
    cmd = [
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_format", path,
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


def _extract_png_frame(video_path: str, t_sec: float, out_png: str) -> bool:
    """
    One PNG frame via accurate seek (``-ss`` after ``-i``).
    Slower than keyframe seek but works reliably on HEVC / limited-range YUV / all MKV.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-threads", "1",
        "-i", video_path,
        "-ss", f"{t_sec:.3f}",
        "-an", "-sn", "-dn",
        "-vframes", "1",
        "-vf", f"scale='min({EXTRACT_MAX_WIDTH},iw)':-1",
        "-c:v", "png",
        "-y", out_png,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if r.returncode != 0:
            logger.warning(
                "ffmpeg PNG frame failed (rc=%d, t=%.2fs): %s",
                r.returncode, t_sec, r.stderr[:500],
            )
            return False
        if not os.path.isfile(out_png) or os.path.getsize(out_png) == 0:
            logger.warning("ffmpeg produced empty/no PNG at t=%.2fs", t_sec)
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.warning("ffmpeg frame timed out at t=%.2fs", t_sec)
        return False
    except FileNotFoundError:
        logger.error("ffmpeg not found in PATH")
        return False


def _png_to_jpeg(png_path: str, jpg_path: str, quality: int = 90) -> bool:
    """Pillow: PNG → JPEG."""
    try:
        with Image.open(png_path) as im:
            im.convert("RGB").save(jpg_path, format="JPEG", quality=quality, optimize=True)
        return os.path.isfile(jpg_path) and os.path.getsize(jpg_path) > 0
    except Exception as e:
        logger.warning("Pillow PNG→JPEG failed: %s", e)
        return False


def _shrink_jpegs_under_cap(
    images: list[Image.Image],
    prefix: str,
    cap: int,
) -> list[Path]:
    """Re-encode all frames to JPEG, lowering quality/scale until total ≤ cap."""
    quality = 82
    scale = 1.0

    while True:
        blobs: list[bytes] = []
        for im in images:
            w, h = im.size
            if scale < 0.999:
                rim = im.resize(
                    (max(320, int(w * scale)), max(180, int(h * scale))),
                    Image.Resampling.LANCZOS,
                )
            else:
                rim = im
            buf = BytesIO()
            rim.save(buf, format="JPEG", quality=max(5, quality), optimize=True)
            blobs.append(buf.getvalue())

        total = sum(len(b) for b in blobs)
        if total <= cap:
            keep = tempfile.mkdtemp(prefix="ss_jpg_")
            paths: list[Path] = []
            for i, blob in enumerate(blobs):
                p = Path(keep) / f"{prefix}_{i:02d}.jpg"
                p.write_bytes(blob)
                paths.append(p)
            logger.debug("JPEG bundle: %d bytes (cap %d) q=%d scale=%.2f", total, cap, quality, scale)
            return paths

        quality -= 8
        if quality < 28:
            quality = 78
            scale *= 0.85
        if scale < 0.25:
            keep = tempfile.mkdtemp(prefix="ss_jpg_")
            paths = []
            for i, blob in enumerate(blobs):
                p = Path(keep) / f"{prefix}_{i:02d}.jpg"
                p.write_bytes(blob)
                paths.append(p)
            logger.warning("JPEG bundle %d bytes > cap %d; forced q=%d scale=%.2f", total, cap, quality, scale)
            return paths


def extract_keyframes_webp(
    video_path: str,
    *,
    name_prefix: str = "kf",
    num_frames: int = NUM_FRAMES_DEFAULT,
    max_total_bytes: int = MAX_TOTAL_BYTES_DEFAULT,
) -> list[Path]:
    """
    Extract ``num_frames`` screenshots, compress to JPEG, total ≤ 5 MiB.
    Function name kept for callers; output is ``.jpg``.
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

        # Phase 1: extract PNG frames (accurate seek)
        jpg_paths: list[Path] = []
        for i, t in enumerate(times):
            png_p = os.path.join(tmp, f"{prefix}_{i:02d}.png")
            jpg_p = os.path.join(tmp, f"{prefix}_{i:02d}.jpg")

            ok = _extract_png_frame(video_path, t, png_p)
            if not ok:
                for offset in (2.0, -2.0, 5.0, -5.0):
                    retry_t = max(0.5, min(t + offset, duration - 0.5))
                    if retry_t == t:
                        continue
                    logger.info("Retrying frame %d at t=%.2fs (offset %+.1f)", i, retry_t, offset)
                    ok = _extract_png_frame(video_path, retry_t, png_p)
                    if ok:
                        break

            if not ok:
                logger.warning("Skipping frame %d (all seeks failed around t=%.2fs)", i, t)
                continue

            if _png_to_jpeg(png_p, jpg_p):
                jpg_paths.append(Path(jpg_p))
            try:
                os.unlink(png_p)
            except OSError:
                pass

        if not jpg_paths:
            logger.warning("All frame extractions failed for %s", video_path)
            return []

        logger.info("Extracted %d/%d frames from %s", len(jpg_paths), num_frames, os.path.basename(video_path))

        # Phase 2: check total size, shrink if needed
        cap = min(max_total_bytes, MAX_TOTAL_BYTES_DEFAULT)
        raw_total = sum(p.stat().st_size for p in jpg_paths)
        if raw_total <= cap:
            keep = tempfile.mkdtemp(prefix="ss_jpg_")
            kept: list[Path] = []
            for p in jpg_paths:
                dest = Path(keep) / p.name
                shutil.copy2(p, dest)
                kept.append(dest)
            shutil.rmtree(tmp, ignore_errors=True)
            tmp = None
            return kept

        # Pillow re-compress
        images: list[Image.Image] = []
        try:
            for p in jpg_paths:
                im = Image.open(p).convert("RGB")
                im.load()
                images.append(im)
            return _shrink_jpegs_under_cap(images, prefix, cap)
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
