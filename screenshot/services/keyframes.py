"""
Extract N evenly-spaced JPEG screenshots from a video, total ≤ 5 MiB.

Practices aligned with FFmpeg / community guidance:

- **Hybrid seek** (fast + accurate): ``-ss T1 -i input -ss T2`` with ``T2 = t - T1`` and
  ``T1`` a few seconds before ``t`` jumps near a keyframe, then decodes only a short span
  to the exact frame — same accuracy as ``-i``-then-``-ss`` on long files, much faster
  (widely recommended on Stack Overflow / encoding guides; FFmpeg wiki on seeking is the
  conceptual reference).
- **``-ss`` before ``-i`` alone** is fast; **after ``-i``** is slow but robust when hybrid
  or fast seek fails (e.g. some HEVC/MKV edge cases on Reddit / Super User).
- **``-fflags +genpts``** helps missing/broken timestamps (common HEVC/mkv symptom).
- **``-nostdin``** avoids blocking on stdin when run under subprocess (FFmpeg docs).
- **``format=yuv420p``** in ``-vf`` before mjpeg avoids “non full-range YUV” encoder errors.
- **``-c:v mjpeg``** + ``-q:v`` makes the JPEG path explicit.
- **Hardware decode**: ``SCREENSHOT_FFMPEG_HWACCEL`` — ``auto`` (try ``-hwaccel auto`` then CPU),
  ``none`` / ``cpu``, or an explicit value (``cuda``, ``vaapi``, ``qsv``, ``videotoolbox``, …).
- **Extra demuxer flags**: ``SCREENSHOT_FFMPEG_EXTRA_FFLAGS`` — space-separated tokens appended
  to ``-fflags`` (e.g. ``+discardcorrupt`` for damaged streams; use sparingly).
- **PNG + Pillow** only if all JPEG paths fail.

Pillow is used only to enforce the **5 MiB** total bundle cap (re-encode / scale).

``thumbnail`` + ``-frames:v N`` in one pass does **not** evenly sample a long file (one
thumbnail per decoded window from the start). Evenly spaced ``-ss`` times keep full-length
coverage.
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
from typing import Literal

from PIL import Image

logger = logging.getLogger(__name__)

MAX_TOTAL_BYTES_DEFAULT = 5 * 1024 * 1024
NUM_FRAMES_DEFAULT = 6
EXTRACT_MAX_WIDTH = 854
# mjpeg quality 2–31 (lower = better). ~3 is high; Pillow may shrink further for 5 MiB cap.
FFMPEG_JPEG_Q = 3
# Seconds before target time for hybrid fast jump; bounds decode work after keyframe seek.
HYBRID_LEAD_SEC = 12.0
# Minimum T1 for hybrid (skip hybrid when seek is already near start).
HYBRID_MIN_T_FAST = 1.0


def _env_hwaccel_raw() -> str:
    return os.environ.get("SCREENSHOT_FFMPEG_HWACCEL", "auto").strip().lower()


def _env_extra_fflags() -> str:
    """Optional extra input flags, e.g. ``+discardcorrupt`` (space-separated +flags)."""
    return os.environ.get("SCREENSHOT_FFMPEG_EXTRA_FFLAGS", "").strip()


def _fflags_value() -> str:
    parts = ["+genpts"]
    extra = _env_extra_fflags()
    if extra:
        for token in extra.split():
            t = token.strip()
            if t and t not in parts:
                parts.append(t if t.startswith("+") else f"+{t.lstrip('+')}")
    return "".join(parts)


def _decoder_attempts() -> list[tuple[str | None, bool]]:
    """
    (hwaccel value for ``-hwaccel``, use_sw_threads).

    When using HW, omit ``-threads 0`` (let device decode); CPU fallback uses all cores.
    """
    raw = _env_hwaccel_raw()
    if raw in ("", "none", "off", "0", "false", "cpu"):
        return [(None, True)]
    if raw == "auto":
        return [("auto", False), (None, True)]
    return [(raw, False), (None, True)]


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


def _ffprobe_video_codec(path: str) -> str:
    cmd = [
        "ffprobe", "-v", "quiet",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-print_format", "json",
        path,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            streams = json.loads(r.stdout).get("streams", [])
            if streams:
                return streams[0].get("codec_name", "")
    except Exception:
        pass
    return ""


def _safe_prefix(name: str) -> str:
    return re.sub(r"[^\w.\-]+", "_", name)[:80] or "frame"


def _hybrid_seek_pair(t_sec: float, duration: float) -> tuple[float, float] | None:
    """Return (ss_before_i, ss_after_i) or None if hybrid is not worthwhile."""
    t = max(0.05, min(t_sec, duration - 0.05))
    margin = min(HYBRID_LEAD_SEC, t)
    t_fast = max(0.0, t - margin)
    t_rem = t - t_fast
    if t_fast < HYBRID_MIN_T_FAST or t_rem <= 0.001:
        return None
    return t_fast, t_rem


def _lead(hwaccel: str | None, sw_threads: bool) -> list[str]:
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "warning"]
    if hwaccel:
        cmd.extend(["-hwaccel", hwaccel])
    if sw_threads:
        cmd.extend(["-threads", "0"])
    cmd.extend(["-fflags", _fflags_value()])
    return cmd


SeekMode = Literal["hybrid", "fast", "accurate"]


def _seek_input_args(
    video_path: str,
    t_sec: float,
    mode: SeekMode,
    hybrid: tuple[float, float] | None,
) -> list[str]:
    if mode == "fast":
        return ["-ss", f"{t_sec:.3f}", "-i", video_path]
    if mode == "accurate":
        return ["-i", video_path, "-ss", f"{t_sec:.3f}"]
    if mode == "hybrid" and hybrid is not None:
        t_fast, t_rem = hybrid
        return ["-ss", f"{t_fast:.3f}", "-i", video_path, "-ss", f"{t_rem:.3f}"]
    raise ValueError(f"hybrid mode requires pair, got {hybrid!r}")


def _video_filter_jpeg() -> str:
    return (
        f"format=yuv420p,"
        f"scale='min({EXTRACT_MAX_WIDTH},iw)':-2:flags=lanczos"
    )


def _run_ffmpeg_jpeg(
    video_path: str,
    t_sec: float,
    out_jpg: str,
    *,
    duration: float,
    mode: SeekMode,
    hwaccel: str | None,
    sw_threads: bool,
) -> tuple[bool, str]:
    hybrid = _hybrid_seek_pair(t_sec, duration) if mode == "hybrid" else None
    if mode == "hybrid" and hybrid is None:
        return False, "hybrid not applicable"
    try:
        mid = _seek_input_args(video_path, t_sec, mode, hybrid)
    except ValueError as e:
        return False, str(e)

    vf = _video_filter_jpeg()
    tail = [
        "-an", "-sn", "-dn",
        "-frames:v", "1",
        "-vf", vf,
        "-c:v", "mjpeg",
        "-q:v", str(FFMPEG_JPEG_Q),
        "-y", out_jpg,
    ]
    cmd = _lead(hwaccel, sw_threads) + mid + tail

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        stderr = (r.stderr or "").strip()
        if r.returncode != 0:
            return False, f"rc={r.returncode} {stderr[:800]}"
        if not os.path.isfile(out_jpg) or os.path.getsize(out_jpg) == 0:
            return False, f"rc=0 empty out. stderr={stderr[:800]}"
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except FileNotFoundError:
        return False, "ffmpeg not found"


def _run_ffmpeg_png_fallback(
    video_path: str,
    t_sec: float,
    out_png: str,
    *,
    duration: float,
    mode: SeekMode,
    hwaccel: str | None,
    sw_threads: bool,
) -> tuple[bool, str]:
    hybrid = _hybrid_seek_pair(t_sec, duration) if mode == "hybrid" else None
    if mode == "hybrid" and hybrid is None:
        return False, "hybrid not applicable"
    try:
        mid = _seek_input_args(video_path, t_sec, mode, hybrid)
    except ValueError as e:
        return False, str(e)

    vf = f"format=rgb24,scale='min({EXTRACT_MAX_WIDTH},iw)':-2:flags=lanczos"
    tail = [
        "-an", "-sn", "-dn",
        "-frames:v", "1",
        "-vf", vf,
        "-c:v", "png",
        "-y", out_png,
    ]
    cmd = _lead(hwaccel, sw_threads) + mid + tail
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        stderr = (r.stderr or "").strip()
        if r.returncode != 0:
            return False, f"rc={r.returncode} {stderr[:800]}"
        if not os.path.isfile(out_png) or os.path.getsize(out_png) == 0:
            return False, f"rc=0 empty. stderr={stderr[:800]}"
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except FileNotFoundError:
        return False, "ffmpeg not found"


def _jpeg_seek_modes(duration: float, t_sec: float) -> list[SeekMode]:
    modes: list[SeekMode] = []
    if _hybrid_seek_pair(t_sec, duration) is not None:
        modes.append("hybrid")
    modes.extend(["fast", "accurate"])
    return modes


def _extract_one_frame_jpeg(
    video_path: str,
    t_sec: float,
    out_jpg: str,
    *,
    duration: float,
) -> bool:
    for mode in _jpeg_seek_modes(duration, t_sec):
        for hwaccel, sw_threads in _decoder_attempts():
            ok, err = _run_ffmpeg_jpeg(
                video_path, t_sec, out_jpg,
                duration=duration, mode=mode, hwaccel=hwaccel, sw_threads=sw_threads,
            )
            if ok:
                return True
            logger.debug(
                "ffmpeg JPEG mode=%s hw=%s t=%.2fs: %s",
                mode, hwaccel or "cpu", t_sec, err,
            )

    tmp_png = out_jpg + ".fallback.png"
    for mode in _jpeg_seek_modes(duration, t_sec):
        for hwaccel, sw_threads in _decoder_attempts():
            ok, err = _run_ffmpeg_png_fallback(
                video_path, t_sec, tmp_png,
                duration=duration, mode=mode, hwaccel=hwaccel, sw_threads=sw_threads,
            )
            if not ok:
                logger.debug(
                    "ffmpeg PNG mode=%s hw=%s t=%.2fs: %s",
                    mode, hwaccel or "cpu", t_sec, err,
                )
                continue
            try:
                with Image.open(tmp_png) as im:
                    im.convert("RGB").save(
                        out_jpg, format="JPEG", quality=92, optimize=True,
                    )
                if os.path.isfile(out_jpg) and os.path.getsize(out_jpg) > 0:
                    return True
            except Exception as e:
                logger.warning("Pillow PNG→JPEG fallback failed: %s", e)
            finally:
                try:
                    os.unlink(tmp_png)
                except OSError:
                    pass

    logger.warning("All ffmpeg paths failed at t=%.2fs for JPEG output", t_sec)
    return False


def _shrink_jpegs_under_cap(
    images: list[Image.Image],
    prefix: str,
    cap: int,
) -> list[Path]:
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
    Extract ``num_frames`` JPEG screenshots; total size ≤ 5 MiB after optional Pillow pass.
    Name kept for callers.
    """
    if not video_path or not os.path.isfile(video_path):
        return []

    duration = _ffprobe_duration(video_path)
    if duration <= 0.5:
        logger.warning("Duration too short or unknown: %s", video_path)
        return []

    codec = _ffprobe_video_codec(video_path)
    logger.info(
        "Screenshot: %s | codec=%s | duration=%.1fs | hybrid_lead=%.0fs | genpts mjpeg | hw=%s",
        os.path.basename(video_path),
        codec or "unknown",
        duration,
        HYBRID_LEAD_SEC,
        _env_hwaccel_raw() or "auto",
    )

    prefix = _safe_prefix(name_prefix)
    tmp = tempfile.mkdtemp(prefix="ss_raw_")
    try:
        times = [duration * (i + 0.5) / num_frames for i in range(num_frames)]

        jpg_paths: list[Path] = []
        for i, t in enumerate(times):
            jpg_p = os.path.join(tmp, f"{prefix}_{i:02d}.jpg")

            ok = _extract_one_frame_jpeg(video_path, t, jpg_p, duration=duration)
            if not ok:
                for offset in (3.0, -3.0, 8.0, -8.0, 15.0):
                    retry_t = max(0.5, min(t + offset, duration - 0.5))
                    if abs(retry_t - t) < 0.05:
                        continue
                    logger.info("Retrying frame %d at t=%.2fs (offset %+.1f)", i, retry_t, offset)
                    ok = _extract_one_frame_jpeg(video_path, retry_t, jpg_p, duration=duration)
                    if ok:
                        break

            if not ok:
                logger.warning("Skipping frame %d (all seeks failed around t=%.2fs)", i, t)
                continue

            jpg_paths.append(Path(jpg_p))

        if not jpg_paths:
            logger.warning("All frame extractions failed for %s (codec=%s)", video_path, codec)
            return []

        logger.info("Extracted %d/%d frames from %s", len(jpg_paths), num_frames, os.path.basename(video_path))

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
