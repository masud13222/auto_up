"""
Extract N JPEG screenshots from a video (random times in the middle of the timeline), total ≤ 5 MiB.

Practices aligned with FFmpeg / community guidance:

- **Hybrid seek**: ``-ss T1 -noaccurate_seek -i input -ss T2`` with ``T2 = t - T1`` and
  ``T1`` a few seconds before ``t``. ``-noaccurate_seek`` keeps the first jump on a **nearby
  keyframe** (fast); we never use **decode-to-exact-timestamp** seeks (``-i`` then ``-ss``) —
  screenshots do not need the mathematically exact frame.
- **Fast seek**: ``-ss t -noaccurate_seek -i`` — same keyframe-near behaviour when hybrid is
  not used (e.g. very early ``t``).
- **``-fflags +genpts``** helps missing/broken timestamps (common HEVC/mkv symptom).
- **``-nostdin``** avoids blocking on stdin when run under subprocess (FFmpeg docs).
- **``format=yuv420p``** in ``-vf`` before mjpeg avoids “non full-range YUV” encoder errors.
- **``-c:v mjpeg``** + ``-q:v`` makes the JPEG path explicit.
- **Hardware decode**: ``SCREENSHOT_FFMPEG_HWACCEL`` — ``auto`` (try ``-hwaccel auto`` then CPU),
  ``none`` / ``cpu``, or an explicit value (``cuda``, ``vaapi``, ``qsv``, ``videotoolbox``, …).
- **Extra demuxer flags**: ``SCREENSHOT_FFMPEG_EXTRA_FFLAGS`` — space-separated tokens appended
  to ``-fflags`` (e.g. ``+discardcorrupt`` for damaged streams; use sparingly).
- **PNG + Pillow** only if all JPEG paths fail.
- **Primary video stream**: Many MKVs put **cover art** as the first video stream
  (``attached_pic``). Decoding that stream at ``t=250s`` yields **no frames** — empty JPEG
  and exit code 0 matches FFmpeg’s “Output file is empty…” / stream-map guidance
  (https://trac.ffmpeg.org/wiki/Errors). We ``ffprobe`` all video streams, skip
  ``attached_pic``, skip **still-image** codecs (``png``, ``mjpeg``, …), then pick
  **largest width×height** among real video tracks and ``-map 0:<index>``.

Pillow is used only to enforce the **5 MiB** total bundle cap (re-encode / scale).

``thumbnail`` + ``-frames:v N`` in one pass does **not** evenly sample a long file (one
thumbnail per decoded window from the start).

**Sampling:** Timestamps are **random** inside the **middle** of the file (roughly 20%–80%
of duration; widened for very short videos) so grabs avoid opening titles and tail credits.
No Django settings — logic is code-only.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Literal

from PIL import Image

from upload.utils.ffmpeg_low_priority import low_priority_cmd

logger = logging.getLogger(__name__)

MAX_TOTAL_BYTES_DEFAULT = 5 * 1024 * 1024
NUM_FRAMES_DEFAULT = 6
EXTRACT_MAX_WIDTH = 854
# MKV often has multiple “video” tracks: mjpeg cover, png poster, then h264/hevc episode.
_STILL_IMAGE_CODECS = frozenset(
    {"png", "mjpeg", "jpegls", "ljpeg", "gif", "bmp", "webp", "tiff", "jpeg2000", "pam", "pbm", "pgm", "ppm"},
)
# mjpeg quality 2–31 (lower = better). ~3 is high; Pillow may shrink further for 5 MiB cap.
FFMPEG_JPEG_Q = 3
# Seconds before target time for hybrid fast jump; bounds decode work after keyframe seek.
HYBRID_LEAD_SEC = 12.0
# Minimum T1 for hybrid (skip hybrid when seek is already near start).
HYBRID_MIN_T_FAST = 1.0


def _random_sample_times_middle(duration: float, num_frames: int) -> list[float]:
    """
    Pick ``num_frames`` random seek times in the middle band of the file (skip start/end).
    Sorted ascending for deterministic ffmpeg order in logs.
    """
    if num_frames < 1:
        return []
    # Default band: 20%–80% of runtime (avoids logos/titles and late credits).
    t_lo = duration * 0.20
    t_hi = duration * 0.80
    if t_hi - t_lo < 8.0:
        t_lo = max(0.5, duration * 0.08)
        t_hi = max(t_lo + 2.0, duration * 0.92)
    span = t_hi - t_lo
    if span < 0.5:
        t_lo = max(0.25, duration * 0.05)
        t_hi = max(t_lo + 0.5, duration - 0.25)
        span = t_hi - t_lo
    times = sorted(random.uniform(t_lo, t_hi) for _ in range(num_frames))
    logger.info(
        "Screenshot middle-band random [%.1fs–%.1fs] of %.1fs: %s",
        t_lo,
        t_hi,
        duration,
        ", ".join(f"{t:.2f}s" for t in times),
    )
    return times


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

    **CPU first**, then HW: in Docker, ``-hwaccel auto`` can misbehave on some builds;
    cover-art / stream bugs are unrelated but CPU-first keeps logs cleaner.
    """
    raw = _env_hwaccel_raw()
    if raw in ("", "none", "off", "0", "false", "cpu"):
        return [(None, True)]
    if raw == "auto":
        return [(None, True), ("auto", False)]
    return [(None, True), (raw, False)]


# Single probe: duration + minimal stream fields (avoids two full demux passes).
_FFPROBE_KEYFRAMES_ENTRIES = (
    "format=duration:stream=index,codec_type,codec_name,width,height,disposition"
)


def _ffprobe_keyframes_payload(path: str) -> dict:
    cmd = low_priority_cmd(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_entries",
            _FFPROBE_KEYFRAMES_ENTRIES,
            path,
        ]
    )
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if r.returncode != 0:
            logger.error("ffprobe failed: %s", (r.stderr or "")[:500])
            return {}
        data = json.loads(r.stdout)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError, subprocess.TimeoutExpired) as e:
        logger.error("ffprobe error: %s", e)
        return {}


@dataclass(frozen=True)
class _PrimaryVideo:
    """Absolute stream index in file (for ``-map 0:N``), plus probe metadata."""

    stream_index: int
    codec_name: str
    width: int
    height: int


def _primary_video_from_streams(streams: list[dict]) -> _PrimaryVideo | None:
    """
    Pick the **episode** video stream from a probe ``streams`` list:

    1. Skip ``attached_pic``.
    2. Prefer codecs that are normal **moving** video (drop ``png`` / ``mjpeg`` / ``gif``
       etc. — often hi-res posters that are not the x264/hevc track).
    3. Among those, take largest ``width * height``.
    """
    if not isinstance(streams, list):
        return None

    videos: list[dict] = [
        s for s in streams if isinstance(s, dict) and s.get("codec_type") == "video"
    ]
    if not videos:
        return None

    def area(s: dict) -> int:
        try:
            return int(s.get("width", 0) or 0) * int(s.get("height", 0) or 0)
        except (TypeError, ValueError):
            return 0

    def is_attached(s: dict) -> bool:
        disp = s.get("disposition") or {}
        try:
            return int(disp.get("attached_pic", 0) or 0) == 1
        except (TypeError, ValueError):
            return False

    def codec_name(s: dict) -> str:
        return str(s.get("codec_name", "") or "").lower()

    def is_still_image_codec(s: dict) -> bool:
        return codec_name(s) in _STILL_IMAGE_CODECS

    candidates = [s for s in videos if not is_attached(s)]
    pool = candidates if candidates else videos
    movie_like = [s for s in pool if not is_still_image_codec(s)]
    if movie_like:
        pool = movie_like
    best = max(pool, key=area)
    if area(best) <= 0:
        best = max(videos, key=area)

    try:
        idx = int(best["index"])
    except (KeyError, TypeError, ValueError):
        return None

    return _PrimaryVideo(
        stream_index=idx,
        codec_name=str(best.get("codec_name", "") or ""),
        width=int(best.get("width", 0) or 0),
        height=int(best.get("height", 0) or 0),
    )


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
    return low_priority_cmd(cmd)


SeekMode = Literal["hybrid", "fast"]


def _seek_input_args(
    video_path: str,
    t_sec: float,
    mode: SeekMode,
    hybrid: tuple[float, float] | None,
) -> list[str]:
    if mode == "fast":
        return ["-ss", f"{t_sec:.3f}", "-noaccurate_seek", "-i", video_path]
    if mode == "hybrid" and hybrid is not None:
        t_fast, t_rem = hybrid
        return [
            "-ss",
            f"{t_fast:.3f}",
            "-noaccurate_seek",
            "-i",
            video_path,
            "-ss",
            f"{t_rem:.3f}",
        ]
    raise ValueError(f"hybrid mode requires pair, got {hybrid!r}")


def _video_filter_jpeg() -> str:
    return (
        f"format=yuv420p,"
        f"scale='min({EXTRACT_MAX_WIDTH},iw)':-2:flags=lanczos"
    )


def _map_args(map_0_index: int | None) -> list[str]:
    """``-map 0:N`` so we decode the feature track, not embedded cover (``v:0``)."""
    if map_0_index is None:
        return []
    return ["-map", f"0:{map_0_index}"]


def _run_ffmpeg_jpeg(
    video_path: str,
    t_sec: float,
    out_jpg: str,
    *,
    duration: float,
    mode: SeekMode,
    hwaccel: str | None,
    sw_threads: bool,
    video_stream_index: int | None,
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
    cmd = _lead(hwaccel, sw_threads) + mid + _map_args(video_stream_index) + tail

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
    video_stream_index: int | None,
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
    cmd = _lead(hwaccel, sw_threads) + mid + _map_args(video_stream_index) + tail
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
    """Keyframe-near seeks only: hybrid (preferred) then plain fast jump."""
    modes: list[SeekMode] = []
    if _hybrid_seek_pair(t_sec, duration) is not None:
        modes.append("hybrid")
    modes.append("fast")
    return modes


def _extract_one_frame_jpeg(
    video_path: str,
    t_sec: float,
    out_jpg: str,
    *,
    duration: float,
    video_stream_index: int | None,
) -> bool:
    last_err = ""
    for mode in _jpeg_seek_modes(duration, t_sec):
        for hwaccel, sw_threads in _decoder_attempts():
            ok, err = _run_ffmpeg_jpeg(
                video_path, t_sec, out_jpg,
                duration=duration, mode=mode, hwaccel=hwaccel, sw_threads=sw_threads,
                video_stream_index=video_stream_index,
            )
            if ok:
                return True
            last_err = err
            logger.debug(
                "ffmpeg JPEG mode=%s hw=%s map=0:%s t=%.2fs: %s",
                mode,
                hwaccel or "cpu",
                video_stream_index if video_stream_index is not None else "def",
                t_sec,
                err,
            )

    tmp_png = out_jpg + ".fallback.png"
    for mode in _jpeg_seek_modes(duration, t_sec):
        for hwaccel, sw_threads in _decoder_attempts():
            ok, err = _run_ffmpeg_png_fallback(
                video_path, t_sec, tmp_png,
                duration=duration, mode=mode, hwaccel=hwaccel, sw_threads=sw_threads,
                video_stream_index=video_stream_index,
            )
            if not ok:
                last_err = err
                logger.debug(
                    "ffmpeg PNG mode=%s hw=%s map=0:%s t=%.2fs: %s",
                    mode,
                    hwaccel or "cpu",
                    video_stream_index if video_stream_index is not None else "def",
                    t_sec,
                    err,
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

    logger.warning(
        "All ffmpeg paths failed at t=%.2fs (map=0:%s): %s",
        t_sec,
        video_stream_index if video_stream_index is not None else "default",
        (last_err or "no detail")[:1200],
    )
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
    Seek times are random within the middle segment of the video (see module docstring).
    """
    if not video_path or not os.path.isfile(video_path):
        return []

    probe = _ffprobe_keyframes_payload(video_path)
    fmt = probe.get("format") or {}
    try:
        duration = float(fmt.get("duration", 0) or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration <= 0.5:
        logger.warning("Duration too short or unknown: %s", video_path)
        return []

    primary = _primary_video_from_streams(probe.get("streams") or [])
    vidx = primary.stream_index if primary else None
    logger.info(
        "Screenshot: %s | video_stream=0:%s %s %dx%d | duration=%.1fs | hybrid=%.0fs | hw=%s",
        os.path.basename(video_path),
        vidx if vidx is not None else "?",
        primary.codec_name if primary else "?",
        primary.width if primary else 0,
        primary.height if primary else 0,
        duration,
        HYBRID_LEAD_SEC,
        _env_hwaccel_raw() or "auto",
    )

    prefix = _safe_prefix(name_prefix)
    tmp = tempfile.mkdtemp(prefix="ss_raw_")
    try:
        times = _random_sample_times_middle(duration, num_frames)

        jpg_paths: list[Path] = []
        for i, t in enumerate(times):
            jpg_p = os.path.join(tmp, f"{prefix}_{i:02d}.jpg")

            ok = _extract_one_frame_jpeg(
                video_path, t, jpg_p, duration=duration, video_stream_index=vidx,
            )
            if not ok:
                for offset in (3.0, -3.0, 8.0, -8.0, 15.0):
                    retry_t = max(0.5, min(t + offset, duration - 0.5))
                    if abs(retry_t - t) < 0.05:
                        continue
                    logger.info("Retrying frame %d at t=%.2fs (offset %+.1f)", i, retry_t, offset)
                    ok = _extract_one_frame_jpeg(
                        video_path, retry_t, jpg_p, duration=duration, video_stream_index=vidx,
                    )
                    if ok:
                        break

            if not ok:
                logger.warning("Skipping frame %d (all seeks failed around t=%.2fs)", i, t)
                continue

            jpg_paths.append(Path(jpg_p))

        if not jpg_paths:
            logger.warning(
                "All frame extractions failed for %s (stream 0:%s)",
                video_path,
                vidx if vidx is not None else "?",
            )
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
