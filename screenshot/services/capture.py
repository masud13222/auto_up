"""
Orchestrate keyframe extraction + Telegram upload + Worker URLs for FlixBD.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from screenshot.models import ScreenshotSettings
from screenshot.services import keyframes
from screenshot.services import telegram_client as tg

logger = logging.getLogger(__name__)


def capture_screenshots_for_publish(video_path: str, name_prefix: str) -> list[str]:
    """
    From the largest downloaded (cleaned) video: ffmpeg grabs JPEG frames, Pillow compresses
    so **all frames together ≤ 5 MiB**. Upload each via Telegram sendDocument, return Worker
    URLs for FlixBD ``screen_shots_url``.
    """
    cfg = ScreenshotSettings.get_solo()
    if not cfg.is_enabled:
        logger.info("Screenshot capture skipped (disabled in Screenshot settings).")
        return []
    if not (
        cfg.worker_base_url
        and cfg.telegram_bot_token
        and cfg.telegram_chat_id
        and cfg.crypto_phrase
    ):
        logger.warning(
            "Screenshot capture skipped: set worker_base_url, telegram_bot_token, "
            "telegram_chat_id, and crypto_phrase in Admin → Screenshot settings."
        )
        return []

    if not video_path or not os.path.isfile(video_path):
        return []

    paths: list[Path] = keyframes.extract_keyframes_webp(
        video_path,
        name_prefix=name_prefix,
        num_frames=keyframes.NUM_FRAMES_DEFAULT,
        max_total_bytes=keyframes.MAX_TOTAL_BYTES_DEFAULT,
    )
    if not paths:
        logger.warning("No keyframes extracted from %s", video_path)
        return []

    urls: list[str] = []
    parent = paths[0].parent
    try:
        for p in paths:
            try:
                tpath = tg.upload_document_get_file_path(
                    bot_token=cfg.telegram_bot_token,
                    chat_id=cfg.telegram_chat_id,
                    file_path_local=str(p),
                    filename=p.name,
                )
                if not tpath:
                    continue
                url = tg.worker_image_url(
                    worker_base=cfg.worker_base_url,
                    crypto_phrase=cfg.crypto_phrase,
                    telegram_file_path=tpath,
                    download_basename=p.name,
                )
                urls.append(url)
            except Exception as e:
                logger.error("Screenshot upload failed for %s: %s", p, e)
    finally:
        shutil.rmtree(parent, ignore_errors=True)

    logger.info("Published %d screenshot URL(s) to Worker", len(urls))
    return urls
