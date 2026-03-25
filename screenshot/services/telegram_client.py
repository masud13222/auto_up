"""Upload files to Telegram Bot API; build Worker image URLs (matches screenshot/worker.js)."""

from __future__ import annotations

import base64
import hashlib
import io
import logging
import os
import secrets
from urllib.parse import quote

import httpx
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger(__name__)


def _aes_key(crypto_phrase: str) -> bytes:
    return hashlib.sha256(crypto_phrase.encode("utf-8")).digest()


def encrypt_telegram_file_path(file_path: str, crypto_phrase: str) -> str:
    """Encrypt Telegram getFile path only (short token)."""
    aes = AESGCM(_aes_key(crypto_phrase))
    nonce = secrets.token_bytes(12)
    ct = aes.encrypt(nonce, file_path.encode("utf-8"), None)
    raw = nonce + ct
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _api_post(base: str, method: str, **kwargs) -> dict:
    kwargs.setdefault("timeout", 120.0)
    r = httpx.post(f"{base}/{method}", **kwargs)
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"HTTP {r.status_code} (non-JSON): {r.text[:500]}") from None
    if not data.get("ok"):
        raise RuntimeError(
            f"Telegram API (HTTP {r.status_code}): {data.get('description', data)}"
        )
    return data["result"]


def _file_id_from_send_result(result: dict) -> str | None:
    doc = result.get("document")
    if doc and doc.get("file_id"):
        return doc["file_id"]
    st = result.get("sticker")
    if st and st.get("file_id"):
        return st["file_id"]
    photos = result.get("photo") or []
    if photos:
        best = max(photos, key=lambda p: p.get("file_size") or p.get("width") or 0)
        return best.get("file_id")
    return None


def upload_document_get_file_path(
    *,
    bot_token: str,
    chat_id: str,
    file_path_local: str,
    filename: str,
) -> str | None:
    """sendDocument → getFile → return Telegram file_path."""
    base = f"https://api.telegram.org/bot{bot_token}"
    with open(file_path_local, "rb") as f:
        raw = f.read()
    buf = io.BytesIO(raw)
    buf.seek(0)
    files = {"document": (filename, buf, "application/octet-stream")}
    data = {
        "chat_id": str(chat_id),
        "disable_content_type_detection": "true",
    }
    try:
        result = _api_post(base, "sendDocument", data=data, files=files)
    except Exception as e:
        logger.error("Telegram sendDocument failed: %s", e)
        return None
    fid = _file_id_from_send_result(result)
    if not fid:
        logger.error("No file_id in Telegram response")
        return None
    try:
        finfo = _api_post(base, "getFile", data={"file_id": fid})
    except Exception as e:
        logger.error("Telegram getFile failed: %s", e)
        return None
    tpath = finfo.get("file_path")
    if tpath:
        logger.info("Telegram sendDocument ok: %s (%d bytes)", filename, len(raw))
    return tpath


def worker_image_url(
    *,
    worker_base: str,
    crypto_phrase: str,
    telegram_file_path: str,
    download_basename: str,
) -> str:
    base = worker_base.rstrip("/")
    token = encrypt_telegram_file_path(telegram_file_path, crypto_phrase)
    tail = quote(download_basename, safe="-_.")
    return f"{base}/image/{token}/{tail}"
