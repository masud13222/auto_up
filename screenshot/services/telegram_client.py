"""Upload files to Telegram Bot API; build Worker image URLs (matches screenshot/worker.js)."""

from __future__ import annotations

import base64
import hashlib
import io
import logging
import secrets
import time
from urllib.parse import quote

import httpx
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger(__name__)

# Seconds to wait before 2nd and 3rd upload attempt (after a failure).
_TELEGRAM_UPLOAD_RETRY_DELAYS_SEC = (10.0, 20.0)


def _aes_key(crypto_phrase: str) -> bytes:
    return hashlib.sha256(crypto_phrase.encode("utf-8")).digest()


def encrypt_telegram_file_id(file_id: str, crypto_phrase: str) -> str:
    """Encrypt Telegram file_id for Worker URL segment (AES-GCM, matches worker.js)."""
    aes = AESGCM(_aes_key(crypto_phrase))
    nonce = secrets.token_bytes(12)
    ct = aes.encrypt(nonce, file_id.encode("utf-8"), None)
    raw = nonce + ct
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decrypt_telegram_file_id(token: str, crypto_phrase: str) -> str:
    """Decrypt Worker URL segment back to plaintext Telegram file_id."""
    s = token.strip().strip("/")
    if not s:
        raise ValueError("empty token")
    pad = (4 - len(s) % 4) % 4
    b64 = s + "=" * pad
    raw = base64.urlsafe_b64decode(b64.encode("ascii"))
    if len(raw) < 13:
        raise ValueError("token too short")
    nonce, ciphertext = raw[:12], raw[12:]
    aes = AESGCM(_aes_key(crypto_phrase))
    plain = aes.decrypt(nonce, ciphertext, None)
    return plain.decode("utf-8")


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


def upload_document_get_file_id(
    *,
    bot_token: str,
    chat_id: str,
    file_path_local: str,
    filename: str,
) -> str | None:
    """sendDocument → getFile → return Telegram file_id (stable; Worker resolves path via getFile).

    Retries up to 3 attempts total: first immediate, then after 10s, then after 20s.
    """
    base = f"https://api.telegram.org/bot{bot_token}"
    with open(file_path_local, "rb") as f:
        raw = f.read()
    data = {
        "chat_id": str(chat_id),
        "disable_content_type_detection": "true",
    }
    max_attempts = 1 + len(_TELEGRAM_UPLOAD_RETRY_DELAYS_SEC)
    last_error: Exception | str | None = None

    for attempt in range(max_attempts):
        if attempt > 0:
            delay = _TELEGRAM_UPLOAD_RETRY_DELAYS_SEC[attempt - 1]
            logger.warning(
                "Telegram upload retry %s/%s after %.0fs (last error: %s)",
                attempt + 1,
                max_attempts,
                delay,
                last_error,
            )
            time.sleep(delay)

        buf = io.BytesIO(raw)
        buf.seek(0)
        files = {"document": (filename, buf, "application/octet-stream")}
        try:
            result = _api_post(base, "sendDocument", data=data, files=files)
        except Exception as e:
            last_error = e
            logger.error("Telegram sendDocument failed (attempt %s/%s): %s", attempt + 1, max_attempts, e)
            continue

        fid = _file_id_from_send_result(result)
        if not fid:
            last_error = "No file_id in Telegram response"
            logger.error("No file_id in Telegram response (attempt %s/%s)", attempt + 1, max_attempts)
            continue

        try:
            finfo = _api_post(base, "getFile", data={"file_id": fid})
        except Exception as e:
            last_error = e
            logger.error("Telegram getFile failed (attempt %s/%s): %s", attempt + 1, max_attempts, e)
            continue

        tpath = finfo.get("file_path")
        if tpath:
            logger.info("Telegram sendDocument ok: %s (%d bytes)", filename, len(raw))
            return fid
        last_error = "getFile returned empty file_path"
        logger.error("Telegram getFile empty file_path (attempt %s/%s)", attempt + 1, max_attempts)

    logger.error("Telegram upload gave up after %s attempts: %s", max_attempts, last_error)
    return None


def worker_image_url(
    *,
    worker_base: str,
    crypto_phrase: str,
    telegram_file_id: str,
    download_basename: str,
) -> str:
    """Build /image/{token}/{filename}; token encrypts file_id (Worker calls getFile to get file_path)."""
    base = worker_base.rstrip("/")
    token = encrypt_telegram_file_id(telegram_file_id, crypto_phrase)
    tail = quote(download_basename, safe="-_.")
    return f"{base}/image/{token}/{tail}"
