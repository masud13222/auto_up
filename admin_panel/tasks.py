"""
django-q task: dump DB and upload to Google Drive (see BackupSettings).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)


def _finish(ok: bool, note: str):
    from admin_panel.models import BackupSettings

    if not BackupSettings.objects.filter(pk=1).exists():
        return
    BackupSettings.objects.filter(pk=1).update(
        last_backup_at=timezone.now(),
        last_backup_ok=ok,
        last_backup_note=(note or "")[:2000],
    )


def run_database_backup():
    """Called by django-q on schedule."""
    from admin_panel.models import BackupSettings
    from settings.models import GoogleConfig
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    from upload.service.uploader import DriveUploader

    cfg = BackupSettings.objects.filter(pk=1).first()
    if not cfg or not cfg.is_enabled:
        logger.info("run_database_backup: skipped (no settings or disabled).")
        return
    folder_id = (cfg.drive_backup_folder_id or "").strip()
    if not folder_id:
        _finish(False, "drive_backup_folder_id empty")
        return

    gconf = cfg.google_config or GoogleConfig.objects.first()
    if not gconf:
        _finish(False, "No GoogleConfig")
        return

    db = settings.DATABASES["default"]
    engine = (db.get("ENGINE") or "").lower()
    tmp_path = None

    try:
        stamp = timezone.now().strftime("%Y%m%d_%H%M%S")

        if "postgresql" in engine:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f"_db_{stamp}.dump")
            tmp_path = tmp.name
            tmp.close()
            env = os.environ.copy()
            env["PGPASSWORD"] = str(db.get("PASSWORD") or "")
            cmd = [
                "pg_dump",
                "-h",
                db.get("HOST") or "localhost",
                "-p",
                str(db.get("PORT") or "5432"),
                "-U",
                str(db.get("USER") or ""),
                "-d",
                str(db.get("NAME") or ""),
                "-Fc",
                "-f",
                tmp_path,
            ]
            r = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=7200,
            )
            if r.returncode != 0:
                err = (r.stderr or r.stdout or "pg_dump failed")[:2000]
                _finish(False, err)
                return
            upload_name = f"db_backup_{stamp}.dump"
            mime = "application/octet-stream"

        elif "sqlite" in engine:
            src = db.get("NAME")
            if not src or not Path(str(src)).is_file():
                _finish(False, "SQLite database file missing")
                return
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f"_db_{stamp}.sqlite3")
            tmp_path = tmp.name
            tmp.close()
            shutil.copy2(str(src), tmp_path)
            upload_name = f"db_backup_{stamp}.sqlite3"
            mime = "application/octet-stream"

        else:
            _finish(False, f"Unsupported DB engine: {engine!r}")
            return

        gconf = GoogleConfig.objects.get(pk=gconf.pk)
        token_data = DriveUploader._load_token_data(gconf)
        if DriveUploader._is_token_expired(token_data) or not token_data.get("token"):
            token_data = DriveUploader._refresh_and_save(gconf, token_data)
        creds = DriveUploader._build_credentials(token_data)
        service = build("drive", "v3", credentials=creds)

        media = MediaFileUpload(tmp_path, mimetype=mime, resumable=True)
        service.files().create(
            body={"name": upload_name, "parents": [folder_id]},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()

        _finish(True, f"Uploaded {upload_name}")
        logger.info("run_database_backup: uploaded %s", upload_name)

    except FileNotFoundError as e:
        _finish(False, f"Missing binary or file: {e}")
        logger.error("run_database_backup: %s", e)
    except subprocess.TimeoutExpired:
        _finish(False, "pg_dump timed out")
        logger.error("run_database_backup: pg_dump timeout")
    except Exception as e:
        _finish(False, str(e)[:2000])
        logger.exception("run_database_backup failed")
    finally:
        if tmp_path and os.path.isfile(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
