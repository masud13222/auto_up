from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


_LOCK_DIR = Path(tempfile.gettempdir()) / "upload-runtime-locks"


def _ensure_lock_dir() -> None:
    _LOCK_DIR.mkdir(parents=True, exist_ok=True)


def _lock_path(name: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in name)
    return _LOCK_DIR / f"{safe}.lock"


def _pid_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _read_lock_meta(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _lock_is_stale(
    path: Path,
    meta: dict,
    stale_after_seconds: int,
    *,
    allow_steal_from_alive_process: bool = True,
) -> bool:
    started_at = float(meta.get("started_at") or 0)
    pid = meta.get("pid")
    age = max(0.0, time.time() - started_at) if started_at else None

    if isinstance(pid, int) and _pid_alive(pid):
        if not allow_steal_from_alive_process:
            return False
        if age is None:
            return False
        return age > stale_after_seconds

    if age is None:
        try:
            age = max(0.0, time.time() - path.stat().st_mtime)
        except OSError:
            return False
    return age > 2 or not _pid_alive(pid if isinstance(pid, int) else None)


@dataclass
class RuntimeLock:
    path: Path
    fd: int
    token: str

    def release(self) -> None:
        try:
            os.close(self.fd)
        except OSError:
            pass
        try:
            meta = _read_lock_meta(self.path)
            if not meta or meta.get("token") == self.token:
                self.path.unlink(missing_ok=True)
        except OSError:
            pass


def acquire_runtime_lock(
    name: str,
    *,
    stale_after_seconds: int,
    wait: bool = False,
    timeout_seconds: float | None = None,
    poll_interval_seconds: float = 1.0,
    allow_steal_from_alive_process: bool = True,
) -> RuntimeLock | None:
    _ensure_lock_dir()
    path = _lock_path(name)
    deadline = (
        None
        if not wait or timeout_seconds is None
        else time.monotonic() + max(0.0, float(timeout_seconds))
    )
    poll_interval_seconds = max(0.1, float(poll_interval_seconds))

    while True:
        for _ in range(2):
            token = uuid.uuid4().hex
            payload = {
                "pid": os.getpid(),
                "started_at": time.time(),
                "token": token,
            }
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                meta = _read_lock_meta(path)
                if _lock_is_stale(
                    path,
                    meta,
                    stale_after_seconds,
                    allow_steal_from_alive_process=allow_steal_from_alive_process,
                ):
                    try:
                        path.unlink(missing_ok=True)
                    except OSError:
                        return None
                    continue
                break

            try:
                os.write(fd, json.dumps(payload).encode("utf-8"))
            except Exception:
                try:
                    os.close(fd)
                except OSError:
                    pass
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
            return RuntimeLock(path=path, fd=fd, token=token)

        if not wait:
            return None
        if deadline is not None and time.monotonic() >= deadline:
            return None
        time.sleep(poll_interval_seconds)
