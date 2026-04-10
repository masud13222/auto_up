"""
Prefix ffmpeg/ffprobe invocations with OS-level deprioritization when available.

Uses ionice idle I/O class and nice 19 so background workers yield to interactive
load. If ``nice``/``ionice`` are absent (e.g. some Windows installs), the command
list is returned unchanged.
"""

from __future__ import annotations

import shutil
from collections.abc import Sequence


def low_priority_cmd(cmd: Sequence[str]) -> list[str]:
    if not cmd:
        return []
    prefix: list[str] = []
    if shutil.which("ionice"):
        prefix.extend(["ionice", "-c", "3"])
    if shutil.which("nice"):
        prefix.extend(["nice", "-n", "19"])
    return [*prefix, *list(cmd)]
