"""Shared JSON serialization for inlined schemas inside LLM prompts."""

from __future__ import annotations

import json
from typing import Any


_JSON_KWARGS: dict[str, Any] = {"separators": (",", ":"), "ensure_ascii": False}


def json_compact(obj: Any) -> str:
    """Minified JSON for embedding in Markdown / system prompts."""
    return json.dumps(obj, **_JSON_KWARGS)
