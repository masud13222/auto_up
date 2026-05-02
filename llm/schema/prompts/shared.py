"""Fragments shared between movie and TV combined extract prompts."""

from __future__ import annotations

from ..blocked_names import BLOCKED_SITE_NAMES

_BLOCKED = ", ".join(BLOCKED_SITE_NAMES)


def build_resolution_note(extra_below: bool = False, extra_above: bool = False, max_extra: int = 0) -> str:
    parts = ["Base: 480p, 720p, 1080p always included when present."]
    if extra_below:
        parts.append("Below 720p: ON (include 520p, 360p, 240p etc).")
    else:
        parts.append("Below 720p: OFF.")
    if extra_above:
        parts.append("Above 1080p: ON (include 2160p/4K).")
    else:
        parts.append("Above 1080p: OFF.")
    if max_extra > 0:
        parts.append(f"Max extras beyond base: {max_extra}.")
    return " ".join(parts)


def core_rules_block_shared() -> str:
    return f"""### CORE RULES (follow strictly, in this order):
1. Return ONLY valid JSON. No markdown fences, no extra text.
2. Use only what is explicit in the Markdown. Never guess. Omit missing fields (no null, no empty strings).
3. Download URLs: copy each URL exactly as written in the Markdown link target. Do not shorten, decode, rebuild, or alter in any way.
4. Never use watch/stream/player/preview/embed links as download entries — only real download/gateway URLs.
5. URL must be absolute with complete hostname. Relative → prepend page domain.
6. Strip blocked site names from TEXT fields only (title, filenames): {_BLOCKED}. URLs: copy as-is even if they contain a blocked name.
7. One dual/multi-audio file = ONE entry with `l` as array. Do not split same file into separate language entries.
8. If same resolution has both dual-audio and single-language files, keep only dual-audio.
9. Prefer x264 when multiple codec options exist.
10. Never invent resolution keys not shown on the page."""


def seo_block_shared() -> str:
    return """### SEO:
- meta_title: 50-60 chars. meta_description: 140-160 chars, natural CTA. meta_keywords: 10-15 comma-separated."""
