"""
HTML / URL helpers for scraping — no browser, no MarkItDown.

Used by ``web_scrape`` for fragment absolutization, URL normalization, and LLM-oriented markdown trimming.
"""

import re
from urllib.parse import urljoin, urlparse, urlunparse

from selectolax.lexbor import LexborHTMLParser

_URL_WITH_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*://", re.I)

# First matching line (exact, after strip) starts tail discarded for LLM prompts.
_MARKDOWN_DISCARD_FROM_LINE = (
    "### How to Download?",
    "## You May Also Like",
    # Raw string: avoid invalid escapes (\_ \*); value must still match MarkItDown line exactly.
    r"**\_\_\_\_\_\_\_ [ \*\*Play in VLC / Playit player if audio is not supporting in MX player\*\*⇓] \_\_\_\_\_\_\_**",
)


def truncate_markdown_for_llm(md: str) -> str:
    """Remove a known heading and everything after it (comments, related posts, how-to filler)."""
    lines = md.split("\n")
    cut = len(lines)
    for trigger in _MARKDOWN_DISCARD_FROM_LINE:
        for i, line in enumerate(lines):
            if line.strip() == trigger:
                cut = min(cut, i)
                break
    if cut < len(lines):
        return "\n".join(lines[:cut]).rstrip()
    return md


def sanitize_markdown_for_llm(md: str) -> str:
    """
    After HTML→markdown: light cleanup only.

    Removes full lines containing the word "Screenshot" (case-insensitive).
    Does not strip .jpg/.webp links — those are needed for poster_url and similar.
    """
    if not md:
        return md

    lines_out: list[str] = []
    for line in md.split("\n"):
        if "screenshot" in line.casefold():
            continue
        lines_out.append(line)
    text = "\n".join(lines_out)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_http_url(url: str) -> str:
    """
    Ensure the URL has a scheme so Chromium can navigate.
    Example: cinefreak.net/foo -> https://cinefreak.net/foo
    """
    if not url or not isinstance(url, str):
        return url
    u = url.strip()
    if not u:
        return u
    if u.startswith("//"):
        return "https:" + u
    if not _URL_WITH_SCHEME.match(u):
        return "https://" + u.lstrip("/")
    return u


def normalize_download_gateway_path(url: str) -> str:
    """
    Some hosts use ``/x/...`` where the download gateway expects ``/f/...``.
    Rewrite path segments only (query/fragment unchanged).
    """
    if not url or not isinstance(url, str):
        return url
    u = url.strip()
    if not u or "/x/" not in u:
        return u
    try:
        p = urlparse(u)
        if "/x/" not in p.path:
            return u
        new_path = p.path.replace("/x/", "/f/")
        out = urlunparse((p.scheme, p.netloc, new_path, p.params, p.query, p.fragment))
        return out
    except Exception:
        return u


def absolutize_resource_urls(html_fragment: str, page_url: str) -> str:
    """
    Rewrite relative href/src in an HTML fragment to absolute URLs (page_url as base).
    Converters leave relative links as e.g. [text](/path); the LLM cannot prepend domain reliably.
    """
    base = normalize_http_url(page_url)
    tree = LexborHTMLParser(html_fragment)

    for el in tree.css("a[href]"):
        href = (el.attributes.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        low = href.lower()
        if low.startswith(("javascript:", "mailto:", "tel:", "data:")):
            continue
        if href.startswith(("http://", "https://")):
            continue
        if href.startswith("//"):
            el.attrs["href"] = urljoin("https:", href)
        else:
            el.attrs["href"] = urljoin(base, href)

    for el in tree.css("img[src]"):
        src = (el.attributes.get("src") or "").strip()
        if not src or src.lower().startswith("data:"):
            continue
        if src.startswith(("http://", "https://")):
            continue
        if src.startswith("//"):
            el.attrs["src"] = urljoin("https:", src)
        else:
            el.attrs["src"] = urljoin(base, src)

    body = tree.body
    if body is None or body.child is None:
        return tree.html
    parts: list[str] = []
    n = body.child
    while n:
        parts.append(n.html)
        n = n.next
    return "".join(parts) if parts else tree.html
