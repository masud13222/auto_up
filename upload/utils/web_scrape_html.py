"""
HTML / URL helpers for scraping — no browser, no MarkItDown.

Used by ``web_scrape`` for fragment absolutization, URL normalization, and LLM-oriented markdown trimming.
"""

import re
from urllib.parse import urljoin

from selectolax.lexbor import LexborHTMLParser

_URL_WITH_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*://", re.I)

# First matching line (exact, after strip) starts tail discarded for LLM prompts.
_MARKDOWN_DISCARD_FROM_LINE = (
    "### How to Download?",
    "## You May Also Like",
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


_IMAGE_EXT = (".jpg", ".jpeg", ".webp")


def _url_path_looks_like_raster_image(url: str) -> bool:
    if not url:
        return False
    path = url.split("?", 1)[0].split("#", 1)[0].strip().lower()
    return path.endswith(_IMAGE_EXT)


def sanitize_markdown_for_llm(md: str) -> str:
    """
    After HTML→markdown: drop screenshot sections, strip raster image links, shrink noise for LLM.

    - Removes any full line containing the word "Screenshot" (case-insensitive).
    - Removes markdown images ``![...](...)``.
    - Replaces ``[text](url)`` with ``text`` when url is .jpg/.jpeg/.webp (query string allowed).
    - Removes bare/autolink URLs that end with those extensions.
    """
    if not md:
        return md

    lines_out: list[str] = []
    for line in md.split("\n"):
        if "screenshot" in line.casefold():
            continue
        lines_out.append(line)
    text = "\n".join(lines_out)

    # Markdown images
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)

    def _link_repl(m: re.Match) -> str:
        label, url = m.group(1), m.group(2).strip()
        if _url_path_looks_like_raster_image(url):
            return label.strip()
        return m.group(0)

    text = re.sub(r"\[([^\]]*)\]\(([^)]+)\)", _link_repl, text)

    # Angle-bracket autolinks to raster images
    text = re.sub(
        r"<https?://[^>\s]+>",
        lambda m: "" if _url_path_looks_like_raster_image(m.group(0)[1:-1]) else m.group(0),
        text,
        flags=re.I,
    )

    # Bare http(s) URLs ending in image extensions
    text = re.sub(
        r"https?://[^\s\[\]()<>\"']+?(?:\.jpe?g|\.webp)(?:\?[^\s\[\]()<>\"']*)?",
        "",
        text,
        flags=re.I,
    )

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
