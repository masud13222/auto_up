"""
Canonical URL form for comparing homepage scrape URLs with user-managed skip list entries.
"""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse

from upload.utils.web_scrape import normalize_http_url


def canonical_skip_url(url: str) -> str:
    """
    Normalize a URL for equality checks (skip list vs scraped links).

    - Applies ``normalize_http_url`` (scheme, bare host, etc.)
    - Lowercases scheme; rebuilds host from ``hostname`` (ASCII/punycode safe)
    - Strips a single leading ``www.`` from the hostname (common duplicate form)
    - Omits default ports (80 / 443) from the authority
    - Strips trailing slash from path (except root)
    - Drops fragment; keeps query string when present
    """
    if not url or not isinstance(url, str):
        return ""
    u = normalize_http_url(url.strip())
    if not u:
        return ""
    parsed = urlparse(u)
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    port = parsed.port
    if scheme == "https" and port == 443:
        port = None
    elif scheme == "http" and port == 80:
        port = None
    if port:
        netloc = f"{host}:{port}"
    else:
        netloc = host
    path = parsed.path or ""
    path = path.rstrip("/") or "/"
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))
