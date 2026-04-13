"""
Force ``is_adult`` when the source URL's registrable domain matches configured root labels.

Uses ``tldextract`` (Mozilla Public Suffix List) so ``primehub`` matches ``primehub.me``,
``primehub.to``, ``www.primehub.com``, ``cdn.primehub.co.uk``, etc. Subdomains are handled
by the library (extracted ``domain`` is the label before the public suffix).

Does not substring-match full URLs. IPs and hostnames without a public suffix do not match.
"""
from __future__ import annotations

import logging
from urllib.parse import urlparse

import tldextract

from constant import FORCE_IS_ADULT_SOURCE_ROOT_DOMAIN_LABELS

logger = logging.getLogger(__name__)


def hostname_from_url(url: str) -> str | None:
    """Return lowercase hostname from a URL or URL-like string; None if unparseable."""
    if not url or not isinstance(url, str):
        return None
    raw = url.strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
        host = (parsed.hostname or "").strip().lower()
        return host or None
    except (ValueError, UnicodeError):
        return None


def _normalized_root_labels(
    root_labels: list[str] | tuple[str, ...] | None,
) -> frozenset[str]:
    src = (
        root_labels
        if root_labels is not None
        else (FORCE_IS_ADULT_SOURCE_ROOT_DOMAIN_LABELS or ())
    )
    return frozenset(str(x).strip().lower() for x in src if str(x).strip())


def hostname_matches_force_is_adult_roots(
    hostname: str,
    root_labels: list[str] | tuple[str, ...] | None = None,
) -> bool:
    """
    True if ``hostname``'s ICANN registrable domain label (before public suffix) is listed.

    Example: ``www.primehub.to`` → domain ``primehub`` matches label ``primehub``.
    """
    h = (hostname or "").strip().lower().rstrip(".")
    if not h:
        return False
    labs = _normalized_root_labels(root_labels)
    if not labs:
        return False
    ext = tldextract.extract(h)
    if not ext.suffix:
        return False
    root = (ext.domain or "").strip().lower()
    if not root:
        return False
    return root in labs


def url_matches_any_force_is_adult_domain(
    url: str,
    root_labels: list[str] | tuple[str, ...] | None = None,
) -> bool:
    """True if the URL's hostname matches any configured root domain label."""
    host = hostname_from_url(url)
    if not host:
        return False
    return hostname_matches_force_is_adult_roots(host, root_labels=root_labels)


def urls_match_force_is_adult_domains(
    urls: list | tuple | None,
    root_labels: list[str] | tuple[str, ...] | None = None,
) -> bool:
    """True if any URL in the iterable matches a configured root label (all TLDs via PSL)."""
    for raw in urls or ():
        if not raw or not isinstance(raw, str):
            continue
        if url_matches_any_force_is_adult_domain(raw, root_labels=root_labels):
            return True
    return False


def apply_force_is_adult_from_source_urls(
    data: dict,
    urls: list | tuple | None,
    root_labels: list[str] | tuple[str, ...] | None = None,
) -> dict:
    """
    When any source URL matches a configured root domain label, set ``is_adult`` True.

    Mutates ``data`` in place; returns ``data``.
    """
    if not isinstance(data, dict):
        return data
    if not urls_match_force_is_adult_domains(urls, root_labels=root_labels):
        return data
    if data.get("is_adult") is not True:
        logger.info(
            "Forced is_adult=True from source domain policy (LLM had %r)",
            data.get("is_adult"),
        )
        data["is_adult"] = True
    return data
