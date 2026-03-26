#!/usr/bin/env python
"""
One-off helper: fetch CineFreak listing pages via the app's pydoll Chrome singleton
(same as upload.utils.web_scrape — Cloudflare / Turnstile path).

Default auto mode: highest page first (371 → … → 1). Parallel: up to N concurrent tabs
on one browser; each finished page appends new unique links to the output file.

Run from project root (where manage.py lives):
    python tes.py
    python tes.py --workers 20 --settle 3
    python tes.py --ascending
    python tes.py -i m.txt --reverse-input

User-Agent for navigation is set on Chrome in upload.utils.web_scrape (_chrome_options),
not as raw HTTP headers (browser engine performs the request).

Does not modify auto_up/scraper.py.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from selectolax.lexbor import LexborHTMLParser

from upload.utils.web_scrape import (
    _fetch_html,
    _fetch_html_async,
    _submit,
    normalize_http_url,
)

CINEFREAK_ORIGIN = "https://www.cinefreak.net"
DEFAULT_LAST_PAGE = 371


def _page_to_url(page_num: int) -> str:
    if page_num <= 1:
        return f"{CINEFREAK_ORIGIN}/"
    return f"{CINEFREAK_ORIGIN}/page/{page_num}/"


def build_cinefreak_listing_urls(
    start_page: int,
    end_page: int,
    *,
    descending: bool,
) -> list[str]:
    if end_page < start_page:
        return []
    pages = list(range(start_page, end_page + 1))
    if descending:
        pages.reverse()
    return [_page_to_url(n) for n in pages]


def read_listing_urls(path: Path) -> list[str]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    out: list[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        s = s.strip().strip(",").strip('"').strip("'")
        if s.lower().startswith(("http://", "https://")):
            out.append(s)
    return out


def _skip_href(href: str) -> bool:
    if not href or href.startswith("#"):
        return True
    low = href.lower()
    return low.startswith(("javascript:", "mailto:", "tel:", "data:"))


def extract_movie_card_links(html: str, page_url: str) -> list[str]:
    """CineFreak-style grid cards (same selectors as auto_up scraper)."""
    parser = LexborHTMLParser(html)
    seen: set[str] = set()
    ordered: list[str] = []
    for node in parser.css("div.card-grid a.movie-card"):
        href = node.attrs.get("href", "").strip()
        if _skip_href(href):
            continue
        full = normalize_http_url(urljoin(page_url, href))
        if full not in seen:
            seen.add(full)
            ordered.append(full)
    return ordered


def extract_all_anchor_links(html: str, page_url: str) -> list[str]:
    """Every <a href=...> on the page (noisier)."""
    parser = LexborHTMLParser(html)
    seen: set[str] = set()
    ordered: list[str] = []
    for node in parser.css("a[href]"):
        href = node.attrs.get("href", "").strip()
        if _skip_href(href):
            continue
        full = normalize_http_url(urljoin(page_url, href))
        if full not in seen:
            seen.add(full)
            ordered.append(full)
    return ordered


def _append_new_unique_links(
    output_path: Path,
    seen_global: set[str],
    batch: list[str],
) -> tuple[int, int]:
    new_lines = [u for u in batch if u not in seen_global]
    for u in new_lines:
        seen_global.add(u)
    if new_lines:
        with output_path.open("a", encoding="utf-8") as f:
            for line in new_lines:
                f.write(line + "\n")
            f.flush()
    return len(new_lines), len(batch)


async def _fetch_one_listing(
    page_url: str,
    settle: float,
    sem: asyncio.Semaphore,
) -> tuple[str, str | None, BaseException | None]:
    async with sem:
        try:
            html = await _fetch_html_async(page_url, settle)
            return page_url, html, None
        except BaseException as exc:
            return page_url, None, exc


async def _run_parallel_listings(
    page_urls: list[str],
    *,
    settle: float,
    workers: int,
    output_path: Path,
    extract_fn,
) -> None:
    sem = asyncio.Semaphore(max(1, workers))
    total = len(page_urls)
    seen_global: set[str] = set()
    lock = asyncio.Lock()
    output_path.write_text("", encoding="utf-8")

    tasks = [asyncio.create_task(_fetch_one_listing(u, settle, sem)) for u in page_urls]
    finished = 0

    for coro in asyncio.as_completed(tasks):
        page_url, html, err = await coro
        finished += 1
        if err is not None:
            print(f"[{finished}/{total}] {page_url} fetch failed: {err}", file=sys.stderr)
            continue
        assert html is not None
        batch = extract_fn(html, page_url)
        async with lock:
            new_n, on_page = _append_new_unique_links(output_path, seen_global, batch)
            uniq = len(seen_global)
        print(
            f"[{finished}/{total}] {page_url} "
            f"links_on_page={on_page} new_unique={new_n} total_unique={uniq}"
        )


def _sync_browser_pages(
    page_urls: list[str],
    *,
    settle: float,
    delay: float,
    output_path: Path,
    extract_fn,
) -> None:
    seen_global: set[str] = set()
    output_path.write_text("", encoding="utf-8")
    total = len(page_urls)

    for idx, page_url in enumerate(page_urls, start=1):
        print(f"[{idx}/{total}] {page_url}")
        try:
            html = _fetch_html(page_url, settle=settle)
        except Exception as e:
            print(f"  fetch failed: {e}", file=sys.stderr)
            if idx < total and delay > 0:
                time.sleep(delay)
            continue

        batch = extract_fn(html, page_url)
        new_n, on_page = _append_new_unique_links(output_path, seen_global, batch)
        print(
            f"  links_on_page={on_page} new_unique={new_n} total_unique={len(seen_global)}"
        )
        if idx < total and delay > 0:
            time.sleep(delay)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="CineFreak listings via pydoll (default: page 371 down to 1)."
    )
    ap.add_argument(
        "-i",
        "--input",
        type=Path,
        default=None,
        help="Optional file: listing URLs, one per line",
    )
    ap.add_argument(
        "--from-page",
        type=int,
        default=1,
        metavar="N",
        help="Auto mode: lowest page number in range (default 1)",
    )
    ap.add_argument(
        "--to-page",
        type=int,
        default=DEFAULT_LAST_PAGE,
        metavar="N",
        help=f"Auto mode: highest page number (default {DEFAULT_LAST_PAGE})",
    )
    ap.add_argument(
        "--ascending",
        action="store_true",
        help="Auto mode: fetch 1 → N instead of N → 1",
    )
    ap.add_argument(
        "--reverse-input",
        action="store_true",
        help="With -i: reverse order of URLs in the file",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("extracted_links.txt"),
        help="Output: one unique link per line",
    )
    ap.add_argument(
        "--settle",
        type=float,
        default=3.0,
        help="Seconds to wait after navigation (pydoll)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=20,
        metavar="N",
        help="Concurrent browser tabs (default 20). Use 1 for sequential.",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Sequential mode only: seconds between pages",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=0,
        metavar="SEC",
        help="Parallel mode: max seconds for whole run (0 = auto from page count)",
    )
    ap.add_argument(
        "--all-anchors",
        action="store_true",
        help="Collect every anchor href instead of only .movie-card links",
    )
    args = ap.parse_args()

    if args.input is not None:
        if not args.input.is_file():
            print(f"Input file not found: {args.input.resolve()}", file=sys.stderr)
            return 1
        page_urls = read_listing_urls(args.input)
        if not page_urls:
            print("No http(s) URLs found in input file.", file=sys.stderr)
            return 1
        if args.reverse_input:
            page_urls = list(reversed(page_urls))
        order_note = "file order" + (" (reversed)" if args.reverse_input else "")
    else:
        if args.from_page < 1:
            print("--from-page must be >= 1", file=sys.stderr)
            return 1
        descending = not args.ascending
        page_urls = build_cinefreak_listing_urls(
            args.from_page, args.to_page, descending=descending
        )
        if not page_urls:
            print("No pages in range.", file=sys.stderr)
            return 1
        order_note = "descending (high → low)" if descending else "ascending (low → high)"
        print(
            f"Auto mode: {len(page_urls)} pages, {order_note}: "
            f"{page_urls[0]} … {page_urls[-1]}"
        )

    extract_fn = extract_all_anchor_links if args.all_anchors else extract_movie_card_links
    total_pages = len(page_urls)
    print(f"Order: {order_note} | output: {args.output.resolve()}")

    if args.workers <= 1:
        _sync_browser_pages(
            page_urls,
            settle=args.settle,
            delay=args.delay,
            output_path=args.output,
            extract_fn=extract_fn,
        )
    else:
        workers = max(1, args.workers)
        per_wave = max(45.0, args.settle + 35.0)
        auto_timeout = int((total_pages + workers - 1) / workers * per_wave + 300)
        timeout_sec = args.timeout if args.timeout > 0 else min(86400, max(900, auto_timeout))

        print(
            f"Parallel pydoll: {workers} concurrent tabs, submit timeout ~{timeout_sec}s "
            f"(UA from web_scrape._chrome_options)"
        )
        try:
            _submit(
                _run_parallel_listings(
                    page_urls,
                    settle=args.settle,
                    workers=workers,
                    output_path=args.output,
                    extract_fn=extract_fn,
                ),
                timeout=timeout_sec,
            )
        except Exception as e:
            print(f"Parallel run failed: {e}", file=sys.stderr)
            return 1

    nlines = 0
    if args.output.is_file():
        text = args.output.read_text(encoding="utf-8", errors="replace")
        nlines = sum(1 for line in text.splitlines() if line.strip())
    print(f"Done. {nlines} lines in {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
