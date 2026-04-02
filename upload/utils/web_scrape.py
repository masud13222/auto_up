"""
Web scraping service using pydoll (headless Chromium).

Browser Singleton Strategy — Remote-Connect Architecture:
  Root cause of previous failures: pydoll's internal ``get_browser_ws_address()``
  uses ``aiohttp`` to query ``http://localhost:PORT/json/version``. In this Docker
  environment aiohttp raises ``ssl:default [Connect call failed]`` for plain HTTP
  on localhost, regardless of aiohttp version. This is an environment-level issue
  (confirmed via test_chrome*.py diagnostic scripts).

  Fix: We manage the Chrome subprocess ourselves and resolve the WS address via
  ``urllib`` (stdlib, no SSL quirks). We then connect with pydoll's
  ``browser.connect(ws_url)`` which goes straight to the WebSocket — bypassing
  the broken aiohttp HTTP call entirely.

  Architecture:
    - One Chrome subprocess per worker, bound to a fixed CDP port.
    - One background asyncio event loop thread per worker process.
    - ``browser.connect()`` gives us a Tab; we use ``browser.new_tab()`` for each
      fetch, navigate, read, close.
    - On any crash the subprocess is killed, a fresh one is started, and we
      reconnect — all lock-protected to prevent concurrent restart races.

Cloudflare Turnstile:
  Each navigation uses ``tab.expect_and_bypass_cloudflare_captcha`` (pydoll CM).
  If the Turnstile shadow root does not appear within ``time_to_wait_captcha``
  pydoll logs an ERROR and continues — page usually still loads. Expected noise.

Public API (WebScrapeService.*) is unchanged.
"""

import asyncio
import io
import json
import logging
import os
import re
import subprocess
import threading
import time
import urllib.request
from urllib.parse import urlparse

from markitdown import MarkItDown
from selectolax.lexbor import LexborHTMLParser

from upload.utils.web_scrape_html import (
    absolutize_resource_urls,
    normalize_download_gateway_path,
    normalize_http_url,
    sanitize_markdown_for_llm,
    truncate_markdown_for_llm,
)

logger = logging.getLogger(__name__)
_markitdown = MarkItDown()

# Main content wrapper (CineFreak). Fallback for PrimeHub-style layout.
_CONTENT_SELECTOR_FALLBACK = "div.single-service-content"

# Suppress pydoll internal CDP/websocket logs
logging.getLogger("pydoll").setLevel(logging.WARNING)
logging.getLogger("pydoll.browser.tab").setLevel(logging.ERROR)
logging.getLogger("pydoll.connection.connection_handler").setLevel(logging.ERROR)

# CDP port for our managed Chrome subprocess (fixed — avoids random-port collisions)
_CDP_PORT = int(os.environ.get("PYDOLL_CDP_PORT", "9222"))

# Chrome binary to use
_CHROME_BINARY = "google-chrome-stable"

# How long to wait for Chrome CDP port after launching (seconds)
_CHROME_STARTUP_TIMEOUT = 20

# Retry/backoff for browser launch failures
_MAX_RESTART_ATTEMPTS = 3
_RESTART_BACKOFF = [2, 5, 10]

# submit() outer timeout: startup + navigation budget
_FETCH_HTML_SUBMIT_TIMEOUT = 240


# ── Chrome subprocess args ────────────────────────────────────────────────────

def _chrome_args() -> list[str]:
    return [
        _CHROME_BINARY,
        f"--remote-debugging-port={_CDP_PORT}",
        "--remote-debugging-address=127.0.0.1",
        "--headless=new",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--enable-webgl",
        "--disable-blink-features=AutomationControlled",
        "--disable-extensions",
        "--disable-default-apps",
        "--disable-sync",
        "--disable-translate",
        "--disable-background-networking",
        "--window-size=1280,720",
        "--no-zygote",
        (
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        ),
        f"--user-data-dir=/tmp/chrome-pydoll-{_CDP_PORT}",
    ]


# ── WS address resolver (stdlib only — avoids aiohttp ssl:default bug) ───────

def _get_ws_address_sync(timeout: int = _CHROME_STARTUP_TIMEOUT) -> str:
    """
    Poll Chrome's /json/version endpoint via urllib until it responds.
    Returns the browser-level WebSocket URL with 127.0.0.1 (not localhost).
    Raises RuntimeError if Chrome doesn't respond within ``timeout`` seconds.
    """
    deadline = time.monotonic() + timeout
    last_exc: Exception = RuntimeError("Chrome never started")
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{_CDP_PORT}/json/version",
                timeout=2,
            ) as resp:
                data = json.loads(resp.read())
                ws = data["webSocketDebuggerUrl"]
                # Replace 'localhost' with '127.0.0.1' to avoid IPv6 surprises
                return ws.replace("localhost", "127.0.0.1")
        except Exception as exc:
            last_exc = exc
            time.sleep(0.5)
    raise RuntimeError(f"Chrome CDP port {_CDP_PORT} not ready: {last_exc}")


# ── Per-Worker Browser Singleton ──────────────────────────────────────────────
#
# Globals (module-level, per-worker-process):
#   _chrome_proc  — the Chrome subprocess
#   _browser      — pydoll Chrome instance (connected via remote-connect)
#   _browser_lock — asyncio.Lock guards init + restart (created inside the loop)
#   _event_loop / _loop_thread — the single persistent asyncio loop for this worker

_event_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_chrome_proc: subprocess.Popen | None = None
_browser = None
_browser_lock: asyncio.Lock | None = None


def _get_persistent_loop() -> asyncio.AbstractEventLoop:
    global _event_loop, _loop_thread
    if _event_loop is None or _event_loop.is_closed():
        _event_loop = asyncio.new_event_loop()
        _loop_thread = threading.Thread(
            target=_event_loop.run_forever,
            name="pydoll-worker-loop",
            daemon=True,
        )
        _loop_thread.start()
        logger.info("[Browser] Persistent asyncio event loop started")
    return _event_loop


def _submit(coro, timeout: int = 180):
    loop = _get_persistent_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)


async def _get_browser_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _kill_chrome_proc():
    """Kill the managed Chrome subprocess (sync, safe to call from any thread)."""
    global _chrome_proc
    proc = _chrome_proc
    if proc is None:
        return
    try:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=4)
    except Exception as exc:
        logger.debug(f"[Browser] kill chrome proc: {exc}")
    finally:
        _chrome_proc = None


def _kill_zombie_chrome():
    """
    Kill any leftover Chrome processes by name (fallback for orphaned procs).
    Sync — run in a thread via asyncio.to_thread from async context.
    """
    try:
        result = subprocess.run(
            ["pkill", "-9", "-f", f"remote-debugging-port={_CDP_PORT}"],
            capture_output=True, timeout=5,
        )
        if result.returncode == 0:
            logger.info("[Browser] Killed zombie Chrome processes")
        time.sleep(1)
    except Exception as exc:
        logger.debug(f"[Browser] pkill chrome: {exc}")


async def _launch_browser():
    """
    Start Chrome subprocess + connect pydoll via remote-connect.

    1. Kill any existing Chrome on our port.
    2. Launch fresh Chrome subprocess.
    3. Poll /json/version via urllib until CDP port is ready.
    4. pydoll browser.connect(ws_url) — no aiohttp involved.
    """
    global _chrome_proc, _browser
    from pydoll.browser.chromium import Chrome

    last_exc: Exception = RuntimeError("never tried")

    for attempt in range(_MAX_RESTART_ATTEMPTS):
        if attempt > 0:
            backoff = _RESTART_BACKOFF[min(attempt - 1, len(_RESTART_BACKOFF) - 1)]
            logger.warning(
                f"[Browser] Launch attempt {attempt + 1}/{_MAX_RESTART_ATTEMPTS}, waiting {backoff}s..."
            )
            await asyncio.sleep(backoff)

        # Kill previous process and any zombies on the same port
        _kill_chrome_proc()
        await asyncio.to_thread(_kill_zombie_chrome)

        try:
            # Start Chrome subprocess
            proc = subprocess.Popen(
                _chrome_args(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            _chrome_proc = proc
            logger.info(f"[Browser] Chrome subprocess started (pid={proc.pid}, port={_CDP_PORT})")

            # Wait for CDP port in a thread (urllib blocks — keep event loop free)
            ws_url = await asyncio.to_thread(_get_ws_address_sync)
            logger.info(f"[Browser] CDP ready: {ws_url}")

            # Connect pydoll (pure WebSocket — no aiohttp)
            browser = Chrome()
            tab = await browser.connect(ws_url)
            _browser = browser
            logger.info("[Browser] Chrome singleton connected via remote-connect")
            return

        except Exception as exc:
            last_exc = exc
            logger.warning(f"[Browser] Launch attempt {attempt + 1} failed: {exc}")
            _kill_chrome_proc()

    raise last_exc


async def _ensure_browser():
    """Start browser if not already running. Lock-protected for concurrent callers."""
    if _browser is not None:
        return
    lock = await _get_browser_lock()
    async with lock:
        if _browser is None:
            await _launch_browser()


async def _restart_browser():
    """Kill crashed browser + restart. Lock-protected."""
    global _browser
    lock = await _get_browser_lock()
    async with lock:
        logger.warning("[Browser] Restarting Chrome singleton after crash...")
        if _browser is not None:
            try:
                await asyncio.wait_for(_browser.close(), timeout=5)
            except Exception:
                pass
            _browser = None
        await _launch_browser()


# ── HTML fetcher ──────────────────────────────────────────────────────────────

async def _fetch_html_async(url: str, settle: float = 2.0) -> str:
    """
    Fetch page HTML via singleton browser.
    Opens a new tab, navigates (with CF Turnstile bypass), reads HTML, closes tab.
    Auto-restarts browser once on crash.
    """
    await _ensure_browser()

    for attempt in range(2):
        try:
            current_browser = _browser
            if current_browser is None:
                await _ensure_browser()
                current_browser = _browser

            tab = await current_browser.new_tab()
            try:
                cm = getattr(tab, "expect_and_bypass_cloudflare_captcha", None)
                nav_timeout = 45.0
                if cm is not None:
                    async with cm(time_to_wait_captcha=10):
                        await asyncio.wait_for(tab.go_to(url), timeout=nav_timeout)
                else:
                    logger.warning("[Browser] CF captcha helper missing — plain navigation")
                    await asyncio.wait_for(tab.go_to(url), timeout=30)
                await asyncio.sleep(settle)
                return await tab.page_source
            finally:
                try:
                    await asyncio.wait_for(tab.close(), timeout=5)
                except Exception:
                    pass

        except Exception as exc:
            if attempt == 0:
                logger.warning(f"[Browser] Fetch failed: {exc} — restarting browser, retrying...")
                await _restart_browser()
            else:
                raise


def _fetch_html(url: str, settle: float = 2.0) -> str:
    """Sync entry point: fetch HTML via singleton browser."""
    normalized = normalize_http_url(url)
    if normalized != url:
        logger.info(f"[Browser] Normalized URL: {url!r} -> {normalized!r}")
    gateway_fixed = normalize_download_gateway_path(normalized)
    if gateway_fixed != normalized:
        logger.info(f"[Browser] Gateway path fix: {normalized!r} -> {gateway_fixed!r}")
    return _submit(_fetch_html_async(gateway_fixed, settle), timeout=_FETCH_HTML_SUBMIT_TIMEOUT)


def _prepare_nav_url(url: str) -> str:
    n = normalize_http_url((url or "").strip())
    return normalize_download_gateway_path(n)


# ── URL pattern matchers ──────────────────────────────────────────────────────

_PATTERN_R2 = re.compile(
    r'href=["\'](?P<url>(?:https?:)?//[^"\']*(?:\.r2\.dev|r2\.cloudflarestorage\.com)[^"\']*)["\']'
)
_PATTERN_VIDEO = re.compile(
    r'href=["\'](?P<url>https://video-downloads\.googleusercontent\.com[^"\']*)["\']'
)
_PATTERN_LOC = re.compile(r'window\.location\.href\s*=\s*["\'](.+?)["\']')
_DIRECT_MEDIA_EXT_RE = re.compile(r"\.(?:mkv|mp4|avi|mov|m4v|ts|webm)(?:$|[?#])", re.I)


def _is_direct_media_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if "video-downloads.googleusercontent.com" in host:
        return True
    if any(t in host for t in (".r2.dev", "r2.cloudflarestorage.com")) and _DIRECT_MEDIA_EXT_RE.search(path):
        return True
    return False


# ── Download page resolver ────────────────────────────────────────────────────

async def _resolve_download_page_async(url: str):
    """
    One generate.php (or similar) page → R2 / video link list.
    Safe to run concurrently via asyncio.gather (each opens its own tab).
    """
    try:
        u = _prepare_nav_url(url)
        if _is_direct_media_url(u):
            logger.debug(f"[Scrape] Direct media URL, skipping browser: {u}")
            return [u]

        html = await _fetch_html_async(u, 4.0)
        target_url = u
        match_loc = _PATTERN_LOC.search(html)

        if match_loc:
            raw_target = match_loc.group(1)
            target_url = normalize_download_gateway_path(normalize_http_url(raw_target))
            if target_url != raw_target:
                logger.debug(f"[Scrape] Normalized redirect: {raw_target!r} -> {target_url!r}")
            logger.debug(f"[Scrape] Found redirect URL: {target_url}")
            if _is_direct_media_url(target_url):
                return [target_url]
            html = await _fetch_html_async(target_url, 4.0)
        else:
            logger.debug(f"[Scrape] No redirect found, checking current page: {u}")

        matches = _PATTERN_R2.findall(html)
        if matches:
            logger.info(f"[Scrape] Found {len(matches)} R2 link(s)")
            return matches

        video_matches = _PATTERN_VIDEO.findall(html)
        if video_matches:
            logger.info(f"[Scrape] Found {len(video_matches)} video link(s)")
            return video_matches

        logger.info(f"[Scrape] No R2/video links found. Trying fallback for: {target_url}")
        if "/f/" in target_url:
            for fb_url in (
                target_url.replace("/f/", "/w/"),
                target_url.replace("/f/", "/gp/"),
            ):
                try:
                    logger.debug(f"[Scrape] Checking fallback: {fb_url}")
                    fb_html = await _fetch_html_async(fb_url, 6.0)
                    fb_matches = _PATTERN_VIDEO.findall(fb_html)
                    if fb_matches:
                        logger.info(f"[Scrape] Found {len(fb_matches)} video link(s) in {fb_url}")
                        return fb_matches
                except Exception as ve:
                    logger.warning(f"[Scrape] Fallback failed for {fb_url}: {ve}")
        else:
            parsed = urlparse(target_url)
            path_parts = [p for p in parsed.path.strip("/").split("/") if p]
            if path_parts:
                last_id = path_parts[-1]
                instant_url = f"{parsed.scheme}://{parsed.netloc}/instant_{last_id}"
                try:
                    logger.debug(f"[Scrape] Checking instant fallback: {instant_url}")
                    instant_html = await _fetch_html_async(instant_url, 5.0)
                    instant_matches = _PATTERN_VIDEO.findall(instant_html)
                    if instant_matches:
                        logger.info(f"[Scrape] Found {len(instant_matches)} video link(s) via instant: {instant_url}")
                        return instant_matches
                    logger.warning(f"[Scrape] Instant fallback returned no links: {instant_url}")
                except Exception as ie:
                    logger.warning(f"[Scrape] Instant fallback failed for {instant_url}: {ie}")

        logger.warning(f"[Scrape] No links found for URL: {url}")
        return None

    except Exception as exc:
        logger.error(f"[Scrape] resolve download page ({url}): {exc}", exc_info=True)
        return None


async def _resolve_download_pages_parallel(urls: list[str]) -> list:
    if not urls:
        return []
    # Limit to 2 concurrent tabs — more causes WebSocket timeouts under load
    sem = asyncio.Semaphore(2)

    async def _guarded(u):
        async with sem:
            return await _resolve_download_page_async(u)

    return await asyncio.gather(*(_guarded(u) for u in urls), return_exceptions=True)


# ── Public scraping service ───────────────────────────────────────────────────

class WebScrapeService:
    normalize_http_url = staticmethod(normalize_http_url)
    normalize_download_gateway_path = staticmethod(normalize_download_gateway_path)

    @staticmethod
    def clean_html(html: str) -> str:
        """Convert HTML to LLM-friendly Markdown."""
        buf = io.BytesIO(html.encode("utf-8"))
        md = _markitdown.convert_stream(buf, file_extension=".html").text_content
        md = re.sub(r"\n{3,}", "\n\n", md).strip()
        md = truncate_markdown_for_llm(md)
        return sanitize_markdown_for_llm(md)

    @staticmethod
    def get_page_content(url: str, selector: str = "div.content-grid.container"):
        """Fetch page, extract selector block, return Markdown."""
        try:
            logger.info(f"[Scrape] get_page_content → {url}")
            html = _fetch_html(url, settle=5.0)
            parser = LexborHTMLParser(html)
            node = parser.css_first(selector)
            used = selector
            if not node:
                node = parser.css_first(_CONTENT_SELECTOR_FALLBACK)
                used = _CONTENT_SELECTOR_FALLBACK
            if node:
                raw_html = node.html
                raw_html = absolutize_resource_urls(raw_html, url)
                cleaned = WebScrapeService.clean_html(raw_html)
                logger.info(
                    f"[Scrape] Extracted {len(raw_html):,} → {len(cleaned):,} chars "
                    f"({100 - len(cleaned)/len(raw_html)*100:.0f}% reduction) "
                    f"[root={used!r}]"
                )
                return cleaned
            logger.warning(
                f"[Scrape] Selectors {selector!r} and {_CONTENT_SELECTOR_FALLBACK!r} not found in {url}"
            )
            return None
        except Exception as exc:
            logger.error(f"[Scrape] get_page_content({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def cinefreak_title(url: str):
        """Fetch page, return h1 title text."""
        try:
            logger.info(f"[Scrape] cinefreak_title → {url}")
            html = _fetch_html(url, settle=3.0)
            parser = LexborHTMLParser(html)
            node = parser.css_first("div.content-grid.container h1")
            if not node:
                node = parser.css_first(f"{_CONTENT_SELECTOR_FALLBACK} h1")
            if node:
                title = node.text(strip=True)
                logger.info(f"[Scrape] Title: {title}")
                return title
            logger.warning(f"[Scrape] h1 not found in {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] cinefreak_title({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def get_url(url: str):
        """Resolve one gateway URL → R2/video link list."""
        try:
            logger.info(f"[Scrape] get_url → {url}")
            return _submit(_resolve_download_page_async(url), timeout=180)
        except Exception as exc:
            logger.error(f"[Scrape] get_url({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def get_urls_parallel(urls: list[str]) -> list:
        """
        Resolve multiple gateway URLs concurrently (2 tabs at a time).
        Returns list aligned with input — each entry is a link list or None.
        """
        urls = [u for u in urls if u]
        if not urls:
            return []
        timeout = min(900, 120 + 90 * len(urls))
        try:
            logger.info(f"[Scrape] get_urls_parallel → {len(urls)} URL(s)")
            return _submit(_resolve_download_pages_parallel(urls), timeout=timeout)
        except Exception as exc:
            logger.error(f"[Scrape] get_urls_parallel: {exc}", exc_info=True)
            return [None] * len(urls)
