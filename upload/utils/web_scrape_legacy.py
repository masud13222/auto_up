"""
Web scraping service — LEGACY approach (pydoll-managed Chrome).

WARNING: This file is kept for reference only. DO NOT use in production.
         The active implementation is web_scrape.py (Remote-Connect Architecture).

Why this approach was abandoned:
    pydoll's internal ``get_browser_ws_address()`` uses ``aiohttp`` to query
    ``http://localhost:PORT/json/version``. In this Docker environment aiohttp
    raises ``ssl:default [Connect call failed]`` for plain HTTP on localhost —
    regardless of aiohttp version or connector settings. This is an
    environment-level bug confirmed via diagnostic scripts (test_chrome*.py).

    Additionally, ``browser.start()`` (pydoll-managed launch) has no retry logic,
    no zombie-process cleanup, and no way to override the aiohttp call that
    causes the failure.

Legacy approach summary:
    - pydoll.Chrome(options=opts) + browser.start()  →  pydoll manages Chrome
    - pydoll internally uses aiohttp for /json/version → ssl:default bug in Docker
    - Random port (randint 9223-9322)                →  port conflict possible
    - _kill_zombie_chrome killed ALL chrome procs     →  collateral damage possible
    - No subprocess handle                            →  hard to track/kill on crash

Current approach (web_scrape.py) summary:
    - subprocess.Popen(chrome_args)                  →  we own the process
    - urllib.request for /json/version               →  stdlib, zero SSL quirks
    - browser.connect(ws_url)                        →  pure WebSocket, no aiohttp
    - Fixed port (env-var override)                  →  deterministic, no conflicts
    - pkill filtered by port flag                    →  safe, targeted cleanup
"""

import asyncio
import io
import json
import logging
import re
import threading
import time

from markitdown import MarkItDown
from pydoll.browser.chromium import Chrome
from pydoll.browser.options import ChromiumOptions
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

_CONTENT_SELECTOR_FALLBACK = "div.single-service-content"

logging.getLogger("pydoll").setLevel(logging.WARNING)
logging.getLogger("pydoll.browser.tab").setLevel(logging.ERROR)
logging.getLogger("pydoll.connection.connection_handler").setLevel(logging.ERROR)

_MAX_RESTART_ATTEMPTS = 3
_RESTART_BACKOFF = [2, 5, 10]
_FETCH_HTML_SUBMIT_TIMEOUT = 240


# ── Chrome options (pydoll-managed) ──────────────────────────────────────────

def _chrome_options() -> ChromiumOptions:
    """
    Build ChromiumOptions for pydoll-managed Chrome.

    NOTE: pydoll picks a random port internally (randint 9223-9322).
          opts.start_timeout controls how long pydoll waits for the browser
          to respond on that port — using aiohttp, which is broken in Docker.
    """
    opts = ChromiumOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--enable-webgl")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-translate")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--window-size=1280,720")
    opts.add_argument("--no-zygote")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    )
    # pydoll's own startup timeout (seconds) — but aiohttp is still used internally
    opts.start_timeout = 60
    return opts


# ── Zombie killer (all Chrome — no port filter) ───────────────────────────────

def _kill_zombie_chrome():
    """
    Kill all Chrome processes by name (broad — no port filter).
    Sync — must be called via asyncio.to_thread from async context.

    PROBLEM: This kills *any* Chrome on the machine, not just ours.
             The current implementation in web_scrape.py filters by port flag.
    """
    import subprocess
    try:
        result = subprocess.run(
            ["pkill", "-9", "-f", "google-chrome"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            logger.info("[Browser/legacy] Killed zombie Chrome processes")
        time.sleep(1)
    except Exception as exc:
        logger.debug(f"[Browser/legacy] pkill chrome: {exc}")


# ── Per-Worker Browser Singleton (legacy) ────────────────────────────────────

_event_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_browser: Chrome | None = None
_browser_lock: asyncio.Lock | None = None


def _get_persistent_loop() -> asyncio.AbstractEventLoop:
    global _event_loop, _loop_thread
    if _event_loop is None or _event_loop.is_closed():
        _event_loop = asyncio.new_event_loop()
        _loop_thread = threading.Thread(
            target=_event_loop.run_forever,
            name="pydoll-legacy-loop",
            daemon=True,
        )
        _loop_thread.start()
        logger.info("[Browser/legacy] Persistent asyncio event loop started")
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


async def _launch_browser():
    """
    Let pydoll manage Chrome startup.

    FAILURE MODE in Docker:
        pydoll calls get_browser_ws_address() which uses aiohttp.ClientSession
        to GET http://localhost:PORT/json/version. In this Docker environment,
        aiohttp incorrectly attempts SSL handshake (ssl:default) on a plain HTTP
        URL → ConnectionError → FailedToStartBrowser / TimeoutError.

        This is NOT fixed by pinning aiohttp versions.
    """
    global _browser
    last_exc: Exception = RuntimeError("never tried")

    for attempt in range(_MAX_RESTART_ATTEMPTS):
        if attempt > 0:
            backoff = _RESTART_BACKOFF[min(attempt - 1, len(_RESTART_BACKOFF) - 1)]
            logger.warning(
                f"[Browser/legacy] Launch attempt {attempt + 1}/{_MAX_RESTART_ATTEMPTS}, "
                f"waiting {backoff}s..."
            )
            await asyncio.sleep(backoff)

        await asyncio.to_thread(_kill_zombie_chrome)

        try:
            opts = _chrome_options()
            browser = Chrome(options=opts)

            # ─── THIS IS WHERE IT FAILS IN DOCKER ───
            # browser.__aenter__ → browser.start() → get_browser_ws_address()
            # → aiohttp.ClientSession.get("http://localhost:PORT/json/version")
            # → ssl:default error → FailedToStartBrowser
            await browser.__aenter__()

            _browser = browser
            logger.info("[Browser/legacy] Chrome started via pydoll")
            return

        except Exception as exc:
            last_exc = exc
            logger.warning(f"[Browser/legacy] Launch attempt {attempt + 1} failed: {exc}")
            try:
                await browser.__aexit__(None, None, None)
            except Exception:
                pass

    raise last_exc


async def _ensure_browser():
    if _browser is not None:
        return
    lock = await _get_browser_lock()
    async with lock:
        if _browser is None:
            await _launch_browser()


async def _restart_browser():
    global _browser
    lock = await _get_browser_lock()
    async with lock:
        logger.warning("[Browser/legacy] Restarting Chrome singleton...")
        if _browser is not None:
            try:
                await asyncio.wait_for(_browser.__aexit__(None, None, None), timeout=5)
            except Exception:
                pass
            _browser = None
        await asyncio.to_thread(_kill_zombie_chrome)
        await _launch_browser()


# ── HTML fetcher (legacy) ─────────────────────────────────────────────────────

async def _fetch_html_async(url: str, settle: float = 2.0) -> str:
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
                    logger.warning("[Browser/legacy] CF captcha helper missing — plain navigation")
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
                logger.warning(f"[Browser/legacy] Fetch failed: {exc} — restarting...")
                await _restart_browser()
            else:
                raise


def _fetch_html(url: str, settle: float = 2.0) -> str:
    normalized = normalize_http_url(url)
    gateway_fixed = normalize_download_gateway_path(normalized)
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
    from urllib.parse import urlparse
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


# ── Download page resolver (legacy) ──────────────────────────────────────────

async def _resolve_download_page_async(url: str):
    try:
        u = _prepare_nav_url(url)
        if _is_direct_media_url(u):
            return [u]

        html = await _fetch_html_async(u, 4.0)
        target_url = u
        match_loc = _PATTERN_LOC.search(html)

        if match_loc:
            raw_target = match_loc.group(1)
            target_url = normalize_download_gateway_path(normalize_http_url(raw_target))
            if _is_direct_media_url(target_url):
                return [target_url]
            html = await _fetch_html_async(target_url, 4.0)

        matches = _PATTERN_R2.findall(html)
        if matches:
            return matches

        video_matches = _PATTERN_VIDEO.findall(html)
        if video_matches:
            return video_matches

        logger.warning(f"[Scrape/legacy] No links found for URL: {url}")
        return None

    except Exception as exc:
        logger.error(f"[Scrape/legacy] resolve download page ({url}): {exc}", exc_info=True)
        return None


async def _resolve_download_pages_parallel(urls: list[str]) -> list:
    if not urls:
        return []
    sem = asyncio.Semaphore(2)

    async def _guarded(u):
        async with sem:
            return await _resolve_download_page_async(u)

    return await asyncio.gather(*(_guarded(u) for u in urls), return_exceptions=True)


# ── Public scraping service (legacy) ─────────────────────────────────────────

class WebScrapeServiceLegacy:
    """Legacy WebScrapeService — uses pydoll-managed Chrome (broken in Docker)."""

    normalize_http_url = staticmethod(normalize_http_url)
    normalize_download_gateway_path = staticmethod(normalize_download_gateway_path)

    @staticmethod
    def clean_html(html: str) -> str:
        buf = io.BytesIO(html.encode("utf-8"))
        md = _markitdown.convert_stream(buf, file_extension=".html").text_content
        md = re.sub(r"\n{3,}", "\n\n", md).strip()
        md = truncate_markdown_for_llm(md)
        return sanitize_markdown_for_llm(md)

    @staticmethod
    def get_page_content(url: str, selector: str = "div.content-grid.container"):
        try:
            html = _fetch_html(url, settle=5.0)
            parser = LexborHTMLParser(html)
            node = parser.css_first(selector) or parser.css_first(_CONTENT_SELECTOR_FALLBACK)
            if node:
                raw_html = absolutize_resource_urls(node.html, url)
                return WebScrapeServiceLegacy.clean_html(raw_html)
            return None
        except Exception as exc:
            logger.error(f"[Scrape/legacy] get_page_content({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def cinefreak_title(url: str):
        try:
            html = _fetch_html(url, settle=3.0)
            parser = LexborHTMLParser(html)
            node = (
                parser.css_first("div.content-grid.container h1")
                or parser.css_first(f"{_CONTENT_SELECTOR_FALLBACK} h1")
            )
            return node.text(strip=True) if node else None
        except Exception as exc:
            logger.error(f"[Scrape/legacy] cinefreak_title({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def get_url(url: str):
        try:
            return _submit(_resolve_download_page_async(url), timeout=180)
        except Exception as exc:
            logger.error(f"[Scrape/legacy] get_url({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def get_urls_parallel(urls: list[str]) -> list:
        urls = [u for u in urls if u]
        if not urls:
            return []
        timeout = min(900, 120 + 90 * len(urls))
        try:
            return _submit(_resolve_download_pages_parallel(urls), timeout=timeout)
        except Exception as exc:
            logger.error(f"[Scrape/legacy] get_urls_parallel: {exc}", exc_info=True)
            return [None] * len(urls)
