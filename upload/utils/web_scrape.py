"""
Web scraping service using pydoll (headless Chromium).

Browser Singleton Strategy (Approach 3 — Per-Worker Persistent Browser):
  - One persistent asyncio event loop runs in a background daemon thread per worker.
  - One Chrome instance is kept alive for the entire worker lifetime.
  - Every fetch opens a NEW TAB, does the work, then closes that tab.
  - On browser crash, it auto-restarts transparently (one retry).

Cloudflare Turnstile (pydoll docs — Behavioral Captcha Bypass, recommended pattern):
  Each navigation uses ``async with tab.expect_and_bypass_cloudflare_captcha(...): await tab.go_to(url)``.
  If Turnstile appears, pydoll waits (up to ``time_to_wait_captcha``) and performs the checkbox
  interaction; if no Turnstile shadow root appears in time, pydoll skips — no custom site logic here.
  We do not use per-tab ``enable_auto_solve_cloudflare_captcha()`` (that caused WebSocket HTTP 500
  noise on some hosts in practice).

Public API is unchanged — all callers (WebScrapeService.*) work as before.
"""

import asyncio
import io
import logging
import re
import threading

from markitdown import MarkItDown
from selectolax.lexbor import LexborHTMLParser

logger = logging.getLogger(__name__)
_markitdown = MarkItDown()

# Suppress pydoll internal CDP/websocket logs
logging.getLogger("pydoll").setLevel(logging.WARNING)
logging.getLogger("pydoll.browser.tab").setLevel(logging.ERROR)  # CF bypass WebSocket noise
logging.getLogger("pydoll.connection.connection_handler").setLevel(logging.ERROR)


# ── Chrome options ────────────────────────────────────────────────────────────

def _chrome_options():
    from pydoll.browser.options import ChromiumOptions
    opts = ChromiumOptions()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--headless=new")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    )
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--enable-webgl")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-translate")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--window-size=1280,720")
    opts.start_timeout = 30
    opts.block_notifications = True
    opts.block_popups = True
    opts.password_manager_enabled = False
    return opts


# ── Per-Worker Browser Singleton ──────────────────────────────────────────────
#
# Architecture:
#   One background daemon thread runs a persistent asyncio event loop.
#   All async operations are submitted to that loop via run_coroutine_threadsafe().
#   The Chrome browser is started once inside that loop and stays alive.
#   Every fetch call:  new_tab() → navigate → get HTML → tab.close()
#
# Why this works across Django-Q tasks:
#   Django-Q runs each task in the same worker process (ORM connection reuse etc.)
#   Module-level globals persist for the worker's lifetime → browser stays alive.
#   CF clearance cookie is in the browser's cookie store → no re-solve needed.

_event_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_browser = None                     # pydoll Chrome instance (shared)
_browser_lock: asyncio.Lock | None = None   # async lock for init


def _get_persistent_loop() -> asyncio.AbstractEventLoop:
    """Return (or create) the per-worker persistent asyncio event loop."""
    global _event_loop, _loop_thread
    if _event_loop is None or _event_loop.is_closed():
        _event_loop = asyncio.new_event_loop()
        _loop_thread = threading.Thread(
            target=_event_loop.run_forever,
            name="pydoll-worker-loop",
            daemon=True,        # dies automatically when worker process exits
        )
        _loop_thread.start()
        logger.info("[Browser] Persistent asyncio event loop started")
    return _event_loop


def _submit(coro, timeout: int = 180):
    """Submit an async coroutine to the persistent loop and block until done."""
    loop = _get_persistent_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)


async def _get_browser_lock() -> asyncio.Lock:
    """Return (or create) the asyncio.Lock used to guard browser init."""
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _launch_browser():
    """Start Chrome singleton (warmup tab only; Turnstile handling is per-navigation via pydoll CM)."""
    global _browser
    from pydoll.browser.chromium import Chrome
    b = Chrome(options=_chrome_options())
    await b.__aenter__()                            # start Chrome process
    await b.start()                                 # open 1st (warmup) tab
    _browser = b
    logger.info("[Browser] Chrome singleton started")


async def _ensure_browser():
    """Start browser if not already running. Safe for concurrent callers."""
    if _browser is not None:
        return
    lock = await _get_browser_lock()
    async with lock:
        if _browser is None:
            await _launch_browser()


async def _restart_browser():
    """Kill crashed browser and restart cleanly."""
    global _browser
    logger.warning("[Browser] Restarting Chrome singleton after crash...")
    if _browser is not None:
        try:
            await _browser.__aexit__(None, None, None)
        except Exception:
            pass
        _browser = None
    await _launch_browser()


async def _fetch_html_async(url: str, settle: float = 2.0) -> str:
    """
    Fetch HTML using the singleton browser.
    Opens a new tab, navigates, waits, returns HTML, closes tab.
    Auto-restarts browser once on crash.

    Turnstile: pydoll-recommended ``expect_and_bypass_cloudflare_captcha`` around ``go_to`` only.
    """
    await _ensure_browser()

    for attempt in range(2):
        try:
            tab = await _browser.new_tab()
            try:
                cm = getattr(tab, "expect_and_bypass_cloudflare_captcha", None)
                nav_timeout = 45.0
                if cm is not None:
                    # Docs: if shadow root does not appear within time_to_wait_captcha, interaction is skipped.
                    async with cm(time_to_wait_captcha=10):
                        await asyncio.wait_for(tab.go_to(url), timeout=nav_timeout)
                else:
                    logger.warning(
                        "[Browser] expect_and_bypass_cloudflare_captcha missing on this pydoll version; "
                        "plain navigation (no Turnstile helper)"
                    )
                    await asyncio.wait_for(tab.go_to(url), timeout=30)
                await asyncio.sleep(settle)
                return await tab.page_source
            finally:
                try:
                    await tab.close()
                except Exception:
                    pass

        except Exception as exc:
            if attempt == 0:
                logger.warning(f"[Browser] Fetch failed: {exc} — restarting browser, retrying...")
                await _restart_browser()
            else:
                raise


def _fetch_html(url: str, settle: float = 2.0) -> str:
    """Sync entry point: fetch HTML via singleton browser (blocks until done)."""
    return _submit(_fetch_html_async(url, settle))


# ── Public scraping service ───────────────────────────────────────────────────

class WebScrapeService:

    @staticmethod
    def clean_html(html: str) -> str:
        """Converts HTML to LLM-friendly Markdown. Collapses blank lines."""
        buf = io.BytesIO(html.encode("utf-8"))
        md = _markitdown.convert_stream(buf, file_extension=".html").text_content
        return re.sub(r"\n{3,}", "\n\n", md).strip()

    # ── 1. get_page_content ───────────────────────────────────────────────────

    @staticmethod
    def get_page_content(url: str, selector: str = "div.content-grid.container"):
        """Fetch page, extract selector block, return Markdown."""
        try:
            logger.info(f"[Scrape] get_page_content → {url}")
            html = _fetch_html(url, settle=3.0)
            node = LexborHTMLParser(html).css_first(selector)
            if node:
                raw_html = node.html
                cleaned = WebScrapeService.clean_html(raw_html)
                logger.info(
                    f"[Scrape] Extracted {len(raw_html):,} → {len(cleaned):,} chars "
                    f"({100 - len(cleaned)/len(raw_html)*100:.0f}% reduction)"
                )
                return cleaned
            logger.warning(f"[Scrape] Selector '{selector}' not found in {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] get_page_content({url}): {exc}", exc_info=True)
            return None

    # ── 2. cinefreak_title ────────────────────────────────────────────────────

    @staticmethod
    def cinefreak_title(url: str):
        """Fetch page, return h1 title text."""
        try:
            logger.info(f"[Scrape] cinefreak_title → {url}")
            html = _fetch_html(url, settle=3.0)
            node = LexborHTMLParser(html).css_first("div.content-grid.container h1")
            if node:
                title = node.text(strip=True)
                logger.info(f"[Scrape] Title: {title}")
                return title
            logger.warning(f"[Scrape] h1 not found in {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] cinefreak_title({url}): {exc}", exc_info=True)
            return None

    # ── 3. get_url ────────────────────────────────────────────────────────────

    @staticmethod
    def get_url(url: str):
        """
        Follows window.location.href redirects and extracts R2/video links.

        1. Navigate to url → find window.location.href → follow redirect
        2. Scan page for R2 links
        3. Scan page for video links
        4. Fallback: try /w/ and /gp/ variants
        """
        pattern_r2    = r'href=["\'](?P<url>(?:https?:)?//[^"\']*(?:\.r2\.dev|r2\.cloudflarestorage\.com)[^"\']*)["\']'
        pattern_video = r'href=["\'](?P<url>https://video-downloads\.googleusercontent\.com[^"\']*)["\']'
        pattern_loc   = r'window\.location\.href\s*=\s*["\'](.+?)["\']'

        try:
            logger.info(f"[Scrape] get_url → {url}")

            # 1. Initial request
            html = _fetch_html(url, settle=2.0)

            match_loc = re.search(pattern_loc, html)
            target_url = url

            if match_loc:
                target_url = match_loc.group(1)
                logger.debug(f"[Scrape] Found redirect URL: {target_url}")
                html = _fetch_html(target_url, settle=4)
            else:
                logger.debug(f"[Scrape] No redirect found, checking current page: {url}")

            # 2. R2 links
            matches = re.findall(pattern_r2, html)
            if matches:
                logger.info(f"[Scrape] Found {len(matches)} R2 link(s)")
                return matches

            # 3. Video links
            video_matches = re.findall(pattern_video, html)
            if video_matches:
                logger.info(f"[Scrape] Found {len(video_matches)} video link(s)")
                return video_matches

            # 4. Fallback: /w/ and /gp/ variants
            logger.info(f"[Scrape] No R2/video links found. Trying fallback for: {target_url}")
            if "/f/" in target_url:
                for fb_url in (
                    target_url.replace("/f/", "/w/"),
                    target_url.replace("/f/", "/gp/"),
                ):
                    try:
                        logger.debug(f"[Scrape] Checking fallback: {fb_url}")
                        fb_html = _fetch_html(fb_url, settle=5.0)
                        fb_matches = re.findall(pattern_video, fb_html)
                        if fb_matches:
                            logger.info(f"[Scrape] Found {len(fb_matches)} video link(s) in {fb_url}")
                            return fb_matches
                    except Exception as ve:
                        logger.warning(f"[Scrape] Fallback failed for {fb_url}: {ve}")

            # 5. Fallback: instant_{id}
            else:
                from urllib.parse import urlparse
                parsed = urlparse(target_url)
                path_parts = [p for p in parsed.path.strip("/").split("/") if p]
                if path_parts:
                    last_id = path_parts[-1]
                    instant_url = f"{parsed.scheme}://{parsed.netloc}/instant_{last_id}"
                    try:
                        logger.debug(f"[Scrape] Checking instant fallback: {instant_url}")
                        instant_html = _fetch_html(instant_url, settle=5.0)
                        instant_matches = re.findall(pattern_video, instant_html)
                        if instant_matches:
                            logger.info(f"[Scrape] Found {len(instant_matches)} video link(s) via instant fallback: {instant_url}")
                            return instant_matches
                        logger.warning(f"[Scrape] Instant fallback returned no video links: {instant_url}")
                    except Exception as ie:
                        logger.warning(f"[Scrape] Instant fallback failed for {instant_url}: {ie}")

            # 6. Nothing found
            logger.warning(f"[Scrape] No links found for URL: {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] get_url({url}): {exc}", exc_info=True)
            return None
