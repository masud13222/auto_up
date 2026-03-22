"""
Web scraping service using pydoll (headless Chromium + Cloudflare bypass).

A single browser session is maintained per process in a background event-loop
thread. Cloudflare is bypassed once on first use; subsequent calls reuse the
same tab. Thread-safe for sync Django/Django-Q callers.
"""

import asyncio
import threading
import re
import io
import logging

from selectolax.lexbor import LexborHTMLParser
from markitdown import MarkItDown

logger = logging.getLogger(__name__)
_markitdown = MarkItDown()


# ── Browser Session (one per process) ────────────────────────────────────────

class _BrowserSession:
    """
    Owns a single persistent Chromium tab running in a dedicated background
    event-loop thread.  Sync callers use ``run()`` to dispatch coroutines.
    """

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True, name="pydoll-loop"
        )
        self._thread.start()

        self._tab = None
        self._started = False
        self._start_lock = threading.Lock()
        self._keep_alive = threading.Event()   # cleared → background loop exits

    # ── public sync helpers ──────────────────────────────────────────────────

    def run(self, coro, timeout=120):
        """Submit a coroutine to the bg loop and block until it finishes."""
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    def ensure_ready(self):
        """Guarantee the browser is started (idempotent, thread-safe)."""
        with self._start_lock:
            if self._started:
                return
            ready = threading.Event()
            asyncio.run_coroutine_threadsafe(
                self._browser_lifetime(ready), self._loop
            )
            if not ready.wait(timeout=90):
                raise RuntimeError("Browser startup timed out after 90 s")
            self._started = True

    # ── private async internals ──────────────────────────────────────────────

    @staticmethod
    def _make_options():
        from pydoll.browser.options import ChromiumOptions
        opts = ChromiumOptions()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        )
        return opts

    async def _browser_lifetime(self, ready: threading.Event):
        """Long-running coroutine: owns the Chrome context manager,
        bypasses Cloudflare once, then idles until the process exits."""
        from pydoll.browser.chromium import Chrome
        async with Chrome(options=self._make_options()) as browser:
            self._tab = await browser.start()
            logger.info("Browser started — bypassing Cloudflare...")
            try:
                async with self._tab.expect_and_bypass_cloudflare_captcha(
                    time_to_wait_captcha=20
                ):
                    await self._tab.go_to("https://cinefreak.net")
                logger.info("Cloudflare bypassed successfully.")
            except Exception as exc:
                # Site may not be serving a CF challenge right now — carry on.
                logger.warning(f"Cloudflare bypass skipped: {exc}")

            ready.set()          # unblock ensure_ready()

            # Keep context alive until process exits
            while True:
                await asyncio.sleep(30)

    # ── navigation helpers (called from sync code via run()) ─────────────────

    async def _page_source(self, url: str, settle: float = 3.0) -> str:
        """Navigate to ``url``, wait for JS to settle, return full page HTML."""
        await self._tab.go_to(url)
        await asyncio.sleep(settle)
        return await self._tab.page_source

    async def _resolve_url(self, url: str, max_wait: int = 10):
        """Navigate to ``url`` and poll until the browser stops redirecting.
        Returns ``(final_url, page_html)``."""
        await self._tab.go_to(url)
        prev = url
        for _ in range(max_wait):
            await asyncio.sleep(1)
            current = await self._tab.current_url
            # Stop polling once URL is stable and we've left the generate page
            if current != prev and "generate.php" not in current:
                break
            prev = current
        source = await self._tab.page_source
        return current, source


# Module-level singleton (one per process)
_session = _BrowserSession()


# ── Public Service ────────────────────────────────────────────────────────────

class WebScrapeService:

    # Patterns for extracting download links from HTML
    _RE_R2 = re.compile(
        r'href=["\'](?P<u>(?:https?:)?//[^"\']*'
        r'(?:\.r2\.dev|r2\.cloudflarestorage\.com)[^"\']*)["\']'
    )
    _RE_VIDEO = re.compile(
        r'href=["\'](?P<u>https://video-downloads\.googleusercontent\.com[^"\']*)["\']'
    )

    # ── internal helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _to_markdown(html: str) -> str:
        buf = io.BytesIO(html.encode("utf-8"))
        md = _markitdown.convert_stream(buf, file_extension=".html").text_content
        return re.sub(r"\n{3,}", "\n\n", md).strip()

    @staticmethod
    def _find_links(html: str):
        """Return the first non-empty list of download links found in *html*,
        or ``None`` if nothing matches."""
        for pattern in (WebScrapeService._RE_R2, WebScrapeService._RE_VIDEO):
            hits = pattern.findall(html)
            if hits:
                return hits
        return None

    # ── public methods ───────────────────────────────────────────────────────

    @staticmethod
    def get_page_content(url: str, selector: str = "div.single-service-content"):
        """Fetch *url*, extract the content container, return as Markdown."""
        try:
            _session.ensure_ready()
            logger.debug(f"Fetching page: {url}")
            html = _session.run(_session._page_source(url, settle=3.0), timeout=90)
            node = LexborHTMLParser(html).css_first(selector)
            if node:
                raw = node.html
                cleaned = WebScrapeService._to_markdown(raw)
                logger.debug(
                    f"Content extracted: {len(raw)} chars → {len(cleaned)} chars "
                    f"({100 - len(cleaned) / len(raw) * 100:.0f}% reduction)"
                )
                return cleaned
            logger.warning(f"Selector '{selector}' not found at {url}")
            return None
        except Exception as exc:
            logger.error(f"get_page_content({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def cinefreak_title(url: str):
        """Return the h1 text from the content container of *url*."""
        try:
            _session.ensure_ready()
            html = _session.run(_session._page_source(url, settle=3.0), timeout=90)
            node = LexborHTMLParser(html).css_first("div.single-service-content h1")
            if node:
                title = node.text(strip=True)
                logger.debug(f"Title: {title}")
                return title
            logger.warning(f"h1 not found at {url}")
            return None
        except Exception as exc:
            logger.error(f"cinefreak_title({url}): {exc}")
            return None

    @staticmethod
    def get_url(url: str):
        """Resolve a generate.php URL to actual download links.

        The real browser executes the page's JavaScript automatically, so
        ``window.location.href`` redirects are followed without any manual
        regex tricks.  Returns a list of URLs or ``None``.
        """
        try:
            _session.ensure_ready()
            logger.debug(f"Resolving: {url}")
            final_url, html = _session.run(
                _session._resolve_url(url, max_wait=10), timeout=90
            )

            # 1. Current URL itself might already be the download target
            if any(
                marker in final_url
                for marker in ("r2.dev", "r2.cloudflarestorage.com",
                               "video-downloads.googleusercontent.com")
            ):
                logger.info(f"Resolved direct → {final_url}")
                return [final_url]

            # 2. Scan page HTML for known link patterns
            links = WebScrapeService._find_links(html)
            if links:
                logger.info(f"Found {len(links)} link(s) in page HTML")
                return links

            # 3. Try /w/ and /gp/ URL variants
            logger.info(f"No links found — trying URL variants for: {final_url}")
            for variant in (
                final_url.replace("/f/", "/w/"),
                final_url.replace("/f/", "/gp/"),
            ):
                if variant == final_url:
                    continue
                try:
                    _, vh = _session.run(
                        _session._resolve_url(variant, max_wait=6), timeout=60
                    )
                    links = WebScrapeService._find_links(vh)
                    if links:
                        logger.info(f"Found {len(links)} link(s) in variant {variant}")
                        return links
                except Exception as ve:
                    logger.warning(f"Variant {variant} failed: {ve}")

            logger.warning(f"No download links found for: {url}")
            return None

        except Exception as exc:
            logger.error(f"get_url({url}): {exc}", exc_info=True)
            return None
