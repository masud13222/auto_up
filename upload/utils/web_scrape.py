"""
Web scraping service using pydoll (headless Chromium + Cloudflare bypass).

Design
------
* The browser process starts ONCE per Django-Q worker process.
* Cloudflare is bypassed once on the initial tab at startup.
* Every individual scraping call opens a *new tab*, navigates, extracts,
  then closes the tab — so there is no shared mutable tab state between calls.
* Sync Django / Django-Q code uses ``_session.run()`` to dispatch coroutines
  to the background event-loop thread that owns the browser.
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


# ── Browser Session (one per worker process) ──────────────────────────────────

class _BrowserSession:
    """
    Owns one persistent Chrome browser in a dedicated background event-loop
    thread.  Cloudflare is bypassed once on the first (setup) tab.
    All real scraping requests get a *fresh tab* via ``browser.new_tab()``.
    """

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True, name="pydoll-loop"
        )
        self._thread.start()

        self._browser = None   # set inside _browser_lifetime coroutine
        self._started = False
        self._start_lock = threading.Lock()

    # ── public sync helpers ──────────────────────────────────────────────────

    def run(self, coro, timeout: float = 120):
        """Dispatch *coro* to the bg loop; block until done or *timeout*."""
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    def ensure_ready(self):
        """Start the browser if not already running (idempotent, thread-safe)."""
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

    # ── private async helpers ────────────────────────────────────────────────

    @staticmethod
    def _chrome_options():
        from pydoll.browser.options import ChromiumOptions
        opts = ChromiumOptions()

        # ── Headless / sandbox ──────────────────────────────────────────────
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1280,720")

        # ── Anti-detection ──────────────────────────────────────────────────
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        )

        # ── RAM savers (Chrome CLI flags) ───────────────────────────────────
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-sync")
        opts.add_argument("--disable-translate")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-component-extensions-with-background-pages")
        opts.add_argument("--no-first-run")
        opts.add_argument("--no-default-browser-check")
        opts.add_argument("--disable-hang-monitor")
        opts.add_argument("--process-per-site")

        # Disable images (saves ~30-50 MB per tab; CF bypass tab inherits this too
        # but Cloudflare headless bypass does not depend on image loading)
        opts.add_argument("--blink-settings=imagesEnabled=false")

        # Cap JS V8 heap per tab
        opts.add_argument("--js-flags=--max-old-space-size=64")

        # ── RAM savers via pydoll browser_preferences API ───────────────────
        # (These are proper Chrome profile prefs, not CLI args)
        opts.block_notifications = True
        opts.block_popups = True
        opts.password_manager_enabled = False

        return opts

    async def _browser_lifetime(self, ready: threading.Event):
        """
        Long-running coroutine that keeps the Chrome context alive.
        Opens once, bypasses Cloudflare on the initial tab, then idles.
        """
        from pydoll.browser.chromium import Chrome

        async with Chrome(options=self._chrome_options()) as browser:
            self._browser = browser

            setup_tab = await browser.start()
            logger.info("Browser started — bypassing Cloudflare...")
            try:
                async with setup_tab.expect_and_bypass_cloudflare_captcha(
                    time_to_wait_captcha=20
                ):
                    await setup_tab.go_to("https://cinefreak.net")
                logger.info("Cloudflare bypassed.")
            except Exception as exc:
                logger.warning(f"Cloudflare bypass skipped: {exc}")

            ready.set()

            while True:
                await asyncio.sleep(30)

    # ── tab-based navigation (used by WebScrapeService) ──────────────────────

    async def _fetch_source(self, url: str, settle: float = 3.0) -> str:
        """Open a new tab, navigate, wait *settle* seconds, return HTML, close tab."""
        tab = await self._browser.new_tab()
        try:
            await tab.go_to(url)
            await asyncio.sleep(settle)
            return await tab.page_source
        finally:
            await tab.close()

    async def _fetch_and_follow(self, url: str, max_poll: int = 10):
        """
        Open a new tab, navigate, poll until JS redirects settle,
        return ``(final_url, page_html)``, close tab.
        """
        tab = await self._browser.new_tab()
        try:
            await tab.go_to(url)
            prev = url
            for _ in range(max_poll):
                await asyncio.sleep(1)
                current = await tab.current_url
                # Stop as soon as the URL is stable and we've left generate.php
                if current != prev and "generate.php" not in current:
                    break
                prev = current
            source = await tab.page_source
            return current, source
        finally:
            await tab.close()


# Module-level singleton — one browser per process
_session = _BrowserSession()


# ── Public scraping service ───────────────────────────────────────────────────

class WebScrapeService:

    _RE_R2 = re.compile(
        r'href=["\'](?P<u>(?:https?:)?//[^"\']*'
        r'(?:\.r2\.dev|r2\.cloudflarestorage\.com)[^"\']*)["\']'
    )
    _RE_VIDEO = re.compile(
        r'href=["\'](?P<u>https://video-downloads\.googleusercontent\.com[^"\']*)["\']'
    )

    @staticmethod
    def _to_markdown(html: str) -> str:
        buf = io.BytesIO(html.encode("utf-8"))
        md = _markitdown.convert_stream(buf, file_extension=".html").text_content
        return re.sub(r"\n{3,}", "\n\n", md).strip()

    @staticmethod
    def _scan_links(html: str):
        """Return first non-empty list of R2/video download links, or None."""
        for pat in (WebScrapeService._RE_R2, WebScrapeService._RE_VIDEO):
            hits = pat.findall(html)
            if hits:
                return hits
        return None

    # ── public methods ───────────────────────────────────────────────────────

    @staticmethod
    def get_page_content(url: str, selector: str = "div.single-service-content"):
        """Fetch *url* in a new tab, extract the content block, return Markdown."""
        try:
            _session.ensure_ready()
            logger.debug(f"Fetching page: {url}")
            html = _session.run(_session._fetch_source(url, settle=3.0), timeout=90)
            node = LexborHTMLParser(html).css_first(selector)
            if node:
                raw = node.html
                cleaned = WebScrapeService._to_markdown(raw)
                logger.debug(
                    f"Extracted: {len(raw)} → {len(cleaned)} chars "
                    f"({100 - len(cleaned)/len(raw)*100:.0f}% reduction)"
                )
                return cleaned
            logger.warning(f"Selector '{selector}' not found at {url}")
            return None
        except Exception as exc:
            logger.error(f"get_page_content({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def cinefreak_title(url: str):
        """Return the h1 title text from *url*'s content container."""
        try:
            _session.ensure_ready()
            html = _session.run(_session._fetch_source(url, settle=3.0), timeout=90)
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
        """
        Resolve a generate.php URL → actual R2 / Google video download links.

        The browser executes JS automatically, so ``window.location.href``
        redirects are followed natively.  Returns a list of URLs or ``None``.
        """
        try:
            _session.ensure_ready()
            logger.debug(f"Resolving: {url}")
            final_url, html = _session.run(
                _session._fetch_and_follow(url, max_poll=10), timeout=90
            )

            # 1. Current URL is itself an R2/video link
            if any(m in final_url for m in (
                "r2.dev", "r2.cloudflarestorage.com",
                "video-downloads.googleusercontent.com"
            )):
                logger.info(f"Direct link resolved: {final_url}")
                return [final_url]

            # 2. Scan page HTML for known link patterns
            links = WebScrapeService._scan_links(html)
            if links:
                logger.info(f"Found {len(links)} link(s) in page HTML")
                return links

            # 3. Try /w/ and /gp/ URL variants as fallback
            logger.info(f"No links found — trying URL variants for: {final_url}")
            for variant in (
                final_url.replace("/f/", "/w/"),
                final_url.replace("/f/", "/gp/"),
            ):
                if variant == final_url:
                    continue
                try:
                    _, vh = _session.run(
                        _session._fetch_and_follow(variant, max_poll=6), timeout=60
                    )
                    links = WebScrapeService._scan_links(vh)
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
