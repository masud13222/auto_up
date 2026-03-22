"""
Web scraping service using pydoll (headless Chromium + Cloudflare bypass).

Design (matches the official pydoll pattern)
---------------------------------------------
* Browser opens ONCE per process.
* Cloudflare is bypassed ONCE on the persistent tab.
* All subsequent scraping navigates the SAME tab — CF session/cookies persist,
  so no second challenge appears.
* Sync Django / Django-Q code uses ``_session.run()`` to dispatch async
  coroutines to the background event-loop thread.
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
    Owns one persistent Chrome browser and one persistent tab.
    Cloudflare is bypassed once at startup.
    All scraping reuses the same tab (navigate → get HTML → repeat).
    """

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True, name="pydoll-loop"
        )
        self._thread.start()

        self._tab = None        # the one persistent tab
        self._started = False
        self._start_lock = threading.Lock()

    # ── public sync helpers ──────────────────────────────────────────────────

    def run(self, coro, timeout: float = 120):
        """Dispatch *coro* to the bg loop; block until done or *timeout*."""
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    def ensure_ready(self):
        """Start the browser + bypass CF (idempotent, thread-safe)."""
        with self._start_lock:
            if self._started:
                return
            logger.info("[Browser] Starting Chrome + CF bypass...")
            ready = threading.Event()
            asyncio.run_coroutine_threadsafe(
                self._browser_lifetime(ready), self._loop
            )
            if not ready.wait(timeout=120):
                raise RuntimeError("Browser/Cloudflare startup timed out after 120 s")
            self._started = True
            logger.info("[Browser] Ready — CF session active on persistent tab")

    # ── private async internals ───────────────────────────────────────────────

    @staticmethod
    def _chrome_options():
        from pydoll.browser.options import ChromiumOptions
        opts = ChromiumOptions()

        # Headless + anti-detection (matches the official pydoll example)
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--headless=new")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        )
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--enable-webgl")   # improves CF detection evasion

        # RAM savers
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-sync")
        opts.add_argument("--disable-translate")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--no-first-run")
        opts.add_argument("--window-size=1280,720")

        # Pydoll preference API
        opts.block_notifications = True
        opts.block_popups = True
        opts.password_manager_enabled = False

        return opts

    async def _browser_lifetime(self, ready: threading.Event):
        """
        Long-lived coroutine: keeps Chrome + the persistent tab alive forever.
        Bypasses Cloudflare once before signalling ready.
        """
        from pydoll.browser.chromium import Chrome

        logger.info("[Browser] Launching Chrome process...")
        async with Chrome(options=self._chrome_options()) as browser:
            self._tab = await browser.start()
            logger.info("[Browser] Chrome started — beginning Cloudflare bypass...")

            try:
                async with self._tab.expect_and_bypass_cloudflare_captcha(
                    time_to_wait_captcha=15
                ):
                    await asyncio.wait_for(
                        self._tab.go_to("https://cinefreak.net"),
                        timeout=45,
                    )
                logger.info("[CF] Cloudflare bypassed successfully!")
            except asyncio.TimeoutError:
                logger.warning("[CF] go_to() timed out — continuing without bypass")
            except Exception as exc:
                logger.warning(f"[CF] Bypass skipped: {exc}")

            ready.set()   # unblock ensure_ready()

            # Keep async-with context alive until the process exits
            while True:
                await asyncio.sleep(30)

    # ── navigation helpers ────────────────────────────────────────────────────

    async def _navigate(self, url: str, settle: float = 3.0) -> str:
        """Navigate the persistent tab to *url*, wait *settle* s, return HTML."""
        logger.info(f"[Tab] Navigating → {url}")
        await asyncio.wait_for(self._tab.go_to(url), timeout=30)
        logger.info(f"[Tab] Page loaded — settling {settle}s...")
        await asyncio.sleep(settle)
        html = await self._tab.page_source
        logger.info(f"[Tab] Got page source ({len(html):,} bytes)")
        return html

    async def _navigate_and_follow(self, url: str, max_poll: int = 10):
        """
        Navigate to *url* and poll until JS redirects stop.
        Returns ``(final_url, page_html)``.
        """
        logger.info(f"[Tab] Navigating (redirect-follow) → {url}")
        await asyncio.wait_for(self._tab.go_to(url), timeout=30)
        prev = url
        for i in range(max_poll):
            await asyncio.sleep(1)
            current = await self._tab.current_url
            logger.info(f"[Tab] Poll {i+1}/{max_poll} — {current}")
            if current != prev and "generate.php" not in current:
                logger.info(f"[Tab] Redirect settled → {current}")
                break
            prev = current
        html = await self._tab.page_source
        logger.info(f"[Tab] Final URL: {current} | HTML: {len(html):,} bytes")
        return current, html


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

    # ── public methods ────────────────────────────────────────────────────────

    @staticmethod
    def get_page_content(url: str, selector: str = "div.single-service-content"):
        """Navigate to *url*, extract the content block, return Markdown."""
        logger.info(f"[Scrape] get_page_content → {url}")
        try:
            _session.ensure_ready()
            html = _session.run(_session._navigate(url, settle=3.0), timeout=90)
            node = LexborHTMLParser(html).css_first(selector)
            if node:
                raw = node.html
                cleaned = WebScrapeService._to_markdown(raw)
                logger.info(
                    f"[Scrape] Content extracted: {len(raw):,} → {len(cleaned):,} chars "
                    f"({100 - len(cleaned)/len(raw)*100:.0f}% reduction)"
                )
                return cleaned
            logger.warning(f"[Scrape] Selector '{selector}' not found at {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] get_page_content({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def cinefreak_title(url: str):
        """Return the h1 title text from *url*'s content container."""
        logger.info(f"[Scrape] cinefreak_title → {url}")
        try:
            _session.ensure_ready()
            html = _session.run(_session._navigate(url, settle=3.0), timeout=90)
            node = LexborHTMLParser(html).css_first("div.single-service-content h1")
            if node:
                title = node.text(strip=True)
                logger.info(f"[Scrape] Title found: {title}")
                return title
            logger.warning(f"[Scrape] h1 not found at {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] cinefreak_title({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def get_url(url: str):
        """
        Resolve a generate.php URL → actual R2 / Google video download links.
        Returns a list of URLs, or None.
        """
        logger.info(f"[Scrape] get_url → {url}")
        try:
            _session.ensure_ready()
            final_url, html = _session.run(
                _session._navigate_and_follow(url, max_poll=10), timeout=90
            )

            # 1. Current URL itself is an R2/video link
            if any(m in final_url for m in (
                "r2.dev", "r2.cloudflarestorage.com",
                "video-downloads.googleusercontent.com"
            )):
                logger.info(f"[Scrape] Direct link: {final_url}")
                return [final_url]

            # 2. Scan page HTML for link patterns
            links = WebScrapeService._scan_links(html)
            if links:
                logger.info(f"[Scrape] Found {len(links)} link(s) in page HTML")
                return links

            # 3. Fallback: try /w/ and /gp/ URL variants
            logger.info(f"[Scrape] No links found — trying /w/ and /gp/ variants")
            for variant in (
                final_url.replace("/f/", "/w/"),
                final_url.replace("/f/", "/gp/"),
            ):
                if variant == final_url:
                    continue
                logger.info(f"[Scrape] Trying variant: {variant}")
                try:
                    _, vh = _session.run(
                        _session._navigate_and_follow(variant, max_poll=6), timeout=60
                    )
                    links = WebScrapeService._scan_links(vh)
                    if links:
                        logger.info(f"[Scrape] Found {len(links)} link(s) in variant")
                        return links
                except Exception as ve:
                    logger.warning(f"[Scrape] Variant {variant} failed: {ve}")

            logger.warning(f"[Scrape] No download links found for: {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] get_url({url}): {exc}", exc_info=True)
            return None
