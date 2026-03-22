"""
Web scraping service using pydoll (headless Chromium + Cloudflare auto-solve).

Design
------
* Browser opens ONCE per process.
* Three persistent tabs are created and kept alive forever:
    _tab_page   → get_page_content()
    _tab_title  → cinefreak_title()
    _tab_url    → get_url()
* Each tab calls ``tab.enable_auto_solve_cloudflare_captcha()`` once.
  From that point Pydoll automatically detects and clicks the CF Turnstile
  on EVERY navigation — no manual bypass, no session expiry issues.
* Sync Django / Django-Q code uses _session.run() to dispatch async work.
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
    One Chrome browser, three persistent tabs, CF auto-solve enabled on all tabs.
    Cloudflare is handled automatically on every navigation forever.
    """

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True, name="pydoll-loop"
        )
        self._thread.start()

        self._tab_page  = None   # get_page_content
        self._tab_title = None   # cinefreak_title
        self._tab_url   = None   # get_url

        self._started = False
        self._start_lock = threading.Lock()

    # ── public sync helpers ──────────────────────────────────────────────────

    def run(self, coro, timeout: float = 120):
        """Dispatch *coro* to the bg loop; block until done or *timeout*."""
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    def ensure_ready(self):
        """Start browser + enable CF auto-solve on all tabs (idempotent)."""
        with self._start_lock:
            if self._started:
                return
            logger.info("[Browser] Starting Chrome (3 tabs, CF auto-solve)...")
            ready = threading.Event()
            asyncio.run_coroutine_threadsafe(
                self._browser_lifetime(ready), self._loop
            )
            if not ready.wait(timeout=60):
                raise RuntimeError("Chrome failed to start within 60 s")
            self._started = True
            logger.info("[Browser] Ready — CF auto-solve active on all tabs")

    # ── private async internals ───────────────────────────────────────────────

    @staticmethod
    def _chrome_options():
        from pydoll.browser.options import ChromiumOptions
        opts = ChromiumOptions()

        # Anti-detection (matches official pydoll CF-bypass example)
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

        # RAM savers
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-sync")
        opts.add_argument("--disable-translate")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--no-first-run")
        opts.add_argument("--window-size=1280,720")

        opts.block_notifications = True
        opts.block_popups = True
        opts.password_manager_enabled = False

        return opts

    @staticmethod
    async def _prepare_tab(tab, name: str):
        """Enable CF auto-solve on *tab* and log it."""
        await tab.enable_auto_solve_cloudflare_captcha()
        logger.info(f"[Browser] Tab '{name}' created — CF auto-solve ON")

    async def _browser_lifetime(self, ready: threading.Event):
        """
        Launch Chrome, create 3 persistent tabs with CF auto-solve,
        then idle forever.
        """
        from pydoll.browser.chromium import Chrome

        logger.info("[Browser] Launching Chrome process...")
        async with Chrome(options=self._chrome_options()) as browser:

            # Tab 1 — page content
            self._tab_page = await browser.start()
            await self._prepare_tab(self._tab_page, "page")

            # Tab 2 — title lookups
            self._tab_title = await browser.new_tab()
            await self._prepare_tab(self._tab_title, "title")

            # Tab 3 — URL / redirect resolution
            self._tab_url = await browser.new_tab()
            await self._prepare_tab(self._tab_url, "url")

            ready.set()   # unblock ensure_ready()
            logger.info("[Browser] All tabs ready")

            # Keep async-with context alive until process exits
            while True:
                await asyncio.sleep(30)

    # ── per-tab navigation helpers ────────────────────────────────────────────

    async def _navigate(self, tab, url: str, settle: float = 3.0) -> str:
        """Navigate *tab* to *url*, settle *settle* s, return HTML.
        CF is handled automatically by the tab's auto-solve engine."""
        logger.info(f"[Tab] → {url}")
        await asyncio.wait_for(tab.go_to(url), timeout=30)
        logger.info(f"[Tab] Loaded — settling {settle}s for JS / CF...")
        await asyncio.sleep(settle)
        html = await tab.page_source
        logger.info(f"[Tab] Got {len(html):,} bytes")
        return html

    async def _navigate_and_follow(self, tab, url: str, max_poll: int = 10):
        """
        Navigate *tab* to *url*, poll until JS redirects settle.
        CF is handled automatically. Returns ``(final_url, page_html)``.
        """
        logger.info(f"[Tab] redirect-follow → {url}")
        await asyncio.wait_for(tab.go_to(url), timeout=30)
        prev = url
        for i in range(max_poll):
            await asyncio.sleep(1)
            current = await tab.current_url
            logger.info(f"[Tab] poll {i+1}/{max_poll} — {current}")
            if current != prev and "generate.php" not in current:
                logger.info(f"[Tab] Settled → {current}")
                break
            prev = current
        html = await tab.page_source
        logger.info(f"[Tab] Final: {current} | {len(html):,} bytes")
        return current, html


# Module-level singleton — one browser per worker process
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
        """Navigate Tab 1 (page) to *url*, extract content block, return Markdown."""
        logger.info(f"[Scrape] get_page_content → {url}")
        try:
            _session.ensure_ready()
            html = _session.run(
                _session._navigate(_session._tab_page, url, settle=3.0),
                timeout=90,
            )
            node = LexborHTMLParser(html).css_first(selector)
            if node:
                raw = node.html
                cleaned = WebScrapeService._to_markdown(raw)
                logger.info(
                    f"[Scrape] Extracted {len(raw):,} → {len(cleaned):,} chars "
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
        """Navigate Tab 2 (title) to *url*, return h1 title text."""
        logger.info(f"[Scrape] cinefreak_title → {url}")
        try:
            _session.ensure_ready()
            html = _session.run(
                _session._navigate(_session._tab_title, url, settle=3.0),
                timeout=90,
            )
            node = LexborHTMLParser(html).css_first("div.single-service-content h1")
            if node:
                title = node.text(strip=True)
                logger.info(f"[Scrape] Title: {title}")
                return title
            logger.warning(f"[Scrape] h1 not found at {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] cinefreak_title({url}): {exc}", exc_info=True)
            return None

    @staticmethod
    def get_url(url: str):
        """
        Navigate Tab 3 (url) to *url*, follow JS redirects, return download links.
        Returns a list of R2 / Google-video URLs, or None.
        """
        logger.info(f"[Scrape] get_url → {url}")
        try:
            _session.ensure_ready()
            final_url, html = _session.run(
                _session._navigate_and_follow(_session._tab_url, url, max_poll=10),
                timeout=90,
            )

            # 1. The final URL itself is a download link
            if any(m in final_url for m in (
                "r2.dev", "r2.cloudflarestorage.com",
                "video-downloads.googleusercontent.com",
            )):
                logger.info(f"[Scrape] Direct link: {final_url}")
                return [final_url]

            # 2. Scan page HTML for link patterns
            links = WebScrapeService._scan_links(html)
            if links:
                logger.info(f"[Scrape] Found {len(links)} link(s) in HTML")
                return links

            # 3. Fallback: try /w/ and /gp/ URL variants
            logger.info("[Scrape] No links — trying /w/ and /gp/ variants")
            for variant in (
                final_url.replace("/f/", "/w/"),
                final_url.replace("/f/", "/gp/"),
            ):
                if variant == final_url:
                    continue
                logger.info(f"[Scrape] Variant: {variant}")
                try:
                    _, vh = _session.run(
                        _session._navigate_and_follow(
                            _session._tab_url, variant, max_poll=6
                        ),
                        timeout=60,
                    )
                    links = WebScrapeService._scan_links(vh)
                    if links:
                        logger.info(f"[Scrape] Found {len(links)} link(s) in variant")
                        return links
                except Exception as ve:
                    logger.warning(f"[Scrape] Variant failed: {ve}")

            logger.warning(f"[Scrape] No download links found for: {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] get_url({url}): {exc}", exc_info=True)
            return None
