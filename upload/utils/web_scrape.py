"""
Web scraping service using pydoll (headless Chromium + Cloudflare auto-solve).

Design
------
* Each public method uses asyncio.run() — simple, reliable, no background threads.
* Cloudflare is handled automatically via enable_auto_solve_cloudflare_captcha().
* get_page_content  → navigate → CSS selector → return Markdown
* cinefreak_title   → navigate → h1 text
* get_url           → navigate → extract download URL from page HTML automatically
"""

import asyncio
import io
import logging
import re
import sys

from markitdown import MarkItDown
from selectolax.lexbor import LexborHTMLParser

logger = logging.getLogger(__name__)
_markitdown = MarkItDown()

# Suppress pydoll internal CDP/websocket logs
logging.getLogger("pydoll").setLevel(logging.WARNING)


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


def _run(coro):
    """Run async coroutine safely from sync Django/Django-Q context."""
    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    else:
        return asyncio.run(coro)


# ── Public scraping service ───────────────────────────────────────────────────

class WebScrapeService:

    @staticmethod
    def _to_markdown(html: str) -> str:
        buf = io.BytesIO(html.encode("utf-8"))
        md = _markitdown.convert_stream(buf, file_extension=".html").text_content
        return re.sub(r"\n{3,}", "\n\n", md).strip()

    # ── 1. get_page_content ───────────────────────────────────────────────────

    @staticmethod
    def get_page_content(url: str, selector: str = "div.content-grid.container"):
        """Navigate to *url*, extract *selector* block, return Markdown."""
        logger.info(f"[Scrape] get_page_content → {url}")

        async def _impl():
            from pydoll.browser.chromium import Chrome
            async with Chrome(options=_chrome_options()) as browser:
                tab = await browser.start()
                await tab.enable_auto_solve_cloudflare_captcha()
                await asyncio.wait_for(tab.go_to(url), timeout=30)
                await asyncio.sleep(3)
                return await tab.page_source

        try:
            html = _run(_impl())
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

    # ── 2. cinefreak_title ────────────────────────────────────────────────────

    @staticmethod
    def cinefreak_title(url: str):
        """Navigate to *url*, return h1 title text."""
        logger.info(f"[Scrape] cinefreak_title → {url}")

        async def _impl():
            from pydoll.browser.chromium import Chrome
            async with Chrome(options=_chrome_options()) as browser:
                tab = await browser.start()
                await tab.enable_auto_solve_cloudflare_captcha()
                await asyncio.wait_for(tab.go_to(url), timeout=30)
                await asyncio.sleep(3)
                return await tab.page_source

        try:
            html = _run(_impl())
            node = LexborHTMLParser(html).css_first("div.content-grid.container h1")
            if node:
                title = node.text(strip=True)
                logger.info(f"[Scrape] Title: {title}")
                return title
            logger.warning(f"[Scrape] h1 not found at {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] cinefreak_title({url}): {exc}", exc_info=True)
            return None

    # ── 3. get_url ────────────────────────────────────────────────────────────

    @staticmethod
    def get_url(url: str):
        """
        Navigate to *url*, get full page HTML,
        auto-extract download URL from page source.

        Supports:
          - generate.php pages  → extracts window.location.href cinecloud URL
          - R2 / Google-video   → scans href patterns in HTML
        Returns a list of URLs or None.
        """
        logger.info(f"[Scrape] get_url → {url}")

        async def _impl():
            from pydoll.browser.chromium import Chrome
            async with Chrome(options=_chrome_options()) as browser:
                tab = await browser.start()
                await tab.enable_auto_solve_cloudflare_captcha()
                await asyncio.wait_for(tab.go_to(url), timeout=30)
                await asyncio.sleep(2)
                return await tab.page_source

        try:
            html = _run(_impl())

            # 1. generate.php → extract hardcoded cinecloud URL from page JS
            match = re.search(
                r'window\.location\.href\s*=\s*["\']([^"\']*cinecloud\.site[^"\']*)["\']',
                html,
            )
            if match:
                link = match.group(1)
                logger.info(f"[Scrape] Extracted cinecloud URL: {link}")
                return [link]

            # 2. Scan for R2 / Google-video links in href attributes
            for pattern in (
                re.compile(
                    r'href=["\'](?P<u>(?:https?:)?//[^"\']*'
                    r'(?:\.r2\.dev|r2\.cloudflarestorage\.com)[^"\']*)["\']'
                ),
                re.compile(
                    r'href=["\'](?P<u>https://video-downloads\.googleusercontent\.com[^"\']*)["\']'
                ),
            ):
                hits = pattern.findall(html)
                if hits:
                    logger.info(f"[Scrape] Found {len(hits)} link(s) in HTML")
                    return hits

            logger.warning(f"[Scrape] No download links found for: {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] get_url({url}): {exc}", exc_info=True)
            return None
