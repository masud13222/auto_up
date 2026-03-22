"""
Web scraping service using pydoll (headless Chromium + Cloudflare auto-solve).

Design
------
* Each public method call uses asyncio.run() — simple, reliable, no threads.
  Proven to work on both Windows and Linux/Docker.
* Cloudflare handled automatically via enable_auto_solve_cloudflare_captcha().
* Chrome opens and closes per call using proper async with context manager.
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
    """Run *coro* safely from synchronous Django / Django-Q context."""
    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    else:
        return asyncio.run(coro)


# ── Core async navigation ─────────────────────────────────────────────────────

async def _navigate(tab, url: str, settle: float = 3.0) -> str:
    logger.debug(f"[Tab] → {url}")
    await asyncio.wait_for(tab.go_to(url), timeout=30)
    logger.debug(f"[Tab] Loaded — settling {settle}s...")
    await asyncio.sleep(settle)
    html = await tab.page_source
    logger.debug(f"[Tab] Got {len(html):,} bytes")
    return html


async def _navigate_and_follow(tab, url: str, max_poll: int = 10):
    logger.debug(f"[Tab] redirect-follow → {url}")
    await asyncio.wait_for(tab.go_to(url), timeout=30)
    prev = url
    for i in range(max_poll):
        await asyncio.sleep(1)
        current = await tab.current_url
        logger.debug(f"[Tab] poll {i+1}/{max_poll} — {current}")
        if current != prev and "generate.php" not in current:
            logger.debug(f"[Tab] Settled → {current}")
            break
        prev = current
    html = await tab.page_source
    logger.debug(f"[Tab] Final: {current} | {len(html):,} bytes")
    return current, html


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
        for pat in (WebScrapeService._RE_R2, WebScrapeService._RE_VIDEO):
            hits = pat.findall(html)
            if hits:
                return hits
        return None

    @staticmethod
    def get_page_content(url: str, selector: str = "div.content-grid.container"):
        logger.info(f"[Scrape] get_page_content → {url}")

        async def _impl():
            from pydoll.browser.chromium import Chrome
            async with Chrome(options=_chrome_options()) as browser:
                tab = await browser.start()
                await tab.enable_auto_solve_cloudflare_captcha()
                logger.debug("[Browser] Chrome started, CF auto-solve ON")
                return await _navigate(tab, url, settle=3.0)

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

    @staticmethod
    def cinefreak_title(url: str):
        logger.info(f"[Scrape] cinefreak_title → {url}")

        async def _impl():
            from pydoll.browser.chromium import Chrome
            async with Chrome(options=_chrome_options()) as browser:
                tab = await browser.start()
                await tab.enable_auto_solve_cloudflare_captcha()
                logger.debug("[Browser] Chrome started, CF auto-solve ON")
                return await _navigate(tab, url, settle=3.0)

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

    @staticmethod
    def get_url(url: str):
        logger.info(f"[Scrape] get_url → {url}")

        async def _impl(target_url):
            from pydoll.browser.chromium import Chrome
            async with Chrome(options=_chrome_options()) as browser:
                tab = await browser.start()
                await tab.enable_auto_solve_cloudflare_captcha()
                logger.debug("[Browser] Chrome started, CF auto-solve ON")
                return await _navigate_and_follow(tab, target_url, max_poll=10)

        try:
            final_url, html = _run(_impl(url))

            if any(m in final_url for m in (
                "r2.dev", "r2.cloudflarestorage.com",
                "video-downloads.googleusercontent.com",
            )):
                logger.info(f"[Scrape] Direct link: {final_url}")
                return [final_url]

            links = WebScrapeService._scan_links(html)
            if links:
                logger.info(f"[Scrape] Found {len(links)} link(s) in HTML")
                return links

            logger.info("[Scrape] No links — trying /w/ and /gp/ variants")
            for variant in (
                final_url.replace("/f/", "/w/"),
                final_url.replace("/f/", "/gp/"),
            ):
                if variant == final_url:
                    continue
                logger.info(f"[Scrape] Variant: {variant}")
                try:
                    _, vh = _run(_impl(variant))
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
