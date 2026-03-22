"""
Web scraping service using pydoll (headless Chromium + Cloudflare auto-solve).

Exact same logic as the old httpx version — only the HTTP client changed to pydoll.
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


async def _fetch_html(url: str, settle: float = 2.0) -> str:
    """Open Chrome, navigate to url, return page HTML."""
    from pydoll.browser.chromium import Chrome
    async with Chrome(options=_chrome_options()) as browser:
        tab = await browser.start()
        await tab.enable_auto_solve_cloudflare_captcha()
        await asyncio.wait_for(tab.go_to(url), timeout=30)
        await asyncio.sleep(settle)
        return await tab.page_source


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
            html = _run(_fetch_html(url, settle=3.0))
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
            html = _run(_fetch_html(url, settle=3.0))
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
        Exact same logic as the old httpx version.

        1. Navigate to url → find window.location.href → follow redirect
        2. Scan page for R2 links
        3. Scan page for video links
        4. Fallback: try /w/ and /gp/ variants
        """
        # Reusable patterns (same as old code)
        pattern_r2    = r'href=["\'](?P<url>(?:https?:)?//[^"\']*(?:\.r2\.dev|r2\.cloudflarestorage\.com)[^"\']*)["\']'
        pattern_video = r'href=["\'](?P<url>https://video-downloads\.googleusercontent\.com[^"\']*)["\']'
        pattern_loc   = r'window\.location\.href\s*=\s*["\'](.+?)["\']'

        try:
            logger.info(f"[Scrape] get_url → {url}")

            # 1. Initial request
            html = _run(_fetch_html(url, settle=2.0))

            # Find window.location.href redirect (same as old code)
            match_loc = re.search(pattern_loc, html)
            target_url = url

            if match_loc:
                target_url = match_loc.group(1)
                logger.debug(f"[Scrape] Found redirect URL: {target_url}")
                # Follow the redirect
                html = _run(_fetch_html(target_url, settle=2.0))
            else:
                logger.debug(f"[Scrape] No redirect found, checking current page: {url}")

            # 2. Check for R2 links
            matches = re.findall(pattern_r2, html)
            if matches:
                logger.info(f"[Scrape] Found {len(matches)} R2 link(s)")
                return matches

            # 3. Check for video links
            video_matches = re.findall(pattern_video, html)
            if video_matches:
                logger.info(f"[Scrape] Found {len(video_matches)} video link(s)")
                return video_matches

            # 4. Fallback: try /w/ and /gp/ variants
            logger.info(f"[Scrape] No R2/video links found. Trying fallback for: {target_url}")
            if "/f/" in target_url:
                for fb_url in (
                    target_url.replace("/f/", "/w/"),
                    target_url.replace("/f/", "/gp/"),
                ):
                    try:
                        logger.debug(f"[Scrape] Checking fallback: {fb_url}")
                        fb_html = _run(_fetch_html(fb_url, settle=2.0))
                        fb_matches = re.findall(pattern_video, fb_html)
                        if fb_matches:
                            logger.info(f"[Scrape] Found {len(fb_matches)} video link(s) in {fb_url}")
                            return fb_matches
                    except Exception as ve:
                        logger.warning(f"[Scrape] Fallback failed for {fb_url}: {ve}")

            # 5. Nothing found
            logger.warning(f"[Scrape] No links found for URL: {url}")
            return None
        except Exception as exc:
            logger.error(f"[Scrape] get_url({url}): {exc}", exc_info=True)
            return None
