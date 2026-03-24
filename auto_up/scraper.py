"""
Scraper module for CineFreak homepage.

Uses pydoll (headless Chromium + Cloudflare auto-solve) to scrape the
homepage listing and return structured data about each entry.

HTML structure (confirmed):
  <div class="card-grid">
    <a href="URL" class="movie-card" aria-label="...">
      ...
      <div class="movie-card-content">
        <h3 class="movie-card-title">Title text here</h3>
      </div>
    </a>
  </div>
"""

import asyncio
import logging
import sys

from selectolax.lexbor import LexborHTMLParser

logger = logging.getLogger(__name__)

# Suppress pydoll internal CDP/websocket logs
logging.getLogger("pydoll").setLevel(logging.WARNING)


# ── Chrome options (shared with web_scrape.py) ────────────────────────────────

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


async def _scrape_homepage_async(url: str, settle: float = 4.0) -> list[dict]:
    """
    Open Chrome, navigate to homepage, parse card-grid, return entries.

    Selectors (from live HTML):
      Container : div.card-grid
      Each card : a.movie-card          (also has href + aria-label)
      Title     : h3.movie-card-title   (inside each a.movie-card)
    """
    from pydoll.browser.chromium import Chrome

    async with Chrome(options=_chrome_options()) as browser:
        tab = await browser.start()
        await tab.enable_auto_solve_cloudflare_captcha()
        await asyncio.wait_for(tab.go_to(url), timeout=30)
        await asyncio.sleep(settle)

        html = await tab.page_source

    # Parse with selectolax (fast, no JS needed at this point)
    parser = LexborHTMLParser(html)

    entries = []
    for card in parser.css("div.card-grid a.movie-card"):
        href = card.attrs.get("href", "").strip()
        if not href:
            continue

        title_node = card.css_first("h3.movie-card-title")
        if not title_node:
            # Fallback: use aria-label (also contains full title)
            aria = card.attrs.get("aria-label", "").strip()
            raw_title = aria if aria else ""
        else:
            raw_title = title_node.text(strip=True)

        if not raw_title:
            continue

        entries.append({
            "raw_title": raw_title,
            "url": href,
        })

    return entries


class CineFreakScraper:
    """Scrapes CineFreak homepage to discover new media entries."""

    HOMEPAGE_URL = "https://cinefreak.net/"

    @classmethod
    def scrape_homepage(cls) -> list[dict]:
        """
        Scrape the CineFreak homepage and return a list of entries.

        Uses pydoll (headless Chromium + Cloudflare auto-solve) so that
        Cloudflare JS challenges are handled automatically.

        Returns:
            List of dicts:
                {
                    "raw_title": "Full title as shown on site",
                    "url": "https://cinefreak.net/some-movie/",
                }
        """
        logger.info(f"Scraping CineFreak homepage: {cls.HOMEPAGE_URL}")
        try:
            entries = _run(_scrape_homepage_async(cls.HOMEPAGE_URL))
            logger.info(f"Scraped {len(entries)} entries from CineFreak homepage")
            return entries
        except Exception as e:
            logger.error(f"Failed to scrape CineFreak homepage: {e}", exc_info=True)
            return []
