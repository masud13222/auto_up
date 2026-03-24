"""
Scraper module for CineFreak homepage.

Reuses the shared pydoll browser singleton from web_scrape.py.
No separate Chrome instance — same browser, new tab, CF session shared.

HTML structure (confirmed from live page):
  <div class="card-grid">
    <a href="URL" class="movie-card" aria-label="...">
      <div class="movie-card-content">
        <h3 class="movie-card-title">Title text here</h3>
      </div>
    </a>
  </div>
"""

import logging

from selectolax.lexbor import LexborHTMLParser

logger = logging.getLogger(__name__)


class CineFreakScraper:
    """Scrapes CineFreak homepage to discover new media entries."""

    HOMEPAGE_URL = "https://cinefreak.net/"

    @classmethod
    def scrape_homepage(cls) -> list[dict]:
        """
        Scrape the CineFreak homepage and return a list of entries.

        Reuses the shared browser singleton (web_scrape._fetch_html).
        CF session is already solved from previous requests — no re-challenge.

        Returns:
            List of dicts:
                {
                    "raw_title": "Full title as shown on site",
                    "url": "https://cinefreak.net/some-movie/",
                }
        """
        # Import here to reuse the singleton browser — no separate Chrome launch
        from upload.utils.web_scrape import _fetch_html

        logger.info(f"Scraping CineFreak homepage: {cls.HOMEPAGE_URL}")
        try:
            html = _fetch_html(cls.HOMEPAGE_URL, settle=3.0)
        except Exception as e:
            logger.error(f"Failed to fetch CineFreak homepage: {e}", exc_info=True)
            return []

        parser = LexborHTMLParser(html)
        entries = []

        for card in parser.css("div.card-grid a.movie-card"):
            href = card.attrs.get("href", "").strip()
            if not href:
                continue

            title_node = card.css_first("h3.movie-card-title")
            if title_node:
                raw_title = title_node.text(strip=True)
            else:
                # Fallback: aria-label also contains full title
                raw_title = card.attrs.get("aria-label", "").strip()

            if not raw_title:
                continue

            entries.append({
                "raw_title": raw_title,
                "url": href,
            })

        logger.info(f"Scraped {len(entries)} entries from CineFreak homepage")
        return entries
