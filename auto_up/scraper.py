"""
Scraper module for CineFreak homepage.

Scrapes the homepage listing and returns structured data about each entry,
including the raw title, cleaned name/year, and article URL.
"""

import logging
import httpx
from selectolax.lexbor import LexborHTMLParser
from django.conf import settings

logger = logging.getLogger(__name__)


class CineFreakScraper:
    """Scrapes CineFreak homepage to discover new media entries."""

    HOMEPAGE_URL = "https://cinefreak.net/"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/134.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    # Retry config — escalating delays
    MAX_RETRIES = 3
    RETRY_DELAYS = [3, 8, 15]

    # CSS selectors
    ARTICLE_SELECTOR = "section.site-main div.container article"
    TITLE_SELECTOR = "h3.entry-title a"

    @classmethod
    def scrape_homepage(cls) -> list[dict]:
        """
        Scrape the CineFreak homepage and return a list of entries.

        Returns:
            List of dicts:
                {
                    "raw_title": "Full title as shown on site",
                    "url": "https://cinefreak.net/some-movie/",
                }
        """
        proxy = getattr(settings, "SCRAPE_PROXY", None) or None

        try:
            logger.info(f"Scraping CineFreak homepage: {cls.HOMEPAGE_URL}")

            with httpx.Client(
                headers=cls.DEFAULT_HEADERS,
                proxy=proxy,
                timeout=httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=10.0),
                follow_redirects=True,
            ) as client:
                response = cls._request_with_retry(client, cls.HOMEPAGE_URL)
                response.raise_for_status()

                parser = LexborHTMLParser(response.text)
                articles = parser.css(cls.ARTICLE_SELECTOR)

                if not articles:
                    logger.warning("No articles found on CineFreak homepage")
                    return []

                entries = []
                for article in articles:
                    link = article.css_first(cls.TITLE_SELECTOR)
                    if not link:
                        continue

                    raw_title = link.text(strip=True)
                    url = link.attrs.get("href", "")

                    if not raw_title or not url:
                        continue

                    entries.append({
                        "raw_title": raw_title,
                        "url": url,
                    })

                logger.info(f"Scraped {len(entries)} entries from CineFreak homepage")
                return entries

        except Exception as e:
            logger.error(f"Failed to scrape CineFreak homepage: {e}", exc_info=True)
            return []

    @classmethod
    def _request_with_retry(cls, client, url):
        """HTTP request with automatic retries on transient failures."""
        import time

        last_error = None
        for attempt in range(cls.MAX_RETRIES + 1):
            try:
                r = client.get(url)
                if r.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Server error {r.status_code}",
                        request=r.request,
                        response=r,
                    )
                return r
            except (
                httpx.ConnectError,
                httpx.TimeoutException,
                httpx.RemoteProtocolError,
                httpx.ReadError,
                httpx.CloseError,
                httpx.ProxyError,
                httpx.HTTPStatusError,
            ) as e:
                last_error = e
                if attempt < cls.MAX_RETRIES:
                    delay = cls.RETRY_DELAYS[attempt]
                    logger.warning(
                        f"Scrape request failed (attempt {attempt + 1}/{cls.MAX_RETRIES + 1}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Scrape request failed after {cls.MAX_RETRIES + 1} attempts: {e}"
                    )
        raise last_error
