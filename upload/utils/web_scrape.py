import httpx
import re
import io
import ssl
import time
import logging
from django.conf import settings
from selectolax.lexbor import LexborHTMLParser
from markitdown import MarkItDown

logger = logging.getLogger(__name__)

# Reuse a single MarkItDown instance
_markitdown = MarkItDown()


class WebScrapeService:
    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    R2_HEADERS = {
        **DEFAULT_HEADERS,
        "Referer": "https://cinefreak.net/",
    }

    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAYS = [3, 8, 15]  # seconds between retries (escalating)
    # Per-request hard timeout (prevents proxy hangs from blocking worker for hours)
    REQUEST_TIMEOUT = httpx.Timeout(connect=15.0, read=20.0, write=15.0, pool=10.0)

    @staticmethod
    def _request_with_retry(client, url, method="GET"):
        """
        Makes an HTTP request with automatic retry on failure.
        Retries on: connection errors, timeouts, proxy issues, SSL errors, 5xx responses.
        Uses escalating delays: 3s → 8s → 15s
        Total max time: ~46s per request (20s read + retries)
        """
        last_error = None

        for attempt in range(WebScrapeService.MAX_RETRIES + 1):
            try:
                r = client.request(method, url, timeout=WebScrapeService.REQUEST_TIMEOUT)

                # Retry on server errors (5xx)
                if r.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Server error {r.status_code}",
                        request=r.request,
                        response=r,
                    )

                return r

            except (httpx.ConnectError, httpx.TimeoutException,
                    httpx.RemoteProtocolError, httpx.ReadError, httpx.CloseError,
                    httpx.ProxyError, httpx.HTTPStatusError,
                    ssl.SSLError, OSError) as e:
                last_error = e

                if attempt < WebScrapeService.MAX_RETRIES:
                    delay = WebScrapeService.RETRY_DELAYS[attempt]
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{WebScrapeService.MAX_RETRIES + 1}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Request failed after {WebScrapeService.MAX_RETRIES + 1} attempts: {e}"
                    )

        raise last_error

    @staticmethod
    def clean_html(html: str) -> str:
        """
        Converts HTML to LLM-friendly Markdown using MarkItDown.
        Preserves links, images, and content structure while reducing tokens.
        """
        # Convert HTML string to bytes for MarkItDown
        html_bytes = html.encode("utf-8")
        buf = io.BytesIO(html_bytes)

        result = _markitdown.convert_stream(buf, file_extension=".html")
        markdown = result.text_content

        # Collapse excessive blank lines (3+ → 2)
        markdown = re.sub(r'\n{3,}', '\n\n', markdown)

        return markdown.strip()

    @staticmethod
    def get_page_content(url, selector="div.single-service-content"):
        """
        Fetches page content, extracts the target container,
        and converts to Markdown (via MarkItDown) for reduced token usage.
        Retries automatically on connection/proxy failures.
        """
        proxy = getattr(settings, 'SCRAPE_PROXY', None) or None
        try:
            logger.debug(f"Fetching page content from: {url} (Proxy: {bool(proxy)})")
            with httpx.Client(
                headers=WebScrapeService.DEFAULT_HEADERS, 
                proxy=proxy,
                timeout=WebScrapeService.REQUEST_TIMEOUT,
                follow_redirects=True
            ) as client:
                r = WebScrapeService._request_with_retry(client, url)
                r.raise_for_status()
                
                parser = LexborHTMLParser(r.text)
                node = parser.css_first(selector)
                
                if node:
                    raw_html = node.html
                    cleaned = WebScrapeService.clean_html(raw_html)
                    logger.debug(
                        f"Extracted & cleaned content. "
                        f"Raw HTML: {len(raw_html)} chars → Markdown: {len(cleaned)} chars "
                        f"({100 - (len(cleaned) / len(raw_html) * 100):.0f}% reduction)"
                    )
                    return cleaned
                logger.warning(f"Selector '{selector}' not found in {url}")
                return None
        except Exception as e:
            logger.error(f"Error fetching page content from {url}: {e}", exc_info=True)
            return None

    @staticmethod
    def cinefreak_title(url):
        """
        Fetches the page title from div.single-service-content h1.
        Quick fetch — no markdown conversion, just the raw h1 text.
        """
        proxy = getattr(settings, 'SCRAPE_PROXY', None) or None
        try:
            with httpx.Client(
                headers=WebScrapeService.DEFAULT_HEADERS,
                proxy=proxy,
                timeout=WebScrapeService.REQUEST_TIMEOUT,
                follow_redirects=True
            ) as client:
                r = WebScrapeService._request_with_retry(client, url)
                r.raise_for_status()

                parser = LexborHTMLParser(r.text)
                node = parser.css_first("div.single-service-content h1")
                if node:
                    title = node.text(strip=True)
                    logger.debug(f"Cinefreak title: {title}")
                    return title
                logger.warning(f"h1 not found in {url}")
                return None
        except Exception as e:
            logger.error(f"Error fetching cinefreak title from {url}: {e}")
            return None

    @staticmethod
    def get_url(url):
        """
        Follows window.location.href redirects and extracts Cloudflare R2 links.
        Uses specific headers including Referer for cinefreak.net.
        Retries automatically on connection/proxy failures.
        """
        headers = WebScrapeService.R2_HEADERS
        proxy = getattr(settings, 'SCRAPE_PROXY', None) or None

        try:
            logger.debug(f"Extracting R2 links from: {url} (Proxy: {bool(proxy)})")
            with httpx.Client(
                headers=headers, 
                proxy=proxy,
                timeout=WebScrapeService.REQUEST_TIMEOUT,
                follow_redirects=True
            ) as client:
                # 1. Initial request to find window.location.href
                r = WebScrapeService._request_with_retry(client, url)
                
                # Regex for window.location.href
                pattern_loc = r'window\.location\.href\s*=\s*["\'](.+?)["\']'
                match_loc = re.search(pattern_loc, r.text)
                
                if match_loc:
                    short_url = match_loc.group(1)
                    logger.debug(f"Found redirect URL: {short_url}")
                    
                    # 2. Request the redirected short URL
                    r = WebScrapeService._request_with_retry(client, short_url)
                    
                    # 3. Regex for Cloudflare R2 storage links
                    pattern_r2 = r'href=["\'](?P<url>(?:https?:)?//[^"\']*(?:\.r2\.dev|r2\.cloudflarestorage\.com)[^"\']*)["\']'
                    matches = re.findall(pattern_r2, r.text)
                    
                    if matches:
                        logger.info(f"Successfully found {len(matches)} R2 links")
                        return matches
                    
                    # --- Fallback Logic ---
                    logger.info(f"No R2 links found. Trying fallback for: {short_url}")
                    
                    # Pattern for direct Google video downloads
                    pattern_video = r'href=["\'](?P<url>https://video-downloads\.googleusercontent\.com[^"\']*)["\']'
                    
                    # Try modifying /f/ to /w/ or /gp/
                    fallbacks = []
                    if "/f/" in short_url:
                        fallbacks.append(short_url.replace("/f/", "/w/"))
                        fallbacks.append(short_url.replace("/f/", "/gp/"))
                    
                    for fb_url in fallbacks:
                        try:
                            logger.debug(f"Checking fallback: {fb_url}")
                            r_fb = WebScrapeService._request_with_retry(client, fb_url)
                            fb_matches = re.findall(pattern_video, r_fb.text)
                            if fb_matches:
                                logger.info(f"Found {len(fb_matches)} video links in {fb_url}")
                                return fb_matches
                        except Exception as e:
                            logger.warning(f"Fallback failed for {fb_url}: {e}")
                
                logger.warning(f"No links found for URL: {url}")
                return None
        except Exception as e:
            logger.error(f"Error extracting R2 links from {url}: {e}", exc_info=True)
            return None
