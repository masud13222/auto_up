import httpx
import re
import logging
from selectolax.lexbor import LexborHTMLParser

logger = logging.getLogger(__name__)

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

    @staticmethod
    def get_page_content(url, selector="div.single-service-content"):
        """
        Fetches the HTML of a specific container from the given URL.
        """
        try:
            logger.debug(f"Fetching page content from: {url}")
            with httpx.Client(headers=WebScrapeService.DEFAULT_HEADERS, timeout=30.0, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                
                parser = LexborHTMLParser(r.text)
                node = parser.css_first(selector)
                
                if node:
                    logger.debug(f"Successfully extracted content using selector: {selector}")
                    return node.html
                logger.warning(f"Selector '{selector}' not found in {url}")
                return None
        except Exception as e:
            logger.error(f"Error fetching page content from {url}: {e}", exc_info=True)
            return None

    @staticmethod
    def get_url(url):
        """
        Follows window.location.href redirects and extracts Cloudflare R2 links.
        Uses specific headers including Referer for cinefreak.net.
        """
        headers = WebScrapeService.R2_HEADERS
        
        try:
            logger.debug(f"Extracting R2 links from: {url}")
            with httpx.Client(headers=headers, timeout=30.0, follow_redirects=True) as client:
                # 1. Initial request to find window.location.href
                r = client.get(url)
                
                # Regex for window.location.href
                pattern_loc = r'window\.location\.href\s*=\s*["\'](.*?)["\']'
                match_loc = re.search(pattern_loc, r.text)
                
                if match_loc:
                    short_url = match_loc.group(1)
                    logger.debug(f"Found redirect URL: {short_url}")
                    
                    # 2. Request the redirected short URL
                    r = client.get(short_url)
                    
                    # 3. Regex for Cloudflare R2 storage links
                    pattern_r2 = r'href=["\']((?:https?:)?//[^"\']*(?:\.r2\.dev|r2\.cloudflarestorage\.com)[^"\']*)["\']'
                    matches = re.findall(pattern_r2, r.text)
                    
                    if matches:
                        logger.info(f"Successfully found {len(matches)} R2 links")
                        return matches
                    
                    # --- Fallback Logic ---
                    logger.info(f"No R2 links found. Trying fallback for: {short_url}")
                    
                    # Pattern for direct Google video downloads
                    pattern_video = r'href=["\'](https://video-downloads\.googleusercontent\.com[^"\']*)["\']'
                    
                    # Try modifying /f/ to /w/ or /gp/
                    fallbacks = []
                    if "/f/" in short_url:
                        fallbacks.append(short_url.replace("/f/", "/w/"))
                        fallbacks.append(short_url.replace("/f/", "/gp/"))
                    
                    for fb_url in fallbacks:
                        try:
                            logger.debug(f"Checking fallback: {fb_url}")
                            r_fb = client.get(fb_url)
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
