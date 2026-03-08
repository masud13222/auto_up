import json
import re
import logging
from upload.utils.web_scrape import WebScrapeService
from llm.services import LLMService
from llm.schema import SYSTEM_PROMPT, movie_schema
from llm.tvshow_schema import TVSHOW_SYSTEM_PROMPT, tvshow_schema
from llm.content_type_detector import CONTENT_TYPE_DETECTION_PROMPT

logger = logging.getLogger(__name__)


def get_structured_output(llm_response: str, schema: dict) -> dict:
    """
    Extracts and validates JSON from LLM response string.
    Supports both JSON objects and JSON arrays.
    """
    json_obj_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
    json_array_match = re.search(r'\[.*\]', llm_response, re.DOTALL)

    if json_obj_match:
        return json.loads(json_obj_match.group())
    if json_array_match:
        return json.loads(json_array_match.group())
    raise ValueError("No JSON found in response")


def detect_content_type(html_content: str) -> str:
    """
    Uses LLM to detect whether the HTML content is about a movie or TV show.
    Returns: 'movie' or 'tvshow'
    """
    logger.info("Detecting content type (movie vs tvshow)...")
    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=CONTENT_TYPE_DETECTION_PROMPT
    )

    try:
        result = get_structured_output(llm_response, {})
        content_type = result.get("content_type", "movie")
        confidence = result.get("confidence", 0)
        reason = result.get("reason", "N/A")
        logger.info(f"Content type detected: {content_type} (confidence: {confidence}, reason: {reason})")
        return content_type
    except Exception as e:
        logger.warning(f"Content type detection failed, defaulting to 'movie': {e}")
        return "movie"


def _extract_movie_from_html(html_content: str) -> dict:
    """
    Internal: Extract movie data from already-scraped HTML content.
    """
    logger.debug("Sending HTML content to LLM for movie extraction...")
    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=SYSTEM_PROMPT
    )

    logger.debug("Parsing structured JSON from LLM response...")
    movie_data = get_structured_output(llm_response, movie_schema)
    logger.info(f"Extracted info for: {movie_data.get('title', 'Unknown Title')}")

    # Resolve download links
    download_links = movie_data.get("download_links", {})
    if download_links:
        logger.info("Resolving download links via R2 extraction...")
        for quality in ["480p", "720p", "1080p"]:
            if download_links.get(quality):
                logger.debug(f"Resolving {quality}: {download_links[quality]}")
                movie_data["download_links"][quality] = WebScrapeService.get_url(download_links[quality])

    return movie_data


def _extract_tvshow_from_html(html_content: str) -> dict:
    """
    Internal: Extract TV show data from already-scraped HTML content.
    """
    logger.debug("Sending HTML content to LLM for TV show extraction...")
    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=TVSHOW_SYSTEM_PROMPT
    )

    logger.debug("Parsing structured JSON from LLM response...")
    tvshow_data = get_structured_output(llm_response, tvshow_schema)
    logger.info(f"Extracted TV show info for: {tvshow_data.get('title', 'Unknown Title')}")

    # Resolve download links for each season > download_item > resolution
    seasons = tvshow_data.get("seasons", [])
    if seasons:
        logger.info(f"Resolving download links for {len(seasons)} season(s)...")
        for season in seasons:
            season_num = season.get("season_number", "?")
            download_items = season.get("download_items", [])

            for item in download_items:
                item_label = item.get("label", "Unknown")
                resolutions = item.get("resolutions", {})

                for quality in ["480p", "720p", "1080p"]:
                    if resolutions.get(quality):
                        logger.debug(f"Resolving S{season_num} {item_label} {quality}: {resolutions[quality]}")
                        resolved = WebScrapeService.get_url(resolutions[quality])
                        item["resolutions"][quality] = resolved

    return tvshow_data


def get_movie_info(url):
    """
    Full pipeline for MOVIE info extraction.
    Scrapes the page and extracts movie data.
    """
    logger.info(f"Starting movie info extraction for: {url}")
    html_content = WebScrapeService.get_page_content(url)
    if not html_content:
        logger.error(f"Failed to scrape content from {url}")
        raise Exception("Failed to scrape page content from the given URL.")

    movie_data = _extract_movie_from_html(html_content)
    logger.info("Movie info extraction complete.")
    return movie_data


def get_tvshow_info(url):
    """
    Full pipeline for TV SHOW info extraction.
    Scrapes the page and extracts TV show data with season-wise links.
    """
    logger.info(f"Starting TV show info extraction for: {url}")
    html_content = WebScrapeService.get_page_content(url)
    if not html_content:
        logger.error(f"Failed to scrape content from {url}")
        raise Exception("Failed to scrape page content from the given URL.")

    tvshow_data = _extract_tvshow_from_html(html_content)
    logger.info("TV show info extraction complete.")
    return tvshow_data


def get_content_info(url):
    """
    Main entry point: Detects content type (movie vs tvshow) and extracts info accordingly.
    Scrapes only ONCE, then reuses the HTML for both detection and extraction.
    Returns a tuple: (content_type, data)
    """
    # Step 1: Scrape page content (only once)
    logger.info(f"Starting content info extraction for: {url}")
    html_content = WebScrapeService.get_page_content(url)
    if not html_content:
        logger.error(f"Failed to scrape content from {url}")
        raise Exception("Failed to scrape page content from the given URL.")

    # Step 2: Detect content type
    content_type = detect_content_type(html_content)

    # Step 3: Extract info based on type (reuse same HTML, no re-scrape)
    if content_type == "tvshow":
        logger.info("Content identified as TV Show. Using TV show extraction pipeline.")
        data = _extract_tvshow_from_html(html_content)
        logger.info("TV show info extraction complete.")
        return "tvshow", data
    else:
        logger.info("Content identified as Movie. Using movie extraction pipeline.")
        data = _extract_movie_from_html(html_content)
        logger.info("Movie info extraction complete.")
        return "movie", data