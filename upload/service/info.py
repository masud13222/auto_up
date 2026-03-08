import json
import logging
from upload.utils.web_scrape import WebScrapeService
from llm.services import LLMService
from llm.json_repair import repair_json
from llm.schema import SYSTEM_PROMPT, movie_schema
from llm.tvshow_schema import TVSHOW_SYSTEM_PROMPT, tvshow_schema
from llm.content_type_detector import CONTENT_TYPE_DETECTION_PROMPT

logger = logging.getLogger(__name__)


def get_structured_output(llm_response: str, schema: dict) -> dict:
    """
    Extracts and validates JSON from LLM response string.
    Uses json_repair to handle truncated/malformed responses.
    """
    return repair_json(llm_response)


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


def extract_movie_data(html_content: str) -> dict:
    """
    Extract movie data from HTML content (no URL resolution).
    Returns raw LLM-extracted data with generate.php URLs.
    """
    logger.debug("Sending HTML content to LLM for movie extraction...")
    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=SYSTEM_PROMPT
    )

    logger.debug("Parsing structured JSON from LLM response...")
    movie_data = get_structured_output(llm_response, movie_schema)
    logger.info(f"Extracted info for: {movie_data.get('title', 'Unknown Title')}")
    return movie_data


def extract_tvshow_data(html_content: str) -> dict:
    """
    Extract TV show data from HTML content (no URL resolution).
    Returns raw LLM-extracted data with generate.php URLs.
    """
    logger.debug("Sending HTML content to LLM for TV show extraction...")
    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=TVSHOW_SYSTEM_PROMPT
    )

    logger.debug("Parsing structured JSON from LLM response...")
    tvshow_data = get_structured_output(llm_response, tvshow_schema)
    logger.info(f"Extracted TV show info for: {tvshow_data.get('title', 'Unknown Title')}")
    return tvshow_data


def resolve_movie_links(movie_data: dict) -> dict:
    """
    Resolve download links for a movie (generate.php → actual R2 URLs).
    """
    download_links = movie_data.get("download_links", {})
    if download_links:
        logger.info("Resolving movie download links...")
        for quality in ["480p", "720p", "1080p"]:
            if download_links.get(quality):
                logger.debug(f"Resolving {quality}: {download_links[quality]}")
                movie_data["download_links"][quality] = WebScrapeService.get_url(download_links[quality])
    return movie_data


def resolve_tvshow_links(tvshow_data: dict, on_item_resolved=None) -> dict:
    """
    Resolve download links for a TV show (generate.php → actual R2 URLs).
    
    Args:
        tvshow_data: TV show data with generate.php URLs
        on_item_resolved: Optional callback(tvshow_data) called after each 
                          download item is fully resolved. Use this to save 
                          progress to DB incrementally.
    """
    seasons = tvshow_data.get("seasons", [])
    if not seasons:
        return tvshow_data

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

            # Callback after each item is fully resolved
            if on_item_resolved:
                on_item_resolved(tvshow_data)
                logger.debug(f"Progress saved: S{season_num} {item_label} resolved")

    return tvshow_data


def get_content_info(url, on_progress=None):
    """
    Main entry point: Detects content type and extracts info.
    Scrapes only ONCE, then reuses the HTML.
    
    Args:
        url: Page URL to scrape
        on_progress: Optional callback(data) for incremental DB saves during 
                     URL resolution. Called after each download item is resolved.
    
    Returns: (content_type, data)
    """
    # Step 1: Scrape page content (only once)
    logger.info(f"Starting content info extraction for: {url}")
    html_content = WebScrapeService.get_page_content(url)
    if not html_content:
        logger.error(f"Failed to scrape content from {url}")
        raise Exception("Failed to scrape page content from the given URL.")

    # Step 2: Detect content type
    content_type = detect_content_type(html_content)

    # Step 3: Extract info based on type
    if content_type == "tvshow":
        logger.info("Content identified as TV Show. Using TV show extraction pipeline.")
        data = extract_tvshow_data(html_content)

        # Save immediately after LLM extraction (before URL resolution)
        if on_progress:
            on_progress(data)

        # Step 4: Resolve URLs with progress callback
        data = resolve_tvshow_links(data, on_item_resolved=on_progress)
        logger.info("TV show info extraction complete.")
        return "tvshow", data
    else:
        logger.info("Content identified as Movie. Using movie extraction pipeline.")
        data = extract_movie_data(html_content)

        # Save immediately after LLM extraction
        if on_progress:
            on_progress(data)

        # Step 4: Resolve URLs
        data = resolve_movie_links(data)
        logger.info("Movie info extraction complete.")
        return "movie", data