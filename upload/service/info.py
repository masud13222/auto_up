import json
import logging
from upload.utils.web_scrape import WebScrapeService
from llm.services import LLMService
from llm.json_repair import repair_json
from llm.schema import get_combined_system_prompt

logger = logging.getLogger(__name__)


def get_structured_output(llm_response: str) -> dict:
    """
    Extracts and validates JSON from LLM response string.
    Uses json_repair to handle truncated/malformed responses.
    """
    return repair_json(llm_response)


def detect_and_extract(html_content: str) -> tuple:
    """
    Single LLM call: detects content type AND extracts structured data.
    Reads resolution settings from UploadSettings.
    Returns: (content_type, data)
    """
    from settings.models import UploadSettings
    settings = UploadSettings.objects.first()
    extra_below = settings.extra_res_below if settings else False
    extra_above = settings.extra_res_above if settings else False
    max_extra = settings.max_extra_resolutions if settings else 0

    system_prompt = get_combined_system_prompt(
        extra_below=extra_below,
        extra_above=extra_above,
        max_extra=max_extra,
    )
    logger.info(f"Detecting + extracting (res: below={extra_below}, above={extra_above}, max={max_extra})...")

    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=system_prompt
    )

    result = get_structured_output(llm_response)
    content_type = result.get("content_type", "movie")
    data = result.get("data", {})

    title = data.get("title", "Unknown")
    logger.info(f"Detected: {content_type} — Title: {title}")
    return content_type, data


def resolve_movie_links(movie_data: dict) -> dict:
    """
    Resolve download links for a movie (generate.php → actual R2 URLs).
    """
    download_links = movie_data.get("download_links", {})
    if download_links:
        logger.info("Resolving movie download links...")
        for quality, url in list(download_links.items()):
            if url:
                logger.debug(f"Resolving {quality}: {url}")
                movie_data["download_links"][quality] = WebScrapeService.get_url(url)
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

            for quality, url in list(resolutions.items()):
                if url:
                    logger.debug(f"Resolving S{season_num} {item_label} {quality}: {url}")
                    resolved = WebScrapeService.get_url(url)
                    item["resolutions"][quality] = resolved

            # Callback after each item is fully resolved
            if on_item_resolved:
                on_item_resolved(tvshow_data)
                logger.debug(f"Progress saved: S{season_num} {item_label} resolved")

    return tvshow_data


def get_content_info(url, on_progress=None):
    """
    Main entry point: Single LLM call detects type + extracts info, then resolves URLs.
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

    # Step 2: Single LLM call — detect type + extract data
    content_type, data = detect_and_extract(html_content)

    # Save immediately after LLM extraction (before URL resolution)
    if on_progress:
        on_progress(data)

    # Step 3: Resolve URLs based on content type
    if content_type == "tvshow":
        data = resolve_tvshow_links(data, on_item_resolved=on_progress)
        logger.info("TV show info extraction complete.")
    else:
        data = resolve_movie_links(data)
        logger.info("Movie info extraction complete.")

    return content_type, data