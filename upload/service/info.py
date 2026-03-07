import json
import re
import logging
from upload.utils.web_scrape import WebScrapeService
from llm.services import LLMService
from llm.schema import SYSTEM_PROMPT, movie_schema

logger = logging.getLogger(__name__)


def get_structured_output(llm_response: str, schema: dict) -> dict:
    """
    Extracts and validates JSON from LLM response string.
    """
    json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
    if json_match:
        return json.loads(json_match.group())
    raise ValueError("No JSON found in response")


def get_movie_info(url):
    """
    Full pipeline with logging.
    """
    # Step 1: Scrape page content
    logger.info(f"Starting movie info extraction for: {url}")
    html_content = WebScrapeService.get_page_content(url)
    if not html_content:
        logger.error(f"Failed to scrape content from {url}")
        raise Exception("Failed to scrape page content from the given URL.")

    # Step 2: Send to LLM
    logger.debug("Sending HTML content to LLM for extraction...")
    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=SYSTEM_PROMPT
    )

    # Step 3: Extract structured output
    logger.debug("Parsing structured JSON from LLM response...")
    movie_data = get_structured_output(llm_response, movie_schema)
    logger.info(f"Extracted info for: {movie_data.get('title', 'Unknown Title')}")

    # Step 4: Resolve download links
    download_links = movie_data.get("download_links", {})
    if download_links:
        logger.info("Resolving download links via R2 extraction...")
        if download_links.get("480p"):
            logger.debug(f"Resolving 480p: {download_links['480p']}")
            movie_data["download_links"]["480p"] = WebScrapeService.get_url(download_links["480p"])
        if download_links.get("720p"):
            logger.debug(f"Resolving 720p: {download_links['720p']}")
            movie_data["download_links"]["720p"] = WebScrapeService.get_url(download_links["720p"])
        if download_links.get("1080p"):
            logger.debug(f"Resolving 1080p: {download_links['1080p']}")
            movie_data["download_links"]["1080p"] = WebScrapeService.get_url(download_links["1080p"])

    logger.info("Movie info extraction complete.")
    return movie_data