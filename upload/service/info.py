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


def detect_and_extract(html_content: str, db_match_candidates: list = None, flixbd_results: list = None) -> tuple:
    """
    Single LLM call: detects content type AND extracts structured data.
    If db_match_candidates or flixbd_results provided, also performs duplicate check in same call.
    Reads resolution settings from UploadSettings.
    Returns: (content_type, data, duplicate_check_or_None)
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
        db_match_candidates=db_match_candidates,
        flixbd_results=flixbd_results,
    )
    has_dup_ctx = bool(db_match_candidates or flixbd_results)
    dup_tag = " + duplicate check" if has_dup_ctx else ""
    logger.info(f"Detecting + extracting{dup_tag} (res: below={extra_below}, above={extra_above}, max={max_extra})...")

    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=system_prompt,
        purpose='extract+dup_check' if has_dup_ctx else 'extract',
    )

    result = get_structured_output(llm_response)
    content_type = result.get("content_type", "movie")
    data = result.get("data", {})
    dup_result = result.get("duplicate_check", None)

    title = data.get("title", "Unknown")
    logger.info(f"Detected: {content_type} — Title: {title}")
    if dup_result:
        logger.info(f"Duplicate check: action={dup_result.get('action')}, reason={dup_result.get('reason', '')[:80]}")
    return content_type, data, dup_result


def resolve_movie_links(movie_data: dict, existing_result: dict = None) -> dict:
    """
    Resolve download links for a movie (generate.php → actual R2 URLs).
    Skips qualities that already have Drive links in existing_result.
    """
    from upload.tasks.helpers import is_drive_link

    # Build lookup of existing drive links
    existing_links = {}
    if existing_result:
        for q, link in existing_result.get("download_links", {}).items():
            if is_drive_link(link):
                existing_links[q] = link

    download_links = movie_data.get("download_links", {})
    if download_links:
        skipped = 0
        resolved = 0
        logger.info("Resolving movie download links...")
        pending: list[tuple[str, str]] = []
        for quality, url in list(download_links.items()):
            if quality in existing_links:
                movie_data["download_links"][quality] = existing_links[quality]
                skipped += 1
                logger.debug(f"Skipping {quality}: already has Drive link")
                continue
            if url:
                pending.append((quality, url))
                logger.debug(f"Queued {quality}: {url}")
        if pending:
            urls = [u for _, u in pending]
            batch = WebScrapeService.get_urls_parallel(urls)
            for (quality, url), res in zip(pending, batch):
                if isinstance(res, Exception):
                    logger.error(f"Resolving movie {quality} ({url}): {res}", exc_info=res)
                    movie_data["download_links"][quality] = None
                else:
                    movie_data["download_links"][quality] = res
                resolved += 1
        if skipped:
            logger.info(f"Link resolution: {resolved} resolved, {skipped} skipped (already uploaded)")
    return movie_data


def resolve_tvshow_links(tvshow_data: dict, on_item_resolved=None, existing_result: dict = None) -> dict:
    """
    Resolve download links for a TV show (generate.php → actual R2 URLs).
    Skips items/qualities that already have Drive links in existing_result.
    
    Args:
        tvshow_data: TV show data with generate.php URLs
        on_item_resolved: Optional callback(tvshow_data) called after each 
                          download item is fully resolved. Use this to save 
                          progress to DB incrementally.
        existing_result: Optional dict with previous task result containing
                         Drive links to skip resolving.
    """
    from upload.tasks.helpers import is_drive_link

    seasons = tvshow_data.get("seasons", [])
    if not seasons:
        return tvshow_data

    # Build lookup of existing drive links: {(season_num, label, quality): drive_link}
    existing_links = {}
    if existing_result:
        for season in existing_result.get("seasons", []):
            snum = season.get("season_number")
            for item in season.get("download_items", []):
                label = item.get("label", "")
                item_type = item.get("type", "")
                for q, link in item.get("resolutions", {}).items():
                    if is_drive_link(link):
                        existing_links[(snum, label, q)] = link
                        # Also index by type for flexible matching
                        existing_links[(snum, item_type, q)] = link

    total_skipped = 0
    total_resolved = 0

    logger.info(f"Resolving download links for {len(seasons)} season(s)..."
                + (f" ({len(existing_links)} existing Drive links to skip)" if existing_links else ""))

    for season in seasons:
        season_num = season.get("season_number", "?")
        download_items = season.get("download_items", [])

        for item in download_items:
            item_label = item.get("label", "Unknown")
            item_type = item.get("type", "")
            resolutions = item.get("resolutions", {})

            # Check if ALL resolutions for this item already have drive links
            all_uploaded = all(
                (season_num, item_label, q) in existing_links
                or (season_num, item_type, q) in existing_links
                for q in resolutions
            ) if resolutions and existing_links else False

            if all_uploaded:
                # Restore all drive links from existing result
                for q in list(resolutions.keys()):
                    link = existing_links.get((season_num, item_label, q)) or existing_links.get((season_num, item_type, q))
                    if link:
                        item["resolutions"][q] = link
                        total_skipped += 1
                logger.debug(f"Skipping S{season_num} {item_label}: all resolutions already uploaded")
                if on_item_resolved:
                    on_item_resolved(tvshow_data)
                continue

            pending: list[tuple[str, str]] = []
            for quality, url in list(resolutions.items()):
                existing_link = existing_links.get((season_num, item_label, quality)) or existing_links.get((season_num, item_type, quality))
                if existing_link:
                    item["resolutions"][quality] = existing_link
                    total_skipped += 1
                    logger.debug(f"Skipping S{season_num} {item_label} {quality}: already has Drive link")
                    continue
                if url:
                    pending.append((quality, url))
                    logger.debug(f"Queued S{season_num} {item_label} {quality}: {url}")
            if pending:
                batch = WebScrapeService.get_urls_parallel([u for _, u in pending])
                for (quality, url), res in zip(pending, batch):
                    if isinstance(res, Exception):
                        logger.error(
                            f"Resolving S{season_num} {item_label} {quality} ({url}): {res}",
                            exc_info=res,
                        )
                        item["resolutions"][quality] = None
                    else:
                        item["resolutions"][quality] = res
                    total_resolved += 1

            # Callback after each item is fully resolved
            if on_item_resolved:
                on_item_resolved(tvshow_data)
                logger.debug(f"Progress saved: S{season_num} {item_label} resolved")

    logger.info(f"Link resolution complete: {total_resolved} resolved, {total_skipped} skipped (already uploaded)")
    return tvshow_data


def get_content_info(url, on_progress=None, db_match_candidates=None, flixbd_results=None, existing_result=None):
    """
    Main entry point: Single LLM call detects type + extracts info + optional duplicate check,
    then resolves URLs. Scrapes only ONCE, then reuses the HTML.

    Args:
        url: Page URL to scrape
        on_progress: Optional callback(data) for incremental DB saves during
                     URL resolution. Called after each download item is resolved.
        db_match_candidates: Optional list of candidate dicts (with id/pk) for duplicate check.
        flixbd_results: Optional list of FlixBD search results (typically max 3) for duplicate check.
        existing_result: Optional dict with previous task result containing
                         Drive links — used to skip resolving already-uploaded items.

    Returns: (content_type, data, dup_result_or_None)
    """
    # Step 1: Scrape page content (only once)
    logger.info(f"Starting content info extraction for: {url}")
    html_content = WebScrapeService.get_page_content(url)
    if not html_content:
        logger.error(f"Failed to scrape content from {url}")
        raise Exception("Failed to scrape page content from the given URL.")

    # Step 2: Single LLM call — detect type + extract data + optional duplicate check
    content_type, data, dup_result = detect_and_extract(
        html_content,
        db_match_candidates=db_match_candidates,
        flixbd_results=flixbd_results,
    )

    # Save immediately after LLM extraction (before URL resolution)
    if on_progress:
        on_progress(data)

    # If duplicate check says skip, return early (no need to resolve URLs)
    if dup_result and dup_result.get("action") == "skip":
        logger.info(f"Duplicate skip detected during extraction. Skipping URL resolution.")
        return content_type, data, dup_result

    # Step 3: Resolve URLs based on content type
    if content_type == "tvshow":
        data = resolve_tvshow_links(data, on_item_resolved=on_progress, existing_result=existing_result)
        logger.info("TV show info extraction complete.")
    else:
        data = resolve_movie_links(data, existing_result=existing_result)
        logger.info("Movie info extraction complete.")

    return content_type, data, dup_result
