import json
import logging
from datetime import timedelta

from django.utils import timezone

from upload.utils.web_scrape import WebScrapeService
from upload.utils.tv_items import tv_item_key
from upload.tasks.helpers import (
    coerce_download_source_value,
    coerce_entry_language_value,
    entry_language_key,
    is_drive_link,
    primary_download_source_url,
)
from llm.services import LLMService
from llm.json_repair import repair_json
from llm.schema import get_combined_system_prompt
from llm.schema.blocked_names import (
    LEGACY_SITE_ROW_ID_JSON_KEY,
    TARGET_SITE_ROW_ID_JSON_KEY,
)
from llm.update_pass import compute_update_delta
from upload.utils.resolution_policy import apply_upload_resolution_policy
from upload.utils.force_is_adult_source_domain import apply_force_is_adult_from_source_urls

logger = logging.getLogger(__name__)

# Pretty JSON for DB/admin only (readability). LLM prompt is built separately with compact JSON
# in llm/schema/combined_schema.py — this does not add tokens to the model call.
_JSON_FOR_DB = {"indent": 2, "ensure_ascii": False}
_LLM_JSON_RETRY_MAX = 2
# Appended only on JSON-retry calls: same user body as the first request, plus this line.
_JSON_RETRY_USER_SUFFIX = (
    "\n\nReturn a single valid JSON object only (no markdown fences, no text outside JSON). "
    "Previous attempt produced invalid JSON."
)


def _entry_language_key(entry: dict) -> str:
    return entry_language_key((entry or {}).get("l"))


def _entry_filename_key(entry: dict) -> str:
    return str((entry or {}).get("f") or "").strip().lower()


def _entry_copy(entry: dict, *, link: str) -> dict:
    out = {
        "u": coerce_download_source_value(link),
        "l": coerce_entry_language_value(entry.get("l")),
        "f": str(entry.get("f") or "").strip(),
    }
    if isinstance(entry.get("s"), str) and entry["s"].strip():
        out["s"] = entry["s"].strip()
    return out


def _save_duplicate_usage_snapshot_to_latest_usage(
    *,
    dup_result: dict | None,
    db_match_candidates: list | None,
    flixbd_results: list | None,
    purpose: str,
    response_text: str = "",
    extra_context: dict | None = None,
) -> None:
    """Persist duplicate-check output and prompt context on the matching LLMUsage row."""
    has_dup = bool(dup_result)
    has_ctx = bool(db_match_candidates or flixbd_results or extra_context)
    if not purpose or (not has_dup and not has_ctx):
        return
    try:
        from llm.models import LLMUsage

        cutoff = timezone.now() - timedelta(seconds=120)
        query = LLMUsage.objects.filter(purpose=purpose, created_at__gte=cutoff)
        body = (response_text or "").strip()
        if body:
            query = query.filter(response_text=body)
        row = query.order_by("-pk").first()
        if not row:
            return
        update_fields = []
        if has_dup:
            row.duplicate_check_json = json.dumps(dup_result, **_JSON_FOR_DB)
            update_fields.append("duplicate_check_json")
        if has_ctx:
            ctx: dict = {}
            if db_match_candidates or flixbd_results:
                ctx["db_match_candidates"] = db_match_candidates or []
                ctx["flixbd_results"] = flixbd_results or []
            if extra_context:
                ctx.update(extra_context)
            if ctx:
                row.duplicate_context_json = json.dumps(ctx, **_JSON_FOR_DB)
                update_fields.append("duplicate_context_json")
        if update_fields:
            row.save(update_fields=update_fields)
    except Exception as e:
        logger.warning("Could not save duplicate snapshot to LLMUsage: %s", e)


def _find_matched_candidate(db_match_candidates: list, matched_id) -> dict | None:
    """Find the DB candidate dict whose 'id' matches matched_task_id from dup check."""
    if not db_match_candidates or matched_id is None:
        return None
    for c in db_match_candidates:
        if isinstance(c, dict) and c.get("id") == matched_id:
            return c
    return None


def get_structured_output(llm_response: str) -> dict:
    """
    Extracts and validates JSON from LLM response string.
    Uses json_repair to handle truncated/malformed responses.
    """
    return repair_json(llm_response)


def _repair_with_llm_retry(
    *,
    llm_response: str,
    original_user_prompt: str,
    system_prompt: str,
    purpose: str,
) -> tuple[dict, str]:
    """
    Parse structured output, falling back to at most 2 LLM JSON-fix retries.

    Each retry re-sends the same user body as the first call (stateless APIs),
    with a short suffix asking for valid JSON only.
    """
    current_response = llm_response
    last_error = None

    for attempt in range(_LLM_JSON_RETRY_MAX + 1):
        try:
            return get_structured_output(current_response), current_response
        except Exception as e:
            last_error = e
            if attempt >= _LLM_JSON_RETRY_MAX:
                break
            logger.warning(
                "Structured JSON parse failed for purpose=%s (attempt %s/%s): %s. Requesting LLM JSON repair.",
                purpose or "n/a",
                attempt + 1,
                _LLM_JSON_RETRY_MAX + 1,
                e,
            )
            repair_prompt = (original_user_prompt or "") + _JSON_RETRY_USER_SUFFIX
            current_response = LLMService.generate_completion(
                prompt=repair_prompt,
                system_prompt=system_prompt,
                purpose=purpose,
            )

    raise last_error or ValueError("Could not parse structured JSON response")


def detect_and_extract(
    html_content: str,
    db_match_candidates: list = None,
    flixbd_results: list = None,
) -> tuple:
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

    purpose = 'extract+dup_check' if has_dup_ctx else 'extract'
    result, llm_response = _repair_with_llm_retry(
        llm_response=llm_response,
        original_user_prompt=html_content,
        system_prompt=system_prompt,
        purpose=purpose,
    )
    content_type = result.get("content_type", "movie")
    data = result.get("data", {})
    dup_result = result.get("duplicate_check", None)
    if dup_result is not None and not isinstance(dup_result, dict):
        dup_result = None
    if isinstance(dup_result, dict):
        if (
            TARGET_SITE_ROW_ID_JSON_KEY not in dup_result
            and LEGACY_SITE_ROW_ID_JSON_KEY in dup_result
        ):
            dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = dup_result.get(
                LEGACY_SITE_ROW_ID_JSON_KEY
            )
        if TARGET_SITE_ROW_ID_JSON_KEY not in dup_result:
            dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = None
        if "missing_resolutions" not in dup_result or not isinstance(
            dup_result.get("missing_resolutions"), list
        ):
            dup_result["missing_resolutions"] = []
        if "has_new_episodes" not in dup_result:
            dup_result["has_new_episodes"] = False

    # Same tiers as system prompt; strip disallowed qualities before DB snapshot + pipeline.
    if not isinstance(data, dict):
        data = {}
    data, dup_result = apply_upload_resolution_policy(
        content_type,
        data,
        dup_result,
        extra_below=extra_below,
        extra_above=extra_above,
        max_extra=max_extra,
    )

    if has_dup_ctx:
        _save_duplicate_usage_snapshot_to_latest_usage(
            dup_result=dup_result,
            db_match_candidates=db_match_candidates,
            flixbd_results=flixbd_results,
            purpose=purpose,
            response_text=llm_response,
        )

    title = data.get("title", "Unknown")
    logger.info(f"Detected: {content_type} — Title: {title}")
    if dup_result:
        logger.info(f"Duplicate check: action={dup_result.get('action')}, reason={dup_result.get('reason', '')[:80]}")

    # ── Pass 2: Delta filtering for "update" actions ──
    if (
        isinstance(dup_result, dict)
        and dup_result.get("action") == "update"
        and db_match_candidates
    ):
        matched_id = dup_result.get("matched_task_id")
        db_candidate = _find_matched_candidate(db_match_candidates, matched_id)
        if db_candidate is not None:
            logger.info(
                "Pass-2: running delta filter (content_type=%s, matched_id=%s)",
                content_type,
                matched_id,
            )
            delta = compute_update_delta(
                content_type,
                data,
                db_candidate,
                update_details=dup_result.get("update_details"),
                dup_search_context={
                    "db_match_candidates": db_match_candidates,
                    "flixbd_results": flixbd_results,
                },
            )
            if delta is not None:
                is_empty_delta = (
                    (content_type == "movie" and not delta.get("download_links"))
                    or (content_type == "tvshow" and not delta.get("seasons"))
                )
                if is_empty_delta:
                    logger.info(
                        "Pass-2 delta is empty — nothing to update. Changing action to 'skip'."
                    )
                    dup_result["action"] = "skip"
                elif content_type == "movie" and "download_links" in delta:
                    data["download_links"] = delta["download_links"]
                    logger.info("Pass-2 applied: movie download_links replaced with delta (%d res).", len(delta["download_links"]))
                elif content_type == "tvshow" and "seasons" in delta:
                    data["seasons"] = delta["seasons"]
                    logger.info("Pass-2 applied: tvshow seasons replaced with delta (%d seasons).", len(delta["seasons"]))
                else:
                    logger.warning("Pass-2 returned delta but no usable key for content_type=%s. Using full data.", content_type)
            else:
                logger.warning("Pass-2 delta filter returned None. Falling back to full Pass-1 data.")
        else:
            logger.warning(
                "Pass-2 skipped: matched_task_id=%s not found in db_match_candidates.",
                matched_id,
            )

    return content_type, data, dup_result


def resolve_movie_links(movie_data: dict, existing_result: dict = None) -> dict:
    """
    Resolve download links for a movie (generate.php → actual R2 URLs).
    Skips qualities that already have Drive links in existing_result.
    """
    # Build lookup of existing drive links
    existing_links = {}
    if existing_result:
        for resolution, entries in existing_result.get("download_links", {}).items():
            for entry in entries if isinstance(entries, list) else []:
                drive_link = primary_download_source_url(entry.get("u"))
                if is_drive_link(entry.get("u")):
                    existing_links[(resolution, _entry_language_key(entry), _entry_filename_key(entry))] = drive_link

    download_links = movie_data.get("download_links", {})
    if download_links:
        skipped = 0
        resolved = 0
        logger.info("Resolving movie download links...")
        pending: list[tuple[str, int, str]] = []
        for resolution, entries in list(download_links.items()):
            if not isinstance(entries, list):
                continue
            updated_entries = []
            for idx, entry in enumerate(entries):
                if not isinstance(entry, dict):
                    continue
                url = primary_download_source_url(entry.get("u"))
                entry_key = (resolution, _entry_language_key(entry), _entry_filename_key(entry))
                if entry_key in existing_links:
                    updated_entries.append(_entry_copy(entry, link=existing_links[entry_key]))
                    skipped += 1
                    logger.debug("Skipping %s [%s]: already has Drive link", resolution, entry.get("l"))
                    continue
                updated_entries.append(dict(entry))
                if url:
                    pending.append((resolution, idx, url))
                    logger.debug("Queued %s [%s]: %s", resolution, entry.get("l"), url)
            movie_data["download_links"][resolution] = updated_entries
        if pending:
            urls = [u for _, _, u in pending]
            batch = WebScrapeService.get_urls_parallel(urls)
            for (resolution, idx, url), res in zip(pending, batch):
                current = movie_data["download_links"][resolution][idx]
                if isinstance(res, Exception):
                    logger.error(f"Resolving movie {resolution} ({url}): {res}", exc_info=res)
                    movie_data["download_links"][resolution][idx] = _entry_copy(current, link="")
                else:
                    movie_data["download_links"][resolution][idx] = _entry_copy(current, link=res)
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
    seasons = tvshow_data.get("seasons", [])
    if not seasons:
        return tvshow_data

    # Build lookup of existing drive links by exact TV item key.
    existing_links = {}
    if existing_result:
        for season in existing_result.get("seasons", []):
            snum = season.get("season_number")
            for item in season.get("download_items", []):
                key = tv_item_key(item)
                for resolution, entries in item.get("resolutions", {}).items():
                    for entry in entries if isinstance(entries, list) else []:
                        drive_link = primary_download_source_url(entry.get("u"))
                        if is_drive_link(entry.get("u")):
                            existing_links[
                                (snum, key, resolution, _entry_language_key(entry), _entry_filename_key(entry))
                            ] = drive_link

    total_skipped = 0
    total_resolved = 0

    logger.info(f"Resolving download links for {len(seasons)} season(s)..."
                + (f" ({len(existing_links)} existing Drive links to skip)" if existing_links else ""))

    for season in seasons:
        season_num = season.get("season_number", "?")
        download_items = season.get("download_items", [])

        for item in download_items:
            item_label = item.get("label", "Unknown")
            item_key = tv_item_key(item)
            resolutions = item.get("resolutions", {})

            # Check if ALL resolutions for this item already have drive links
            all_uploaded = (
                all(
                    (
                        season_num,
                        item_key,
                        resolution,
                        _entry_language_key(entry),
                        _entry_filename_key(entry),
                    ) in existing_links
                    for resolution, entries in resolutions.items()
                    for entry in (entries if isinstance(entries, list) else [])
                )
                if resolutions and existing_links
                else False
            )

            if all_uploaded:
                # Restore all drive links from existing result
                for resolution, entries in list(resolutions.items()):
                    restored = []
                    for entry in entries if isinstance(entries, list) else []:
                        link = existing_links.get(
                            (season_num, item_key, resolution, _entry_language_key(entry), _entry_filename_key(entry))
                        )
                        restored.append(_entry_copy(entry, link=link or ""))
                        if link:
                            total_skipped += 1
                    item["resolutions"][resolution] = restored
                logger.debug(f"Skipping S{season_num} {item_label}: all resolutions already uploaded")
                if on_item_resolved:
                    on_item_resolved(tvshow_data)
                continue

            pending: list[tuple[str, int, str]] = []
            for resolution, entries in list(resolutions.items()):
                if not isinstance(entries, list):
                    continue
                updated_entries = []
                for idx, entry in enumerate(entries):
                    url = primary_download_source_url(entry.get("u"))
                    existing_link = existing_links.get(
                        (season_num, item_key, resolution, _entry_language_key(entry), _entry_filename_key(entry))
                    )
                    if existing_link:
                        updated_entries.append(_entry_copy(entry, link=existing_link))
                        total_skipped += 1
                        logger.debug(
                            "Skipping S%s %s %s [%s]: already has Drive link",
                            season_num,
                            item_label,
                            resolution,
                            entry.get("l"),
                        )
                        continue
                    updated_entries.append(dict(entry))
                    if url:
                        pending.append((resolution, idx, url))
                        logger.debug(
                            "Queued S%s %s %s [%s]: %s",
                            season_num,
                            item_label,
                            resolution,
                            entry.get("l"),
                            url,
                        )
                item["resolutions"][resolution] = updated_entries
            if pending:
                batch = WebScrapeService.get_urls_parallel([u for _, _, u in pending])
                for (resolution, idx, url), res in zip(pending, batch):
                    current = item["resolutions"][resolution][idx]
                    if isinstance(res, Exception):
                        logger.error(
                            f"Resolving S{season_num} {item_label} {resolution} ({url}): {res}",
                            exc_info=res,
                        )
                        item["resolutions"][resolution][idx] = _entry_copy(current, link="")
                    else:
                        item["resolutions"][resolution][idx] = _entry_copy(current, link=res)
                    total_resolved += 1

            # Callback after each item is fully resolved
            if on_item_resolved:
                on_item_resolved(tvshow_data)
                logger.debug(f"Progress saved: S{season_num} {item_label} resolved")

    logger.info(f"Link resolution complete: {total_resolved} resolved, {total_skipped} skipped (already uploaded)")
    return tvshow_data


def get_content_info(
    url,
    on_progress=None,
    db_match_candidates=None,
    flixbd_results=None,
    existing_result=None,
):
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
    apply_force_is_adult_from_source_urls(data, [url])

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
