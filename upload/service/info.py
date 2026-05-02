import json
import logging
from datetime import timedelta
from typing import Any, Literal

from django.utils import timezone
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError

from upload.utils.web_scrape import WebScrapeService
from upload.utils.tv_items import tv_item_key
from upload.utils.media_entry_helpers import (
    coerce_download_source_value,
    coerce_entry_language_value,
    entry_language_key,
    is_drive_link,
    primary_download_source_url,
)
from llm.services import LLMService
from llm.json_repair import repair_json
from llm.schema import get_combined_system_prompt, validate_combined_extract
from llm.schema.response_validate import (
    LLM_SCHEMA_RETRY_MAX as _LLM_JSON_RETRY_MAX,
    VALIDATION_RETRY_SUFFIX,
    format_validation_detail,
)
from llm.schema.blocked_names import TARGET_SITE_ROW_ID_JSON_KEY
from llm.update_pass import compute_update_delta
from upload.utils.resolution_policy import apply_upload_resolution_policy
from upload.utils.force_is_adult_source_domain import apply_force_is_adult_from_source_urls

logger = logging.getLogger(__name__)

_JSON_FOR_DB = {"indent": 2, "ensure_ascii": False}
_JSON_RETRY_USER_SUFFIX = (
    "\n\nReturn a single valid JSON object only (no markdown fences, no text outside JSON). "
    "Previous attempt produced invalid JSON."
)


def _combined_retry_user_suffix(exc: Exception) -> str:
    """Instructor-style: send schema validation errors back to the model on retry."""
    if isinstance(exc, (ValidationError, JsonSchemaValidationError)):
        return VALIDATION_RETRY_SUFFIX.format(detail=format_validation_detail(exc))
    return _JSON_RETRY_USER_SUFFIX + f"\n\nJSON parse/repair error:\n{str(exc)[:1500]}"


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
    search_query_json: dict | None = None,
) -> None:
    has_dup = bool(dup_result)
    has_ctx = bool(db_match_candidates or flixbd_results or extra_context or search_query_json)
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
        if search_query_json:
            row.search_query_json = json.dumps(search_query_json, **_JSON_FOR_DB)
            update_fields.append("search_query_json")
        if update_fields:
            row.save(update_fields=update_fields)
    except Exception as e:
        logger.warning("Could not save duplicate snapshot to LLMUsage: %s", e)


def get_structured_output(llm_response: str) -> dict:
    return repair_json(llm_response)


def _repair_with_llm_retry(
    *,
    llm_response: str,
    original_user_prompt: str,
    system_prompt: str,
    purpose: str,
    event_log: list | None = None,
    persist_usage: bool = True,
    capture_usage_events: list[dict[str, Any]] | None = None,
    locked_content_type: Literal["movie", "tvshow"] = "movie",
    require_duplicate_check: bool = False,
) -> tuple[dict, str]:
    """
    Parse JSON (with ``repair_json``), then validate with JSON Schema (same dicts as prompts).
    Retries the LLM with parse or validation feedback like instructor-style re-asking.
    """
    current_response = llm_response
    last_error: Exception | None = None

    for attempt in range(_LLM_JSON_RETRY_MAX + 1):
        try:
            parsed = get_structured_output(current_response)
            validated = validate_combined_extract(
                parsed,
                locked_content_type=locked_content_type,
                require_duplicate_check=require_duplicate_check,
            )
            return validated, current_response
        except Exception as e:
            last_error = e
            if attempt >= _LLM_JSON_RETRY_MAX:
                break
            logger.warning(
                "Combined extract parse/validation failed for purpose=%s (attempt %s/%s): %s. Retrying LLM.",
                purpose or "n/a",
                attempt + 1,
                _LLM_JSON_RETRY_MAX + 1,
                e,
            )
            if event_log is not None:
                kind = (
                    "validation_failed"
                    if isinstance(e, (ValidationError, JsonSchemaValidationError))
                    else "json_parse_failed"
                )
                event_log.append(
                    {
                        "kind": kind,
                        "attempt": attempt + 1,
                        "error": str(e)[:800],
                    }
                )
            repair_prompt = (original_user_prompt or "") + _combined_retry_user_suffix(e)
            current_response = LLMService.generate_completion(
                prompt=repair_prompt,
                system_prompt=system_prompt,
                purpose=purpose,
                persist_usage=persist_usage,
                capture_usage_events=capture_usage_events,
            )
            if event_log is not None:
                event_log.append(
                    {
                        "kind": "llm_response",
                        "label": "json_or_schema_repair",
                        "text": current_response,
                    }
                )

    raise last_error or ValueError("Could not parse or validate structured JSON response")


def _normalize_duplicate_check(dup_result) -> dict | None:
    if dup_result is None:
        return None
    if not isinstance(dup_result, dict):
        return None
    if TARGET_SITE_ROW_ID_JSON_KEY not in dup_result:
        dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = None
    if "missing_resolutions" not in dup_result or not isinstance(
        dup_result.get("missing_resolutions"), list
    ):
        dup_result["missing_resolutions"] = []
    if "has_new_episodes" not in dup_result:
        dup_result["has_new_episodes"] = False
    return dup_result


def _apply_pass2_update(
    content_type: str,
    data: dict,
    dup_result: dict | None,
    db_match_candidates: list | None,
    flixbd_results: list | None,
    *,
    persist_usage: bool = True,
    capture_usage_events: list[dict[str, Any]] | None = None,
) -> tuple[dict, dict | None]:
    if not isinstance(dup_result, dict) or dup_result.get("action") != "update":
        return data, dup_result

    search_context = {
        "db_match_candidates": db_match_candidates or [],
        "flixbd_results": flixbd_results or [],
    }
    logger.info("Pass-2: running delta filter (content_type=%s)", content_type)
    pass2 = compute_update_delta(
        content_type,
        data,
        search_context,
        persist_usage=persist_usage,
        capture_usage_events=capture_usage_events,
    )
    if pass2 is None:
        logger.warning("Pass-2 delta filter returned None. Falling back to full Pass-1 data.")
        return data, dup_result

    p2_action = pass2.get("action")
    p2_data = pass2.get("data")
    p2_reason = pass2.get("reason", "")

    if p2_action == "skip":
        logger.info("Pass-2 decided: nothing to update → action=skip. reason=%s", p2_reason)
        dup_result["action"] = "skip"
        if p2_reason:
            dup_result["reason"] = f"[Pass-2] {p2_reason}"
        return data, dup_result

    if not isinstance(p2_data, dict):
        logger.warning("Pass-2 returned unexpected structure. Using full data.")
        return data, dup_result

    is_empty_delta = (
        (content_type == "movie" and not p2_data.get("download_links"))
        or (content_type == "tvshow" and not p2_data.get("seasons"))
    )
    if is_empty_delta:
        logger.info("Pass-2 said update but delta is empty → forcing skip. reason=%s", p2_reason)
        dup_result["action"] = "skip"
        dup_result["reason"] = f"[Pass-2] {p2_reason or 'empty delta'}"
        return data, dup_result

    if content_type == "movie" and isinstance(p2_data.get("download_links"), dict):
        data["download_links"] = p2_data["download_links"]
        logger.info(
            "Pass-2 applied: movie download_links replaced with delta (%d res).",
            len(p2_data["download_links"]),
        )
    elif content_type == "tvshow" and isinstance(p2_data.get("seasons"), list):
        data["seasons"] = p2_data["seasons"]
        logger.info("Pass-2 applied: tvshow seasons replaced with delta (%d seasons).", len(p2_data["seasons"]))
    else:
        logger.warning("Pass-2 returned data but no usable key for content_type=%s. Using full data.", content_type)

    return data, dup_result


def detect_and_extract(
    html_content: str,
    *,
    locked_content_type: Literal["movie", "tvshow"],
    db_match_candidates: list | None = None,
    flixbd_results: list | None = None,
    search_query_json: dict | None = None,
    debug_capture: dict | None = None,
    persist_usage: bool = True,
    capture_usage_events: list[dict[str, Any]] | None = None,
) -> tuple:
    from settings.models import UploadSettings

    settings = UploadSettings.objects.first()
    extra_below = settings.extra_res_below if settings else False
    extra_above = settings.extra_res_above if settings else False
    max_extra = settings.max_extra_resolutions if settings else 0

    system_prompt = get_combined_system_prompt(
        locked_content_type,
        extra_below=extra_below,
        extra_above=extra_above,
        max_extra=max_extra,
        db_match_candidates=db_match_candidates,
        flixbd_results=flixbd_results,
    )
    has_dup_ctx = bool(db_match_candidates or flixbd_results)
    dup_tag = " + duplicate check" if has_dup_ctx else ""
    logger.info(
        "Detecting + extracting%s (res: below=%s, above=%s, max=%s)...",
        dup_tag,
        extra_below,
        extra_above,
        max_extra,
    )

    purpose = "extract+dup_check" if has_dup_ctx else "extract"
    llm_events: list | None = None
    if debug_capture is not None:
        llm_events = []
        debug_capture["system_prompt"] = system_prompt
        debug_capture["user_prompt_char_length"] = len(html_content or "")
        debug_capture["locked_content_type"] = locked_content_type
        debug_capture["purpose"] = purpose
        debug_capture["resolution_settings"] = {
            "extra_below": extra_below,
            "extra_above": extra_above,
            "max_extra": max_extra,
        }
        debug_capture["llm_events"] = llm_events

    llm_response = LLMService.generate_completion(
        prompt=html_content,
        system_prompt=system_prompt,
        purpose=purpose,
        persist_usage=persist_usage,
        capture_usage_events=capture_usage_events,
    )
    if llm_events is not None:
        llm_events.append(
            {
                "kind": "llm_response",
                "label": "combined_extract",
                "text": llm_response,
            }
        )

    result, llm_response = _repair_with_llm_retry(
        llm_response=llm_response,
        original_user_prompt=html_content,
        system_prompt=system_prompt,
        purpose=purpose,
        event_log=llm_events,
        persist_usage=persist_usage,
        capture_usage_events=capture_usage_events,
        locked_content_type=locked_content_type,
        require_duplicate_check=has_dup_ctx,
    )
    content_type = result.get("content_type")
    if content_type != locked_content_type:
        logger.warning(
            "LLM content_type=%r != locked %r — using locked type for pipeline.",
            content_type,
            locked_content_type,
        )
    content_type = locked_content_type
    data = result.get("data", {})
    dup_result = _normalize_duplicate_check(result.get("duplicate_check", None))

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

    if persist_usage and (has_dup_ctx or search_query_json):
        _save_duplicate_usage_snapshot_to_latest_usage(
            dup_result=dup_result,
            db_match_candidates=db_match_candidates,
            flixbd_results=flixbd_results,
            purpose=purpose,
            response_text=llm_response,
            search_query_json=search_query_json,
        )

    title = data.get("title", "Unknown")
    logger.info("Detected: %s — Title: %s", content_type, title)
    if dup_result:
        logger.info(
            "Duplicate check: action=%s, reason=%s",
            dup_result.get("action"),
            (dup_result.get("reason", "") or "")[:80],
        )

    data, dup_result = _apply_pass2_update(
        content_type,
        data,
        dup_result,
        db_match_candidates,
        flixbd_results,
        persist_usage=persist_usage,
        capture_usage_events=capture_usage_events,
    )

    if debug_capture is not None:
        debug_capture["final_response_text"] = llm_response
        debug_capture["parsed_summary"] = {
            "content_type": content_type,
            "title": (data or {}).get("title"),
            "duplicate_action": (dup_result or {}).get("action") if dup_result else None,
        }

    return content_type, data, dup_result


def _build_movie_existing_links(existing_result: dict | None) -> dict:
    existing_links: dict = {}
    if not existing_result:
        return existing_links
    for resolution, entries in existing_result.get("download_links", {}).items():
        for entry in entries if isinstance(entries, list) else []:
            drive_link = primary_download_source_url(entry.get("u"))
            if is_drive_link(entry.get("u")):
                key = (resolution, _entry_language_key(entry), _entry_filename_key(entry))
                existing_links[key] = drive_link
    return existing_links


def resolve_movie_links(movie_data: dict, existing_result: dict | None = None) -> dict:
    existing_links = _build_movie_existing_links(existing_result)
    download_links = movie_data.get("download_links", {})
    if not download_links:
        return movie_data

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
                logger.error("Resolving movie %s (%s): %s", resolution, url, res, exc_info=res)
                movie_data["download_links"][resolution][idx] = _entry_copy(current, link="")
            else:
                movie_data["download_links"][resolution][idx] = _entry_copy(current, link=res)
            resolved += 1

    if skipped:
        logger.info("Link resolution: %s resolved, %s skipped (already uploaded)", resolved, skipped)
    return movie_data


def _build_tv_existing_links(existing_result: dict | None) -> dict:
    existing_links: dict = {}
    if not existing_result:
        return existing_links
    for season in existing_result.get("seasons", []):
        snum = season.get("season_number")
        for item in season.get("download_items", []):
            key = tv_item_key(item)
            for resolution, entries in item.get("resolutions", {}).items():
                for entry in entries if isinstance(entries, list) else []:
                    drive_link = primary_download_source_url(entry.get("u"))
                    if is_drive_link(entry.get("u")):
                        lk = (snum, key, resolution, _entry_language_key(entry), _entry_filename_key(entry))
                        existing_links[lk] = drive_link
    return existing_links


def resolve_tvshow_links(
    tvshow_data: dict,
    on_item_resolved=None,
    existing_result: dict | None = None,
) -> dict:
    seasons = tvshow_data.get("seasons", [])
    if not seasons:
        return tvshow_data

    existing_links = _build_tv_existing_links(existing_result)
    total_skipped = 0
    total_resolved = 0

    extra = f" ({len(existing_links)} existing Drive links to skip)" if existing_links else ""
    logger.info("Resolving download links for %s season(s)%s", len(seasons), extra)

    for season in seasons:
        season_num = season.get("season_number", "?")
        download_items = season.get("download_items", [])

        for item in download_items:
            item_label = item.get("label", "Unknown")
            item_key = tv_item_key(item)
            resolutions = item.get("resolutions", {})

            all_uploaded = False
            if resolutions and existing_links:
                all_uploaded = all(
                    (season_num, item_key, resolution, _entry_language_key(entry), _entry_filename_key(entry))
                    in existing_links
                    for resolution, entries in resolutions.items()
                    for entry in (entries if isinstance(entries, list) else [])
                )

            if all_uploaded:
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
                logger.debug("Skipping S%s %s: all resolutions already uploaded", season_num, item_label)
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
                            "Resolving S%s %s %s (%s): %s",
                            season_num,
                            item_label,
                            resolution,
                            url,
                            res,
                            exc_info=res,
                        )
                        item["resolutions"][resolution][idx] = _entry_copy(current, link="")
                    else:
                        item["resolutions"][resolution][idx] = _entry_copy(current, link=res)
                    total_resolved += 1

            if on_item_resolved:
                on_item_resolved(tvshow_data)
                logger.debug("Progress saved: S%s %s resolved", season_num, item_label)

    logger.info(
        "Link resolution complete: %s resolved, %s skipped (already uploaded)",
        total_resolved,
        total_skipped,
    )
    return tvshow_data


def get_content_info(
    url,
    on_progress=None,
    db_match_candidates=None,
    flixbd_results=None,
    existing_result=None,
    search_query_json: dict | None = None,
    page_markdown: str | None = None,
    *,
    locked_content_type: Literal["movie", "tvshow"],
):
    logger.info("Starting content info extraction for: %s", url)
    html_content = (
        page_markdown.strip()
        if page_markdown and str(page_markdown).strip()
        else WebScrapeService.get_page_content(url)
    )
    if not html_content:
        logger.error("Failed to scrape content from %s", url)
        raise RuntimeError("Failed to scrape page content from the given URL.")

    content_type, data, dup_result = detect_and_extract(
        html_content,
        locked_content_type=locked_content_type,
        db_match_candidates=db_match_candidates,
        flixbd_results=flixbd_results,
        search_query_json=search_query_json,
    )
    apply_force_is_adult_from_source_urls(data, [url])

    if on_progress:
        on_progress(data)

    if dup_result and dup_result.get("action") == "skip":
        logger.info("Duplicate skip detected during extraction. Skipping URL resolution.")
        return content_type, data, dup_result

    if content_type == "tvshow":
        data = resolve_tvshow_links(data, on_item_resolved=on_progress, existing_result=existing_result)
        logger.info("TV show info extraction complete.")
    else:
        data = resolve_movie_links(data, existing_result=existing_result)
        logger.info("Movie info extraction complete.")

    return content_type, data, dup_result
