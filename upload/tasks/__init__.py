import json
import logging

from upload.models import MediaTask
from upload.service.info import get_content_info
from upload.service.duplicate_checker import _search_db, _get_existing_resolutions
from upload.utils.web_scrape import WebScrapeService
from upload.utils.drive_file_delete import cleanup_old_drive_files
from llm.utils.name_extractor import extract_title_info

from .helpers import save_task, is_drive_link
from .movie_pipeline import process_movie_pipeline
from .tvshow_pipeline import process_tvshow_pipeline

logger = logging.getLogger(__name__)


def _fetch_flixbd_results(name: str) -> list:
    """
    Search FlixBD for existing content by name.
    Returns max 5 results as a list of {id, title} dicts.
    Never raises — returns [] on any error or if FlixBD is not configured.

    Results are passed to LLM as context so it can make better duplicate decisions.
    We do NOT save site_content_id here — that happens only after pipeline creates content.
    """
    try:
        from upload.service import flixbd_client as fx
        import httpx
        import re
        from rapidfuzz import fuzz

        fx._get_config()  # Raises RuntimeError if not configured/disabled

        api_url, api_key = fx._get_config()
        endpoint = f"{api_url}/api/v1/search"
        params = {"q": name, "type": "all", "per_page": 5, "page": 1}

        with httpx.Client(timeout=fx._TIMEOUT) as client:
            resp = client.get(endpoint, params=params, headers=fx._headers(api_key))

        if resp.status_code != 200:
            logger.debug(f"FlixBD search: HTTP {resp.status_code} for '{name}'")
            return []

        raw_results = resp.json().get("data", [])
        if not raw_results:
            logger.info(f"FlixBD search: no results for '{name}'")
            return []

        _year_re = re.compile(r'\b(19|20)\d{2}\b')
        name_lower = name.lower().strip()

        results = []
        for item in raw_results:
            item_title = item.get("title", "")
            year_match = _year_re.search(item_title)
            clean = item_title[:year_match.start()].strip() if year_match else item_title
            score = fuzz.ratio(name_lower, clean.lower())
            results.append({
                "id": item["id"],
                "title": item_title,
                "match_score": score,
            })

        # Sort by score so LLM sees best matches first
        results.sort(key=lambda x: x["match_score"], reverse=True)
        logger.info(f"FlixBD search: {len(results)} result(s) for '{name}' (top score={results[0]['match_score'] if results else 0})")
        return results

    except RuntimeError as e:
        logger.debug(f"FlixBD search skipped: {e}")
        return []
    except Exception as e:
        logger.warning(f"FlixBD search error for '{name}': {e}")
        return []


def _merge_new_episodes(existing_result: dict, new_data: dict) -> dict:
    """
    Merge new TV show episodes from new_data INTO existing_result.

    Strategy:
    - Keep ALL existing seasons/episodes (with their Drive links) intact
    - Append only NEW episodes (by label) that don't exist in existing
    - Never overwrite existing episodes — they already have Drive links

    This handles the case where a show releases new episodes under a new URL
    (e.g. Bachelor Point ep 1-72 at URL-A, ep 73-80 at URL-B).
    The new ep 73-80 batch gets appended to the existing season, not replacing it.
    """
    # Only applies to TV shows
    existing_seasons = existing_result.get("seasons", [])
    new_seasons = new_data.get("seasons", [])

    if not existing_seasons:
        # No existing seasons at all — just use new_data as-is
        return new_data

    if not new_seasons:
        # New data has no seasons (edge case) — preserve existing entirely
        logger.warning("Episode merge: new_data has no seasons, preserving existing result to avoid data loss")
        result = dict(existing_result)
        result.update({k: v for k, v in new_data.items() if k not in ("seasons",)})
        result["seasons"] = existing_seasons
        return result

    # Build mutable copy of existing seasons indexed by season_number
    merged_seasons = {s["season_number"]: dict(s) for s in existing_seasons}
    for s in merged_seasons.values():
        # Make download_items a mutable list copy
        s["download_items"] = list(s.get("download_items", []))

    for new_season in new_seasons:
        snum = new_season.get("season_number")
        new_items = new_season.get("download_items", [])

        if snum not in merged_seasons:
            # Entirely new season — add as-is
            merged_seasons[snum] = dict(new_season)
            logger.info(f"Episode merge: added new season {snum}")
            continue

        # Season exists — add only NEW episode labels
        existing_labels = {
            item.get("label", "") for item in merged_seasons[snum]["download_items"]
        }

        added = []
        for new_item in new_items:
            label = new_item.get("label", "")
            if label not in existing_labels:
                merged_seasons[snum]["download_items"].append(new_item)
                existing_labels.add(label)
                added.append(label)

        if added:
            logger.info(f"Episode merge: appended {len(added)} new episode(s) to S{snum}: {added}")
        else:
            logger.info(f"Episode merge: no new episodes to add for S{snum} (all labels already exist)")

    # Reconstruct seasons list sorted by season_number
    merged_season_list = sorted(merged_seasons.values(), key=lambda s: s["season_number"])

    # Build final merged data: keep new_data metadata but use merged seasons
    result = dict(existing_result)    # start from existing (has Drive links)
    result.update({                   # overlay new metadata fields
        k: v for k, v in new_data.items()
        if k not in ("seasons",)      # don't overwrite seasons with new_data's seasons
    })
    result["seasons"] = merged_season_list
    return result


def _merge_drive_links(old_result: dict, new_data: dict) -> dict:
    """
    Merge existing Drive links from old_result into new_data.
    When action=update, old_result has drive.google.com links for
    already-uploaded resolutions. new_data has fresh download URLs.
    We replace download URLs with drive links where they exist,
    so the pipeline's is_drive_link() check skips them.
    """
    # ── Movie: download_links ──
    old_dl = old_result.get("download_links", {})
    new_dl = new_data.get("download_links", {})
    if old_dl and new_dl:
        for res, link in old_dl.items():
            if is_drive_link(link) and res in new_dl:
                new_dl[res] = link
                logger.debug(f"Preserved existing drive link for {res}")
        new_data["download_links"] = new_dl

    # ── TV Show: seasons → download_items → resolutions ──
    old_seasons = {s.get("season_number"): s for s in old_result.get("seasons", [])}
    for new_season in new_data.get("seasons", []):
        snum = new_season.get("season_number")
        old_season = old_seasons.get(snum)
        if not old_season:
            continue

        # Build lookup: label → {resolution: link}
        old_items = {}
        for item in old_season.get("download_items", []):
            old_items[item.get("label", "")] = item.get("resolutions", {})

        for new_item in new_season.get("download_items", []):
            label = new_item.get("label", "")
            old_res = old_items.get(label, {})
            new_res = new_item.get("resolutions", {})

            for res, link in old_res.items():
                if is_drive_link(link) and res in new_res:
                    new_res[res] = link
                    logger.debug(f"Preserved existing drive link for S{snum} {label} {res}")

            new_item["resolutions"] = new_res

    return new_data


def _has_drive_links(result: dict) -> bool:
    """Check if a result dict actually contains any Google Drive upload links."""
    if not result:
        return False
    # Check movie download_links
    for link in result.get('download_links', {}).values():
        if is_drive_link(link):
            return True
    # Check TV show seasons
    for season in result.get('seasons', []):
        for item in season.get('download_items', []):
            for res_val in item.get('resolutions', {}).values():
                if is_drive_link(res_val):
                    return True
    return False


def _clean_result_keep_drive_links(result: dict) -> dict:
    """Strip resolutions without Drive links from a failed task's result.
    
    Keeps metadata (title, plot, poster, etc.) intact.
    Only removes resolution entries that were scraped but never uploaded.
    This ensures reused tasks only have real uploaded data in resume_result.
    """
    if not result:
        return result

    cleaned = dict(result)

    # Clean movie download_links
    if 'download_links' in cleaned:
        cleaned['download_links'] = {
            k: v for k, v in cleaned['download_links'].items()
            if is_drive_link(v)
        }

    # Clean TV show seasons
    for season in cleaned.get('seasons', []):
        items_to_keep = []
        for item in season.get('download_items', []):
            res = item.get('resolutions', {})
            cleaned_res = {k: v for k, v in res.items() if is_drive_link(v)}
            if cleaned_res:
                item['resolutions'] = cleaned_res
                items_to_keep.append(item)
        season['download_items'] = items_to_keep

    return cleaned


def _build_db_match_info(existing_task: MediaTask) -> dict:
    """Build the DB match info dict to inject into the combined LLM prompt."""
    result_data = existing_task.result or {}
    existing_resolutions = _get_existing_resolutions(existing_task)

    is_tvshow = existing_task.content_type == 'tvshow' if existing_task.content_type else bool(result_data.get("seasons"))

    info = {
        "existing_title": existing_task.title,
        "existing_resolutions": existing_resolutions,
        "existing_type": "tvshow" if is_tvshow else "movie",
    }

    if is_tvshow:
        episodes = []
        for season in result_data.get("seasons", []):
            for item in season.get("download_items", []):
                label = item.get("label", "")
                res = item.get("resolutions", {})
                ep_res = sorted(k for k, v in res.items() if v)
                episodes.append(f"{label}: {','.join(ep_res)}")
        info["existing_episode_count"] = len(episodes)
        info["existing_episodes"] = episodes

    return info


def process_media_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    Combined flow (1 LLM call):
    1. Title fetch + DB search (no LLM)
    2. Full page scrape + LLM (extract + duplicate check in one call)
    3. Route to movie or tvshow pipeline
    """
    media_task = MediaTask.objects.get(pk=task_pk)

    # Skip if already completed
    if media_task.status == 'completed':
        logger.info(f"Task already completed, skipping: {media_task.title or media_task.url[:50]} (pk={task_pk})")
        return json.dumps({"status": "skipped", "message": "Already completed"})

    save_task(media_task, status='processing')

    try:
        url = media_task.url
        logger.info(f"Task started for URL: {url}")

        # ── Step 0: Title fetch + DB search (no LLM call) ──
        website_title = WebScrapeService.cinefreak_title(url)
        db_match_info = None
        flixbd_results = []
        existing_task = None
        existing_result = {}
        resume_result_raw = _clean_result_keep_drive_links(media_task.result or {})
        has_existing_drive = _has_drive_links(resume_result_raw)
        resume_result = resume_result_raw if has_existing_drive else {}


        if website_title:
            logger.info(f"Website title: {website_title}")
            info = extract_title_info(website_title)
            name, year = info.title, info.year
            logger.info(f"Extracted: name='{name}', year='{year}'")

            if name:
                matches = _search_db(name, year, exclude_pk=media_task.pk)
                if matches:
                    existing_task = matches[0]
                    logger.info(f"Found existing match: [{existing_task.pk}] {existing_task.title}")
                    db_match_info = _build_db_match_info(existing_task)
                elif resume_result:
                    logger.info(f"No other match, but task has existing result (reused task). Using self for dup check.")
                    db_match_info = _build_db_match_info(media_task)
                else:
                    logger.info(f"No existing match for '{name}'. New content.")

                # ── FlixBD search (pre-LLM, results passed to LLM as context) ──
                flixbd_results = _fetch_flixbd_results(name)

        # ── Step 1: Full scrape + combined LLM call (extract + dup check) ──
        def _on_progress(data):
            title = data.get("title", "")
            if title and not media_task.title:
                save_task(media_task, title=title, result=data)
                logger.info(f"Saved title: {title}")
            else:
                save_task(media_task, result=data)

        content_type, data, dup_result = get_content_info(
            url,
            on_progress=_on_progress,
            db_match_info=db_match_info,
            flixbd_results=flixbd_results if flixbd_results else None,
            existing_result=resume_result if resume_result else None,
        )
        title = data.get("title", "Unknown")

        # ── Merge resume drive links (restart recovery) ──
        # If this task had partial uploads before restart, preserve those drive links
        if resume_result and not existing_task:
            data = _merge_drive_links(resume_result, data)
            logger.info("Checked for drive links from previous partial upload (resume)")

        # ── Handle duplicate result ──
        action = "process"
        if dup_result:
            action = dup_result.get("action", "process")
            reason = dup_result.get("reason", "LLM decision")

            # Validate action
            if action not in ("skip", "update", "replace", "process"):
                action = "process"
                reason = f"Invalid LLM action, defaulting to process: {dup_result}"

            logger.info(f"Duplicate check result: action={action}, reason={reason}")

            if action == "skip":
                if resume_result:
                    # Reused task — restore to completed with the merged data (preserving Drive links)
                    logger.info(f"DUPLICATE SKIP: {reason} — restoring reused task to completed (pk={media_task.pk})")
                    save_task(media_task, status='completed', result=data)
                else:
                    # New task — safe to delete
                    logger.info(f"DUPLICATE SKIP: {reason} — deleting task (pk={media_task.pk})")
                    media_task.delete()
                return json.dumps({"status": "skipped", "message": reason})

            if action in ("update", "replace") and existing_task:
                logger.info(f"DUPLICATE {action.upper()}: {reason} — using existing task [{existing_task.pk}], deleting new entry (pk={media_task.pk})")
                existing_result = existing_task.result or {}

                # Register this new URL in the existing task's extra_urls
                new_url = url
                if existing_task.add_extra_url(new_url):
                    existing_task.save(update_fields=['extra_urls', 'updated_at'])
                    logger.info(f"Registered new source URL in existing task extra_urls: {new_url}")

                # Replace: clean up old Drive files before re-downloading
                if action == "replace" and existing_result:
                    logger.info(f"Cleaning up old Drive files for replace action...")
                    cleanup_old_drive_files(existing_result)

                media_task.delete()
                media_task = existing_task
                media_task.status = 'processing'
                media_task.error_message = ''
                media_task.save(update_fields=['status', 'error_message', 'updated_at'])

        # ── Merge existing data for update action ──
        if action == "update" and existing_result:
            is_tvshow = content_type == "tvshow" or bool(existing_result.get("seasons"))
            has_new_eps = dup_result.get("has_new_episodes", False) if dup_result else False

            if is_tvshow and has_new_eps:
                # TV show with new episodes: merge new episodes INTO existing (do NOT replace)
                data = _merge_new_episodes(existing_result, data)
                logger.info(f"Merged new episodes into existing TV show seasons")
            else:
                # Movie or TV show resolution update: just preserve existing drive links
                data = _merge_drive_links(existing_result, data)
                logger.info(f"Merged existing drive links into new extraction data")

        web_title = data.get("website_movie_title") or data.get("website_tvshow_title") or ""
        save_task(media_task, content_type=content_type, title=title, website_title=web_title, result=data)
        logger.info(f"Detected content type: {content_type} — Title: {title}")

        # ── Step 2: Route to appropriate pipeline ──
        dup_info = {"action": action, "existing_task": existing_task if action != "process" else None}
        if content_type == "tvshow":
            return process_tvshow_pipeline(media_task, data, dup_info=dup_info)
        else:
            return process_movie_pipeline(media_task, data, dup_info=dup_info)

    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        # Clean result: only keep items that have Drive links (remove unprocessed scrape data)
        cleaned = _clean_result_keep_drive_links(media_task.result)
        save_task(media_task, status='failed', error_message=str(e), result=cleaned)
        return json.dumps({"status": "error", "message": str(e)})


# Backward compatibility: old queued tasks still reference this name
process_movie_task = process_media_task
