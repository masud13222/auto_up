import json
import logging
import os
import re
from multiprocessing import current_process

from upload.models import MediaTask
from upload.service.info import get_content_info
from upload.service.duplicate_checker import (
    _search_db,
    _get_existing_resolutions,
    coerce_matched_task_pk,
    site_row_id_from_duplicate_result,
    coerce_target_site_row_id,
)
from upload.utils.web_scrape import WebScrapeService, normalize_http_url
from upload.utils.drive_file_delete import cleanup_old_drive_files
from llm.utils.name_extractor import extract_title_info
from llm.schema.blocked_names import (
    SITE_NAME,
    TARGET_SITE_ROW_ID_JSON_KEY,
    LEGACY_SITE_ROW_ID_JSON_KEY,
)

from .helpers import save_task, is_drive_link
from .movie_pipeline import process_movie_pipeline
from .tvshow_pipeline import process_tvshow_pipeline

logger = logging.getLogger(__name__)


# Max FlixBD hits to fetch from API and pass to LLM (keeps prompt tokens lower).
_FLIXBD_LLM_MAX_RESULTS = 3


def _normalize_flixbd_resolution_keys(qualities: list) -> list[str]:
    """
    Map API strings like 'HD 720p, HD 1080p' to canonical keys ['720p', '1080p'] for LLM comparison
    against extracted download_links keys (480p, 720p, 1080p).
    """
    if not qualities:
        return []
    blob = " ".join(str(q) for q in qualities)
    seen: set[str] = set()
    out: list[str] = []
    for m in re.finditer(r"\b(480|720|1080|1440|2160)\s*p\b", blob, re.I):
        key = f"{m.group(1)}p"
        if key not in seen:
            seen.add(key)
            out.append(key)
    if re.search(r"\b4\s*k\b", blob, re.I) and "2160p" not in seen:
        seen.add("2160p")
        out.append("2160p")
    order = {"480p": 0, "720p": 1, "1080p": 2, "1440p": 3, "2160p": 4}
    return sorted(out, key=lambda k: order.get(k, 99))


def _result_strip_non_drive_download_links(data: dict) -> dict:
    """For skip-without-upload rows: do not persist generate.php / host links as if final."""
    if not data:
        return data
    out = dict(data)
    dl = out.get("download_links")
    if isinstance(dl, dict):
        out["download_links"] = {k: v for k, v in dl.items() if is_drive_link(v)}
    return out


def _fetch_flixbd_results(name: str, min_score: int = 40) -> list:
    """
    Search FlixBD for existing content by name.
    Returns at most _FLIXBD_LLM_MAX_RESULTS items (score >= min_score), sorted best-first, as dicts with:
      - id, title, match_score
      - download_links (as returned by FlixBD search)
      - qualities (optional, parsed list from download_links.qualities for movies)
    Never raises — returns [] on any error or if FlixBD is not configured.

    Results are passed to LLM as context so it can make better duplicate decisions.
    We do NOT save site_content_id here — that happens only after pipeline completes.
    """
    try:
        from upload.service import flixbd_client as fx
        import httpx
        from rapidfuzz import fuzz

        api_url, api_key = fx._get_config()  # single call — raises RuntimeError if disabled
        endpoint = f"{api_url}/api/v1/search"
        params = {"q": name, "type": "all", "per_page": _FLIXBD_LLM_MAX_RESULTS, "page": 1}

        with httpx.Client(timeout=fx._TIMEOUT) as client:
            resp = client.get(endpoint, params=params, headers=fx._headers(api_key))

        if resp.status_code != 200:
            logger.debug(f"FlixBD search: HTTP {resp.status_code} for '{name}'")
            return []

        try:
            payload = resp.json()
        except ValueError:
            snippet = (resp.text or "")[:300].replace("\n", " ")
            logger.warning(
                f"FlixBD search: invalid JSON for '{name}' (HTTP {resp.status_code}): {snippet!r}"
            )
            return []

        raw_results = payload.get("data", [])
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
            if score >= min_score:  # skip low-relevance results — don't confuse LLM
                fid = item.get("id")
                if fid is None:
                    logger.debug("FlixBD search: skipping hit without id: %r", item_title[:80])
                    continue
                download_links = item.get("download_links") or {}
                qualities_raw = download_links.get("qualities")
                qualities = []
                if isinstance(qualities_raw, str):
                    # Example: "1080p, 720p, 480p"
                    qualities = [q.strip() for q in qualities_raw.split(",") if q.strip()]
                elif isinstance(qualities_raw, list):
                    qualities = [str(q).strip() for q in qualities_raw if str(q).strip()]

                resolution_keys = _normalize_flixbd_resolution_keys(qualities)

                results.append({
                    "id": fid,
                    "title": item_title,
                    "match_score": score,
                    "download_links": download_links,
                    "qualities": qualities,
                    "resolution_keys": resolution_keys,
                })

        # Sort by score so LLM sees best matches first; cap count for token budget
        results.sort(key=lambda x: x["match_score"], reverse=True)
        results = results[:_FLIXBD_LLM_MAX_RESULTS]
        top = results[0]["match_score"] if results else 0
        logger.info(
            f"FlixBD search: {len(results)} result(s) (score>={min_score}, max={_FLIXBD_LLM_MAX_RESULTS}) "
            f"for '{name}' (top={top})"
        )
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

    # Same as _merge_drive_links: new scrape must not wipe Telegram/Worker screenshot URLs
    old_ss = existing_result.get("screen_shots_url")
    if isinstance(old_ss, list) and old_ss:
        cur = result.get("screen_shots_url")
        if not isinstance(cur, list) or not cur:
            result["screen_shots_url"] = list(old_ss)

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

    old_fn = old_result.get("download_filenames")
    if isinstance(old_fn, dict) and old_fn and new_dl:
        merged_fn = dict(new_data.get("download_filenames") or {})
        for res in new_dl:
            cur = merged_fn.get(res)
            if not (isinstance(cur, str) and cur.strip()) and res in old_fn:
                ov = old_fn.get(res)
                if isinstance(ov, str) and ov.strip():
                    merged_fn[res] = ov.strip()
        new_data["download_filenames"] = merged_fn

    # ── TV Show: seasons → download_items → resolutions ──
    old_seasons = {s.get("season_number"): s for s in old_result.get("seasons", [])}
    for new_season in new_data.get("seasons", []):
        snum = new_season.get("season_number")
        old_season = old_seasons.get(snum)
        if not old_season:
            continue

        # Build lookup: label → {resolution: link} and label → full item (for download_filenames)
        old_items = {}
        old_items_full = {}
        for item in old_season.get("download_items", []):
            lab = item.get("label", "")
            old_items[lab] = item.get("resolutions", {})
            old_items_full[lab] = item

        for new_item in new_season.get("download_items", []):
            label = new_item.get("label", "")
            old_res = old_items.get(label, {})
            new_res = new_item.get("resolutions", {})

            for res, link in old_res.items():
                if is_drive_link(link) and res in new_res:
                    new_res[res] = link
                    logger.debug(f"Preserved existing drive link for S{snum} {label} {res}")

            new_item["resolutions"] = new_res

            old_full = old_items_full.get(label) or {}
            old_dfn = old_full.get("download_filenames")
            if isinstance(old_dfn, dict) and old_dfn and new_res:
                merged_dfn = dict(new_item.get("download_filenames") or {})
                for res in new_res:
                    cur = merged_dfn.get(res)
                    if not (isinstance(cur, str) and cur.strip()) and res in old_dfn:
                        ov = old_dfn.get(res)
                        if isinstance(ov, str) and ov.strip():
                            merged_dfn[res] = ov.strip()
                new_item["download_filenames"] = merged_dfn

    # Preserve Telegram/Worker screenshot URLs — no re-capture on duplicate update
    old_ss = old_result.get("screen_shots_url")
    if isinstance(old_ss, list) and old_ss:
        cur = new_data.get("screen_shots_url")
        if not isinstance(cur, list) or not cur:
            new_data["screen_shots_url"] = list(old_ss)

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
        dfn = cleaned.get("download_filenames")
        if isinstance(dfn, dict):
            if cleaned["download_links"]:
                cleaned["download_filenames"] = {
                    k: v for k, v in dfn.items() if k in cleaned["download_links"]
                }
            else:
                cleaned["download_filenames"] = {}

    # Clean TV show seasons
    for season in cleaned.get('seasons', []):
        items_to_keep = []
        for item in season.get('download_items', []):
            res = item.get('resolutions', {})
            cleaned_res = {k: v for k, v in res.items() if is_drive_link(v)}
            if cleaned_res:
                item['resolutions'] = cleaned_res
                dfn = item.get("download_filenames")
                if isinstance(dfn, dict):
                    item["download_filenames"] = {
                        k: v for k, v in dfn.items() if k in cleaned_res
                    }
                items_to_keep.append(item)
        season['download_items'] = items_to_keep

    return cleaned


def _build_db_candidate(task: MediaTask) -> dict:
    """Build a single candidate dict (with PK) for the LLM duplicate prompt."""
    result_data = task.result or {}
    existing_resolutions = _get_existing_resolutions(task)
    is_tvshow = task.content_type == 'tvshow' if task.content_type else bool(result_data.get("seasons"))

    candidate = {
        "id": task.pk,
        "title": task.title,
        "year": result_data.get("year"),
        "resolutions": existing_resolutions,
        "type": "tvshow" if is_tvshow else "movie",
    }

    if is_tvshow:
        episodes = []
        for season in result_data.get("seasons", []):
            for item in season.get("download_items", []):
                label = item.get("label", "")
                res = item.get("resolutions", {})
                ep_res = sorted(k for k, v in res.items() if v)
                episodes.append(f"{label}: {','.join(ep_res)}")
        candidate["episode_count"] = len(episodes)
        candidate["episodes"] = episodes

    return candidate


def _build_db_match_candidates(matches: list[MediaTask]) -> list[dict]:
    """Build a list of candidate dicts for the LLM duplicate prompt."""
    return [_build_db_candidate(task) for task in matches]


def _flixbd_site_id_set(flixbd_results: list | None) -> set[int]:
    """Numeric FlixBD content ids from search results (not MediaTask pks)."""
    out: set[int] = set()
    for r in flixbd_results or []:
        fid = r.get("id")
        if fid is None:
            continue
        try:
            out.add(int(fid))
        except (TypeError, ValueError):
            pass
    return out


def _normalize_duplicate_response(
    dup_result: dict | None,
    db_candidate_map: dict,
    flixbd_results: list,
    media_task_pk: int,
) -> None:
    """Canonicalize duplicate_check keys; promote site id wrongly placed in matched_task_id."""
    if not dup_result or not isinstance(dup_result, dict):
        return
    leg = dup_result.get(LEGACY_SITE_ROW_ID_JSON_KEY)
    cur = dup_result.get(TARGET_SITE_ROW_ID_JSON_KEY)
    if cur is None and leg is not None:
        dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = leg
    dup_result.pop(LEGACY_SITE_ROW_ID_JSON_KEY, None)
    if TARGET_SITE_ROW_ID_JSON_KEY not in dup_result:
        dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = None

    flix_ids = _flixbd_site_id_set(flixbd_results)
    mt = coerce_matched_task_pk(dup_result.get("matched_task_id"))
    fd = coerce_target_site_row_id(dup_result.get(TARGET_SITE_ROW_ID_JSON_KEY))

    if mt is not None and mt not in db_candidate_map:
        if mt in flix_ids and fd is None:
            dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = mt
            dup_result["matched_task_id"] = None
            logger.info(
                "Duplicate: promoted matched_task_id=%s to %s (namespace fix, task pk=%s)",
                mt,
                TARGET_SITE_ROW_ID_JSON_KEY,
                media_task_pk,
            )
        else:
            dup_result["matched_task_id"] = None
            logger.warning(
                "Duplicate: invalid matched_task_id=%s not in DB candidates %s (task pk=%s); cleared",
                mt,
                list(db_candidate_map.keys()),
                media_task_pk,
            )


def _donor_result_for_site_content(
    site_content_id: int,
    exclude_pk: int | None,
    content_type: str,
) -> dict:
    """Drive metadata from another MediaTask row or FlixBD API (movies)."""
    q = MediaTask.objects.filter(site_content_id=site_content_id, status="completed")
    if exclude_pk is not None:
        q = q.exclude(pk=exclude_pk)
    donor = q.order_by("-updated_at").first()
    if donor and isinstance(donor.result, dict) and donor.result:
        logger.info(
            "Donor MediaTask pk=%s for %s site_content_id=%s (merge drive links)",
            donor.pk,
            SITE_NAME,
            site_content_id,
        )
        return dict(donor.result)
    if content_type != "tvshow":
        try:
            from upload.service import flixbd_client as fx

            m = fx.fetch_movie_drive_links_by_quality(int(site_content_id))
            if m:
                logger.info(
                    "Hydrated %s drive link(s) from %s API for movie id=%s",
                    len(m),
                    SITE_NAME,
                    site_content_id,
                )
                return {"download_links": m}
        except Exception as e:
            logger.warning("%s hydrate drive links id=%s: %s", SITE_NAME, site_content_id, e)
    return {}


def process_media_task(task_pk: int) -> str:
    """
    Background task: Full pipeline from URL to Google Drive upload.
    Combined flow (1 LLM call):
    1. Title fetch + DB search (no LLM)
    2. Full page scrape + LLM (extract + duplicate check in one call)
    3. Route to movie or tvshow pipeline
    """
    try:
        media_task = MediaTask.objects.get(pk=task_pk)
    except MediaTask.DoesNotExist:
        # Stale django-q job after duplicate-skip delete, admin delete, or re-queue race.
        logger.warning(
            "process_media_task: MediaTask pk=%s missing (row deleted); stale queue job — skipping",
            task_pk,
        )
        return json.dumps({"status": "skipped", "message": "MediaTask does not exist"})

    # Skip if already completed
    if media_task.status == 'completed':
        logger.info(f"Task already completed, skipping: {media_task.title or media_task.url[:50]} (pk={task_pk})")
        return json.dumps({"status": "skipped", "message": "Already completed"})

    save_task(media_task, status='processing')

    try:
        url = normalize_http_url((media_task.url or "").strip())
        if url != (media_task.url or "").strip():
            media_task.url = url
            media_task.save(update_fields=["url", "updated_at"])
            logger.info(f"Normalized task URL saved: {url}")

        logger.info(
            "Task started for URL: %s (pid=%s worker=%s)",
            url,
            os.getpid(),
            getattr(current_process(), "name", "main"),
        )

        # ── Step 0: Title fetch + DB search (no LLM call) ──
        website_title = WebScrapeService.cinefreak_title(url)
        db_match_candidates = None
        db_candidate_map = {}
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
                    db_match_candidates = _build_db_match_candidates(matches)
                    db_candidate_map = {t.pk: t for t in matches}
                    logger.info(
                        f"Found {len(matches)} DB candidate(s): "
                        + ", ".join(f"[{t.pk}] {t.title}" for t in matches)
                    )
                elif resume_result:
                    logger.info(f"No other match, but task has existing result (reused task). Using self for dup check.")
                    db_match_candidates = [_build_db_candidate(media_task)]
                    db_candidate_map = {media_task.pk: media_task}
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
            db_match_candidates=db_match_candidates,
            flixbd_results=flixbd_results if flixbd_results else None,
            existing_result=resume_result if resume_result else None,
        )
        title = data.get("title", "Unknown")

        # ── Normalize duplicate_check (split MediaTask pk vs FlixBD site id) ──
        _flix_ctx = flixbd_results if flixbd_results else []
        if dup_result:
            _normalize_duplicate_response(
                dup_result, db_candidate_map, _flix_ctx, media_task.pk
            )

        target_site_row_id = site_row_id_from_duplicate_result(dup_result) if dup_result else None

        # ── Resolve existing_task from matched_task_id (DB only) ──
        existing_task = None
        if dup_result:
            matched_pk = coerce_matched_task_pk(dup_result.get("matched_task_id"))
            if matched_pk is not None:
                existing_task = db_candidate_map.get(matched_pk)
                if existing_task:
                    logger.info(
                        "LLM matched DB candidate: [%s] %s",
                        existing_task.pk,
                        existing_task.title,
                    )
                else:
                    dup_result["matched_task_id"] = None
                    logger.warning(
                        "matched_task_id=%s not in DB candidates %s (task pk=%s)",
                        matched_pk,
                        list(db_candidate_map.keys()),
                        media_task.pk,
                    )
            else:
                logger.info("matched_task_id=null — no DB row targeted for merge")

        # ── Merge resume drive links (restart recovery) ──
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

            # update/replace without DB merge: require LLM target site row id for site-targeted partial flows
            if action in ("update", "replace") and not existing_task and target_site_row_id is None:
                logger.warning(
                    "PipelineWarning: duplicate action=%s but no MediaTask match and no %s — "
                    "full process (pk=%s).",
                    action,
                    TARGET_SITE_ROW_ID_JSON_KEY,
                    media_task.pk,
                )
                action = "process"
                dup_result["action"] = "process"
                dup_result["missing_resolutions"] = []
            elif (
                action == "skip"
                and not resume_result
                and not existing_task
                and target_site_row_id is None
            ):
                logger.warning(
                    "Duplicate skip without %s — forcing process (pk=%s)",
                    TARGET_SITE_ROW_ID_JSON_KEY,
                    media_task.pk,
                )
                action = "process"
                dup_result["action"] = "process"

            if dup_result and action == "skip":
                if resume_result:
                    # Reused task — restore to completed with the merged data (preserving Drive links)
                    logger.info(f"DUPLICATE SKIP: {reason} — restoring reused task to completed (pk={media_task.pk})")
                    save_task(media_task, status='completed', result=data)
                else:
                    if not existing_task:
                        sid = target_site_row_id
                        if sid is not None:
                            web_title = data.get("website_movie_title") or data.get("website_tvshow_title") or ""
                            result_skip = _result_strip_non_drive_download_links(data)
                            result_skip = {
                                **result_skip,
                                "skipped_without_upload": True,
                                "skipped_duplicate_source": "flixbd",
                                "flixbd_site_content_id": sid,
                            }
                            save_task(
                                media_task,
                                status="completed",
                                content_type=content_type,
                                title=title,
                                website_title=web_title,
                                result=result_skip,
                                error_message="",
                                site_content_id=sid,
                            )
                            logger.info(
                                "DUPLICATE SKIP: %s — saved %s site id=%s to DB (pk=%s)",
                                reason,
                                SITE_NAME,
                                sid,
                                media_task.pk,
                            )
                        else:
                            logger.info(
                                "DUPLICATE SKIP: %s — deleting task (no %s) (pk=%s)",
                                reason,
                                TARGET_SITE_ROW_ID_JSON_KEY,
                                media_task.pk,
                            )
                            media_task.delete()
                    else:
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

        # LLM target site row id → site_content_id for update/replace only (not plain process)
        if (
            target_site_row_id is not None
            and not media_task.site_content_id
            and action in ("update", "replace")
        ):
            media_task.site_content_id = target_site_row_id
            save_task(media_task, site_content_id=target_site_row_id)
            logger.info(
                "LLM %s site_content_id=%s for '%s' (pk=%s)",
                SITE_NAME,
                target_site_row_id,
                title,
                media_task.pk,
            )

        # ── Merge existing data for update (DB row and/or donor / API by site id) ──
        if action == "update" and not existing_result and target_site_row_id:
            existing_result = _donor_result_for_site_content(
                target_site_row_id, media_task.pk, content_type
            )
            if not existing_result:
                logger.warning(
                    "%s=%s: no donor MediaTask and no API drive map — "
                    "cannot hydrate existing qualities; running full downloads (pk=%s)",
                    TARGET_SITE_ROW_ID_JSON_KEY,
                    target_site_row_id,
                    media_task.pk,
                )
                if dup_result and isinstance(dup_result.get("missing_resolutions"), list):
                    dup_result["missing_resolutions"] = []

        if action == "update" and existing_result:
            is_tvshow = content_type == "tvshow" or bool(existing_result.get("seasons"))
            has_new_eps = dup_result.get("has_new_episodes", False) if dup_result else False

            if is_tvshow and has_new_eps:
                data = _merge_new_episodes(existing_result, data)
                logger.info("Merged new episodes into existing TV show seasons")
            else:
                data = _merge_drive_links(existing_result, data)
                logger.info("Merged existing drive links into new extraction data")

            from upload.service.info import resolve_movie_links, resolve_tvshow_links

            if content_type == "movie":
                data = resolve_movie_links(data, existing_result=existing_result)
            else:
                data = resolve_tvshow_links(
                    data, on_item_resolved=None, existing_result=existing_result
                )

        web_title = data.get("website_movie_title") or data.get("website_tvshow_title") or ""
        save_task(media_task, content_type=content_type, title=title, website_title=web_title, result=data)
        logger.info(f"Detected content type: {content_type} — Title: {title}")

        # ── Step 2: Route to appropriate pipeline ──
        dup_info = {
            "action": action,
            "existing_task": existing_task if action != "process" and existing_task is not None else None,
            "clear_flixbd_links": action == "replace",
        }
        if dup_result:
            mr = dup_result.get("missing_resolutions")
            if isinstance(mr, list) and mr:
                dup_info["missing_resolutions"] = mr

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
