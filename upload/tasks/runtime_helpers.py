import json
import logging
import re

from django.db import transaction

from upload.models import MediaTask
from upload.service.duplicate_checker import (
    _get_existing_resolutions,
    coerce_matched_task_pk,
    coerce_target_site_row_id,
)
from upload.tasks.helpers import (
    coerce_download_source_value,
    coerce_entry_language_value,
    entry_language_key,
    is_drive_link,
    normalize_result_download_languages,
    primary_download_source_url,
)
from upload.utils.tv_items import tv_item_key
from llm.schema.blocked_names import (
    SITE_NAME,
    TARGET_SITE_ROW_ID_JSON_KEY,
    LEGACY_SITE_ROW_ID_JSON_KEY,
)

logger = logging.getLogger(__name__)


# Max FlixBD hits to fetch from API and pass to LLM (keeps prompt tokens lower).
_FLIXBD_LLM_MAX_RESULTS = 3


def _entry_language_key(entry: dict) -> str:
    return entry_language_key((entry or {}).get("l"))


def _entry_link(entry: dict) -> str:
    return primary_download_source_url((entry or {}).get("u"))


def _entry_filename(entry: dict) -> str:
    return str((entry or {}).get("f") or "").strip()


def _entry_copy(entry: dict, *, link: str) -> dict:
    out = {
        "u": coerce_download_source_value(link),
        "l": coerce_entry_language_value(entry.get("l")),
        "f": _entry_filename(entry),
    }
    if isinstance(entry.get("s"), str) and entry["s"].strip():
        out["s"] = entry["s"].strip()
    return out


def _json_clone(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def _entry_size(entry: dict) -> str:
    return str((entry or {}).get("s") or "").strip()


def _same_snapshot_entry(existing: dict, incoming: dict) -> bool:
    if _entry_language_key(existing) != _entry_language_key(incoming):
        return False
    existing_filename = _entry_filename(existing).lower()
    incoming_filename = _entry_filename(incoming).lower()
    if existing_filename and incoming_filename:
        return existing_filename == incoming_filename
    existing_size = _entry_size(existing).lower()
    incoming_size = _entry_size(incoming).lower()
    if existing_size and incoming_size:
        return existing_size == incoming_size
    existing_link = _entry_link(existing)
    incoming_link = _entry_link(incoming)
    if existing_link and incoming_link:
        return existing_link == incoming_link
    return False


def _snapshot_entry_with_metadata(incoming: dict, base_entries: list) -> dict:
    incoming_copy = _json_clone(incoming)
    incoming_copy.setdefault("f", "")
    incoming_copy["l"] = coerce_entry_language_value(incoming_copy.get("l"))
    incoming_copy["u"] = coerce_download_source_value(incoming_copy.get("u"))

    for existing in base_entries or []:
        if not isinstance(existing, dict):
            continue
        if not _same_snapshot_entry(existing, incoming_copy):
            continue
        if not _entry_filename(incoming_copy) and _entry_filename(existing):
            incoming_copy["f"] = _entry_filename(existing)
        if not _entry_size(incoming_copy) and _entry_size(existing):
            incoming_copy["s"] = _entry_size(existing)
        break
    return incoming_copy


def extract_site_sync_snapshot_result(snapshot: dict | None) -> dict:
    if not isinstance(snapshot, dict):
        return {}
    data = snapshot.get("data")
    return _json_clone(data) if isinstance(data, dict) and data else {}


def _published_site_view(data: dict) -> dict:
    if not isinstance(data, dict):
        return {}
    return normalize_result_download_languages(clean_result_keep_drive_links(_json_clone(data)))


def overlay_site_sync_snapshot(existing_result: dict, snapshot_result: dict, content_type: str) -> dict:
    base = _json_clone(existing_result or {}) if isinstance(existing_result, dict) else {}
    if not isinstance(snapshot_result, dict) or not snapshot_result:
        return base
    snapshot_clean = _published_site_view(snapshot_result)

    if content_type == "movie":
        base_links = base.get("download_links") if isinstance(base.get("download_links"), dict) else {}
        live_links = snapshot_clean.get("download_links") if isinstance(snapshot_clean.get("download_links"), dict) else {}
        merged_links = {}
        for quality, entries in live_links.items():
            normalized_entries = []
            base_entries = base_links.get(quality, [])
            for entry in entries if isinstance(entries, list) else []:
                if not isinstance(entry, dict):
                    continue
                hydrated = _snapshot_entry_with_metadata(entry, base_entries)
                if is_drive_link(_entry_link(hydrated)):
                    normalized_entries.append(hydrated)
            if normalized_entries:
                merged_links[quality] = normalized_entries
        base["download_links"] = merged_links
        base.pop("download_filenames", None)
    else:
        base_seasons = {
            season.get("season_number"): _json_clone(season)
            for season in (base.get("seasons") or [])
            if isinstance(season, dict) and season.get("season_number") is not None
        }
        authoritative_seasons = []
        for season in snapshot_clean.get("seasons") or []:
            if not isinstance(season, dict):
                continue
            season_num = season.get("season_number")
            if season_num is None:
                continue
            target_season = {"season_number": season_num, "download_items": []}
            base_season = base_seasons.get(season_num) or {}
            existing_items = {
                tv_item_key(item): item
                for item in base_season.get("download_items", [])
                if isinstance(item, dict)
            }
            for incoming_item in season.get("download_items") or []:
                if not isinstance(incoming_item, dict):
                    continue
                key = tv_item_key(incoming_item)
                base_item = existing_items.get(key) or {}
                item_copy = _json_clone(incoming_item)
                if base_item.get("label"):
                    item_copy["label"] = base_item["label"]
                if not item_copy.get("episode_range") and base_item.get("episode_range"):
                    item_copy["episode_range"] = base_item.get("episode_range")
                merged_resolutions = {}
                for quality, entries in (incoming_item.get("resolutions") or {}).items():
                    normalized_entries = []
                    base_entries = (base_item.get("resolutions") or {}).get(quality, [])
                    for entry in entries if isinstance(entries, list) else []:
                        if not isinstance(entry, dict):
                            continue
                        hydrated = _snapshot_entry_with_metadata(entry, base_entries)
                        if is_drive_link(_entry_link(hydrated)):
                            normalized_entries.append(hydrated)
                    if normalized_entries:
                        merged_resolutions[quality] = normalized_entries
                if merged_resolutions:
                    item_copy["resolutions"] = merged_resolutions
                    item_copy.pop("download_filenames", None)
                    target_season["download_items"].append(item_copy)
            if target_season["download_items"]:
                authoritative_seasons.append(target_season)
        base["seasons"] = authoritative_seasons

    return normalize_result_download_languages(base)


def build_site_sync_snapshot(
    content_type: str,
    data: dict,
    *,
    website_title: str = "",
    site_content_id: int | None = None,
) -> dict:
    result = _published_site_view(data or {})
    payload = {
        "version": 1,
        "content_type": content_type,
        "website_title": str(website_title or "").strip(),
        "data": {},
    }
    if site_content_id is not None:
        payload["site_content_id"] = int(site_content_id)
    if result.get("title"):
        payload["title"] = result.get("title")
    if result.get("year") is not None:
        payload["year"] = result.get("year")
    if content_type == "movie":
        payload["data"]["download_links"] = result.get("download_links") or {}
    else:
        payload["data"]["seasons"] = result.get("seasons") or []
    return payload


def save_site_sync_snapshot(
    media_task: MediaTask,
    content_type: str,
    data: dict,
    *,
    website_title: str = "",
    site_content_id: int | None = None,
) -> dict:
    snapshot = build_site_sync_snapshot(
        content_type,
        data,
        website_title=website_title,
        site_content_id=site_content_id,
    )
    media_task.site_sync_snapshot = snapshot
    media_task.save(update_fields=["site_sync_snapshot", "updated_at"])
    return snapshot


def save_publish_state_with_snapshot(
    media_task: MediaTask,
    content_type: str,
    data: dict,
    *,
    website_title: str = "",
    site_content_id: int | None = None,
) -> dict:
    """
    Atomically persist the post-publish local state so `result`, `site_sync_snapshot`,
    `website_title`, and `site_content_id` never diverge mid-save.
    """
    snapshot = build_site_sync_snapshot(
        content_type,
        data,
        website_title=website_title,
        site_content_id=site_content_id,
    )
    result_copy = _json_clone(data)
    update_fields = ["result", "site_sync_snapshot", "updated_at"]

    with transaction.atomic():
        media_task.result = result_copy
        media_task.site_sync_snapshot = snapshot
        media_task.website_title = str(website_title or "").strip()
        update_fields.append("website_title")
        if site_content_id is not None:
            media_task.site_content_id = int(site_content_id)
            update_fields.append("site_content_id")
        media_task.save(update_fields=update_fields)

    return snapshot


def refresh_site_sync_snapshot_from_api(media_task: MediaTask, content_type: str) -> dict:
    site_content_id = getattr(media_task, "site_content_id", None)
    base = extract_site_sync_snapshot_result(getattr(media_task, "site_sync_snapshot", None))
    if not base and isinstance(media_task.result, dict):
        base = _json_clone(media_task.result)
    if not site_content_id:
        return _published_site_view(base)
    try:
        from upload.service import flixbd_client as fx

        if content_type == "movie":
            live = {"download_links": fx.fetch_movie_drive_links_by_quality(int(site_content_id))}
        else:
            live = {"seasons": fx.fetch_series_drive_links_tree(int(site_content_id))}
        merged = overlay_site_sync_snapshot(base, live, content_type)
        snapshot = build_site_sync_snapshot(
            content_type,
            merged,
            website_title=getattr(media_task, "website_title", ""),
            site_content_id=site_content_id,
        )
        media_task.site_sync_snapshot = snapshot
        media_task.save(update_fields=["site_sync_snapshot", "updated_at"])
        return merged
    except Exception as e:
        logger.warning("%s snapshot refresh id=%s: %s", SITE_NAME, site_content_id, e)
    fallback = _published_site_view(base)
    snapshot = build_site_sync_snapshot(
        content_type,
        fallback,
        website_title=getattr(media_task, "website_title", ""),
        site_content_id=site_content_id,
    )
    media_task.site_sync_snapshot = snapshot
    media_task.save(update_fields=["site_sync_snapshot", "updated_at"])
    return fallback


def normalize_flixbd_resolution_keys(qualities: list) -> list[str]:
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


def result_strip_non_drive_download_links(data: dict) -> dict:
    """For skip-without-upload rows: do not persist generate.php / host links as if final."""
    if not data:
        return data
    out = dict(data)
    dl = out.get("download_links")
    if isinstance(dl, dict):
        out["download_links"] = {
            k: [entry for entry in (entries if isinstance(entries, list) else []) if is_drive_link(_entry_link(entry))]
            for k, entries in dl.items()
        }
        out["download_links"] = {k: v for k, v in out["download_links"].items() if v}
    out.pop("download_filenames", None)
    return out


def fetch_flixbd_results(name: str, min_score: int = 40) -> list:
    """
    Search FlixBD for existing content by name.
    Returns at most _FLIXBD_LLM_MAX_RESULTS items (score >= min_score), sorted best-first.
    Never raises — returns [] on any error or if FlixBD is not configured.
    """
    try:
        from upload.service import flixbd_client as fx
        import httpx
        from rapidfuzz import fuzz

        api_url, api_key = fx._get_config()
        endpoint = f"{api_url}/api/v1/search"
        params = {"q": name, "type": "all", "per_page": _FLIXBD_LLM_MAX_RESULTS, "page": 1}

        with httpx.Client(timeout=fx._TIMEOUT) as client:
            resp = client.get(endpoint, params=params, headers=fx._headers(api_key))

        if resp.status_code != 200:
            logger.debug("FlixBD search: HTTP %s for %r", resp.status_code, name)
            return []

        try:
            payload = resp.json()
        except ValueError:
            snippet = (resp.text or "")[:300].replace("\n", " ")
            logger.warning(
                "FlixBD search: invalid JSON for %r (HTTP %s): %r",
                name,
                resp.status_code,
                snippet,
            )
            return []

        raw_results = payload.get("data", [])
        if not raw_results:
            logger.info("FlixBD search: no results for %r", name)
            return []

        year_re = re.compile(r"\b(19|20)\d{2}\b")
        name_lower = name.lower().strip()

        results = []
        for item in raw_results:
            item_title = item.get("title", "")
            year_match = year_re.search(item_title)
            clean = item_title[:year_match.start()].strip() if year_match else item_title
            score = fuzz.ratio(name_lower, clean.lower())
            if score < min_score:
                continue
            fid = item.get("id")
            if fid is None:
                logger.debug("FlixBD search: skipping hit without id: %r", item_title[:80])
                continue
            download_links = item.get("download_links") or {}
            qualities_raw = download_links.get("qualities")
            qualities = []
            if isinstance(qualities_raw, str):
                qualities = [q.strip() for q in qualities_raw.split(",") if q.strip()]
            elif isinstance(qualities_raw, list):
                qualities = [str(q).strip() for q in qualities_raw if str(q).strip()]

            # Slim payload for LLM + duplicate_context_json: avoid duplicate strings (qualities vs
            # resolution_keys vs download_links) — saves prompt tokens; rules use resolution_keys + title.
            results.append(
                {
                    "id": fid,
                    "title": item_title,
                    "match_score": score,
                    "resolution_keys": normalize_flixbd_resolution_keys(qualities),
                }
            )

        results.sort(key=lambda x: x["match_score"], reverse=True)
        results = results[:_FLIXBD_LLM_MAX_RESULTS]
        top = results[0]["match_score"] if results else 0
        logger.info(
            "FlixBD search: %s result(s) (score>=%s, max=%s) for %r (top=%s)",
            len(results),
            min_score,
            _FLIXBD_LLM_MAX_RESULTS,
            name,
            top,
        )
        return results

    except RuntimeError as e:
        logger.debug("FlixBD search skipped: %s", e)
        return []
    except Exception as e:
        logger.warning("FlixBD search error for %r: %s", name, e)
        return []


def merge_new_episodes(existing_result: dict, new_data: dict) -> dict:
    """
    Merge new TV show episodes from new_data INTO existing_result.
    """
    existing_seasons = existing_result.get("seasons", [])
    new_seasons = new_data.get("seasons", [])

    if not existing_seasons:
        return new_data

    if not new_seasons:
        logger.warning(
            "Episode merge: new_data has no seasons, preserving existing result to avoid data loss"
        )
        result = dict(existing_result)
        result.update({k: v for k, v in new_data.items() if k not in ("seasons",)})
        result["seasons"] = existing_seasons
        return result

    merged_seasons = {s["season_number"]: dict(s) for s in existing_seasons}
    for season in merged_seasons.values():
        season["download_items"] = list(season.get("download_items", []))

    for new_season in new_seasons:
        snum = new_season.get("season_number")
        new_items = new_season.get("download_items", [])

        if snum not in merged_seasons:
            merged_seasons[snum] = dict(new_season)
            logger.info("Episode merge: added new season %s", snum)
            continue

        existing_keys = {
            tv_item_key(item) for item in merged_seasons[snum]["download_items"]
        }
        added = []
        for new_item in new_items:
            key = tv_item_key(new_item)
            if key not in existing_keys:
                merged_seasons[snum]["download_items"].append(new_item)
                existing_keys.add(key)
                added.append(new_item.get("label", ""))

        if added:
            logger.info(
                "Episode merge: appended %s new episode(s) to S%s: %s",
                len(added),
                snum,
                added,
            )
        else:
            logger.info("Episode merge: no new episodes to add for S%s", snum)

    result = dict(existing_result)
    for key, value in new_data.items():
        if key == "seasons":
            continue
        current = result.get(key)
        if current in (None, "", [], {}):
            if value not in (None, "", [], {}):
                result[key] = value
        elif key == "total_seasons":
            try:
                result[key] = max(int(current), int(value))
            except (TypeError, ValueError):
                pass
    result["seasons"] = sorted(merged_seasons.values(), key=lambda s: s["season_number"])

    old_ss = existing_result.get("screen_shots_url")
    if isinstance(old_ss, list) and old_ss:
        cur = result.get("screen_shots_url")
        if not isinstance(cur, list) or not cur:
            result["screen_shots_url"] = list(old_ss)

    return result


def merge_drive_links(old_result: dict, new_data: dict) -> dict:
    """
    Merge existing Drive links from old_result into new_data.
    """
    old_dl = old_result.get("download_links", {})
    new_dl = new_data.get("download_links", {})
    if old_dl and new_dl:
        for res, old_entries in old_dl.items():
            if res not in new_dl:
                continue
            existing_by_file = {
                (_entry_language_key(entry), _entry_filename(entry)): entry
                for entry in (old_entries if isinstance(old_entries, list) else [])
                if is_drive_link(_entry_link(entry))
            }
            merged_entries = []
            for cur in new_dl.get(res) or []:
                old_entry = existing_by_file.get((_entry_language_key(cur), _entry_filename(cur)))
                if old_entry:
                    merged_entries.append(_entry_copy(cur, link=_entry_link(old_entry)))
                    logger.debug("Preserved existing drive link for %s [%s] %s", res, cur.get("l"), _entry_filename(cur))
                else:
                    merged_entries.append(cur)
            new_dl[res] = merged_entries
        new_data["download_links"] = new_dl

    old_seasons = {s.get("season_number"): s for s in old_result.get("seasons", [])}
    for new_season in new_data.get("seasons", []):
        snum = new_season.get("season_number")
        old_season = old_seasons.get(snum)
        if not old_season:
            continue

        old_items = {}
        for item in old_season.get("download_items", []):
            key = tv_item_key(item)
            old_items[key] = item.get("resolutions", {})

        for new_item in new_season.get("download_items", []):
            label = new_item.get("label", "")
            key = tv_item_key(new_item)
            old_res = old_items.get(key, {})
            new_res = new_item.get("resolutions", {})

            for res, old_entries in old_res.items():
                if res not in new_res:
                    continue
                existing_by_file = {
                    (_entry_language_key(entry), _entry_filename(entry)): entry
                    for entry in (old_entries if isinstance(old_entries, list) else [])
                    if is_drive_link(_entry_link(entry))
                }
                merged_entries = []
                for cur in new_res.get(res) or []:
                    old_entry = existing_by_file.get((_entry_language_key(cur), _entry_filename(cur)))
                    if old_entry:
                        merged_entries.append(_entry_copy(cur, link=_entry_link(old_entry)))
                        logger.debug(
                            "Preserved existing drive link for S%s %s %s [%s] %s",
                            snum,
                            label,
                            res,
                            cur.get("l"),
                            _entry_filename(cur),
                        )
                    else:
                        merged_entries.append(cur)
                new_res[res] = merged_entries

            new_item["resolutions"] = new_res
            new_item.pop("download_filenames", None)

    old_ss = old_result.get("screen_shots_url")
    if isinstance(old_ss, list) and old_ss:
        cur = new_data.get("screen_shots_url")
        if not isinstance(cur, list) or not cur:
            new_data["screen_shots_url"] = list(old_ss)

    return new_data


def has_drive_links(result: dict) -> bool:
    """Check if a result dict actually contains any Google Drive upload links."""
    if not result:
        return False
    for entries in result.get("download_links", {}).values():
        for entry in entries if isinstance(entries, list) else []:
            if is_drive_link(_entry_link(entry)):
                return True
    for season in result.get("seasons", []):
        for item in season.get("download_items", []):
            for entries in item.get("resolutions", {}).values():
                for entry in entries if isinstance(entries, list) else []:
                    if is_drive_link(_entry_link(entry)):
                        return True
    return False


def clean_result_keep_drive_links(result: dict) -> dict:
    """Strip resolutions without Drive links from a failed task result."""
    if not result:
        return result

    cleaned = dict(result)

    if "download_links" in cleaned:
        cleaned["download_links"] = {
            k: [entry for entry in (entries if isinstance(entries, list) else []) if is_drive_link(_entry_link(entry))]
            for k, entries in cleaned["download_links"].items()
        }
        cleaned["download_links"] = {k: v for k, v in cleaned["download_links"].items() if v}
        cleaned.pop("download_filenames", None)

    for season in cleaned.get("seasons", []):
        items_to_keep = []
        for item in season.get("download_items", []):
            res = item.get("resolutions", {})
            cleaned_res = {
                k: [entry for entry in (entries if isinstance(entries, list) else []) if is_drive_link(_entry_link(entry))]
                for k, entries in res.items()
            }
            cleaned_res = {k: v for k, v in cleaned_res.items() if v}
            if cleaned_res:
                item["resolutions"] = cleaned_res
                item.pop("download_filenames", None)
                items_to_keep.append(item)
        season["download_items"] = items_to_keep

    return cleaned


def build_db_candidate(task: MediaTask) -> dict:
    """Build a single candidate dict (with PK) for the LLM duplicate prompt."""
    result_data = task.result or {}
    existing_resolutions = _get_existing_resolutions(task)
    is_tvshow = task.content_type == "tvshow" if task.content_type else bool(
        result_data.get("seasons")
    )
    website_title = (
        result_data.get("website_movie_title")
        or result_data.get("website_tvshow_title")
        or task.website_title
        or ""
    )

    candidate = {
        "id": task.pk,
        "title": task.title,
        "website_title": website_title,
        "year": result_data.get("year"),
        "resolutions": existing_resolutions,
        "type": "tvshow" if is_tvshow else "movie",
    }

    if is_tvshow:
        episodes = []
        tv_items = []
        for season in result_data.get("seasons", []):
            season_num = season.get("season_number")
            for item in season.get("download_items", []):
                label = item.get("label", "")
                item_type = item.get("type")
                episode_range = item.get("episode_range")
                res = item.get("resolutions", {})
                ep_res = sorted(
                    {
                        str(k).strip().lower()
                        for k, entries in res.items()
                        if any(_entry_link(entry) for entry in (entries if isinstance(entries, list) else []))
                    }
                )
                episodes.append(
                    f"S{season_num} {item_type} {episode_range or '-'} {label}: {','.join(ep_res)}"
                )
                tv_items.append(
                    {
                        "season_number": season_num,
                        "type": item_type,
                        "episode_range": episode_range,
                        "label": label,
                        "resolutions": ep_res,
                    }
                )
        candidate["episode_count"] = len(tv_items)
        candidate["episodes"] = episodes
        candidate["tv_items"] = tv_items

    return candidate


def build_db_match_candidates(matches: list[MediaTask]) -> list[dict]:
    """Build a list of candidate dicts for the LLM duplicate prompt."""
    return [build_db_candidate(task) for task in matches]


def flixbd_site_id_set(flixbd_results: list | None) -> set[int]:
    """Numeric FlixBD content ids from search results (not MediaTask pks)."""
    out: set[int] = set()
    for result in flixbd_results or []:
        fid = result.get("id")
        if fid is None:
            continue
        try:
            out.add(int(fid))
        except (TypeError, ValueError):
            pass
    return out


def normalize_duplicate_response(
    dup_result: dict | None,
    db_candidate_map: dict,
    flixbd_results: list,
    media_task_pk: int,
) -> None:
    """Canonicalize duplicate_check keys; promote site id wrongly placed in matched_task_id."""
    if not dup_result or not isinstance(dup_result, dict):
        return
    legacy_id = dup_result.get(LEGACY_SITE_ROW_ID_JSON_KEY)
    current_id = dup_result.get(TARGET_SITE_ROW_ID_JSON_KEY)
    if current_id is None and legacy_id is not None:
        dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = legacy_id
    dup_result.pop(LEGACY_SITE_ROW_ID_JSON_KEY, None)
    if TARGET_SITE_ROW_ID_JSON_KEY not in dup_result:
        dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = None

    flix_ids = flixbd_site_id_set(flixbd_results)
    matched_task_id = coerce_matched_task_pk(dup_result.get("matched_task_id"))
    target_site_id = coerce_target_site_row_id(dup_result.get(TARGET_SITE_ROW_ID_JSON_KEY))

    if matched_task_id is not None and matched_task_id not in db_candidate_map:
        if matched_task_id in flix_ids and target_site_id is None:
            dup_result[TARGET_SITE_ROW_ID_JSON_KEY] = matched_task_id
            dup_result["matched_task_id"] = None
            logger.info(
                "Duplicate: promoted matched_task_id=%s to %s (namespace fix, task pk=%s)",
                matched_task_id,
                TARGET_SITE_ROW_ID_JSON_KEY,
                media_task_pk,
            )
        else:
            dup_result["matched_task_id"] = None
            logger.warning(
                "Duplicate: invalid matched_task_id=%s not in DB candidates %s (task pk=%s); cleared",
                matched_task_id,
                list(db_candidate_map.keys()),
                media_task_pk,
            )


def donor_result_for_site_content(
    site_content_id: int,
    exclude_pk: int | None,
    content_type: str,
) -> dict:
    """Drive metadata from another MediaTask row or FlixBD API."""
    query = MediaTask.objects.filter(site_content_id=site_content_id, status="completed")
    if exclude_pk is not None:
        query = query.exclude(pk=exclude_pk)
    donor = query.order_by("-updated_at").first()
    if donor:
        snapshot_data = extract_site_sync_snapshot_result(getattr(donor, "site_sync_snapshot", None))
        donor_result = dict(donor.result) if isinstance(donor.result, dict) and donor.result else {}
        combined = overlay_site_sync_snapshot(donor_result, snapshot_data, content_type)
        if combined:
            logger.info(
                "Donor MediaTask pk=%s for %s site_content_id=%s (merge result + snapshot)",
                donor.pk,
                SITE_NAME,
                site_content_id,
            )
            return combined
    if donor and isinstance(donor.result, dict) and donor.result:
        logger.info(
            "Donor MediaTask pk=%s for %s site_content_id=%s (merge drive links)",
            donor.pk,
            SITE_NAME,
            site_content_id,
        )
        return dict(donor.result)
    try:
        from upload.service import flixbd_client as fx

        if content_type != "tvshow":
            movie_links = fx.fetch_movie_drive_links_by_quality(int(site_content_id))
            if movie_links:
                logger.info(
                    "Hydrated %s drive link(s) from %s API for movie id=%s",
                    len(movie_links),
                    SITE_NAME,
                    site_content_id,
                )
                return {"download_links": movie_links}
        else:
            seasons = fx.fetch_series_drive_links_tree(int(site_content_id))
            if seasons:
                logger.info(
                    "Hydrated %s season block(s) from %s API for series id=%s",
                    len(seasons),
                    SITE_NAME,
                    site_content_id,
                )
                return {"seasons": seasons}
    except Exception as e:
        logger.warning("%s hydrate drive links id=%s: %s", SITE_NAME, site_content_id, e)
    return {}
