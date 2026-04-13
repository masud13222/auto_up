import json
import logging
import re

from constant import (
    FLIXBD_FUZZY_THRESHOLD,
    FLIXBD_LLM_MAX_RESULTS,
    FLIXBD_SEARCH_PER_PAGE,
)
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
    movie_download_entry_key,
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


def _normalize_flixbd_row_id(fid) -> int | str | None:
    """
    Canonical id for merge/dedupe so API ``201`` and ``\"201\"`` map to the same key.
    Prefer positive int; otherwise non-empty stripped string; else None.
    """
    if fid is None or isinstance(fid, bool):
        return None
    if isinstance(fid, int):
        return fid if fid > 0 else None
    if isinstance(fid, str):
        s = fid.strip()
        if not s:
            return None
        if s.isdigit():
            v = int(s)
            return v if v > 0 else None
        return s
    try:
        v = int(fid)
        return v if v > 0 else None
    except (TypeError, ValueError):
        s = str(fid).strip()
        return s or None


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


def _tv_download_item_has_any_drive_link(item: dict) -> bool:
    res = item.get("resolutions")
    if not isinstance(res, dict):
        return False
    for entries in res.values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict) and is_drive_link(_entry_link(entry)):
                return True
    return False


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


def strip_movie_download_entries_by_flixbd_failures(movie_data: dict, failed: list[dict]) -> None:
    """
    Remove movie ``download_links`` entries that failed FlixBD POST so ``result`` /
    ``site_sync_snapshot`` only list rows we believe were accepted (after retries).
    No API fetch — matches ``failed`` records from ``add_movie_download_links``.

    When ``failed`` items include ``link_id`` (same as the fourth element of
    ``movie_download_entry_key``), only that specific row is stripped so duplicate
    basenames are safe.
    """
    if not failed or not isinstance(movie_data, dict):
        return
    dl = movie_data.get("download_links")
    if not isinstance(dl, dict):
        return
    fail_by_link: set[tuple[str, str, str, str]] = set()
    fail_legacy_triple: set[tuple[str, str, str]] = set()
    for f in failed:
        if not isinstance(f, dict):
            continue
        q = str(f.get("quality") or "").strip().lower()
        lang = coerce_entry_language_value(f.get("language"))
        fn = str(f.get("filename") or "").strip()
        lid = f.get("link_id")
        if isinstance(lid, str) and lid.strip():
            fail_by_link.add((q, lang, fn, lid.strip()))
        else:
            fail_legacy_triple.add((q, lang, fn))

    for res_key in list(dl.keys()):
        entries = dl.get(res_key)
        if not isinstance(entries, list):
            continue
        rk = str(res_key or "").strip().lower()
        kept = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            lang = coerce_entry_language_value(entry.get("l"))
            fn = str(entry.get("f") or "").strip()
            eid = movie_download_entry_key(rk, entry)
            if eid in fail_by_link:
                continue
            if (rk, lang, fn) in fail_legacy_triple:
                continue
            kept.append(entry)
        if kept:
            dl[res_key] = kept
        else:
            del dl[res_key]


def strip_tvshow_download_entries_by_flixbd_failures(tvshow_data: dict, failed: list[dict]) -> None:
    """
    Remove TV resolution entries that failed FlixBD POST (same matching as movie helper).
    """
    if not failed or not isinstance(tvshow_data, dict):
        return
    fail_set = set()
    for f in failed:
        if not isinstance(f, dict):
            continue
        sn = f.get("season_number")
        try:
            sn = int(sn) if sn is not None else None
        except (TypeError, ValueError):
            sn = None
        label = str(f.get("label") or "").strip()
        q = str(f.get("quality") or "").strip().lower()
        lang = coerce_entry_language_value(f.get("language"))
        fn = str(f.get("filename") or "").strip()
        fail_set.add((sn, label, q, lang, fn))

    for season in tvshow_data.get("seasons") or []:
        if not isinstance(season, dict):
            continue
        snum = season.get("season_number")
        try:
            snum = int(snum) if snum is not None else None
        except (TypeError, ValueError):
            snum = None
        for item in season.get("download_items") or []:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "").strip()
            res = item.get("resolutions")
            if not isinstance(res, dict):
                continue
            for qual_key in list(res.keys()):
                qk = str(qual_key or "").strip().lower()
                entries = res.get(qual_key)
                if not isinstance(entries, list):
                    continue
                kept = []
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    lang = coerce_entry_language_value(entry.get("l"))
                    fn = str(entry.get("f") or "").strip()
                    if (snum, label, qk, lang, fn) in fail_set:
                        continue
                    kept.append(entry)
                if kept:
                    res[qual_key] = kept
                else:
                    del res[qual_key]


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
    update_site_sync_snapshot: bool = True,
) -> dict | None:
    """
    Atomically persist the post-publish local state so `result`, `website_title`, and
    `site_content_id` stay aligned. When ``update_site_sync_snapshot`` is False, the
    previous ``site_sync_snapshot`` row is left unchanged.
    """
    snapshot = None
    if update_site_sync_snapshot:
        snapshot = build_site_sync_snapshot(
            content_type,
            data,
            website_title=website_title,
            site_content_id=site_content_id,
        )
    result_copy = _json_clone(data)
    update_fields = ["result", "updated_at"]

    with transaction.atomic():
        media_task.result = result_copy
        if update_site_sync_snapshot and snapshot is not None:
            media_task.site_sync_snapshot = snapshot
            update_fields.append("site_sync_snapshot")
        media_task.website_title = str(website_title or "").strip()
        update_fields.append("website_title")
        if site_content_id is not None:
            media_task.site_content_id = int(site_content_id)
            update_fields.append("site_content_id")
        media_task.save(update_fields=update_fields)

    return snapshot


def hydrate_existing_result_from_snapshot(media_task: MediaTask, content_type: str) -> dict:
    """
    Build the current published view using only local MediaTask state.

    No live target-site/API fetch is performed here. The source of truth is the
    task's stored ``result`` plus ``site_sync_snapshot``.
    """
    site_content_id = getattr(media_task, "site_content_id", None)
    snapshot_data = extract_site_sync_snapshot_result(getattr(media_task, "site_sync_snapshot", None))
    base_result = _json_clone(media_task.result) if isinstance(media_task.result, dict) else {}
    merged = overlay_site_sync_snapshot(base_result, snapshot_data, content_type)
    hydrated = _published_site_view(merged or snapshot_data or base_result)
    snapshot = build_site_sync_snapshot(
        content_type,
        hydrated,
        website_title=getattr(media_task, "website_title", ""),
        site_content_id=site_content_id,
    )
    media_task.site_sync_snapshot = snapshot
    media_task.save(update_fields=["site_sync_snapshot", "updated_at"])
    return hydrated


def flixbd_slim_qualities_from_download_links(download_links: dict | None) -> list[str]:
    """
    Build string fragments for :func:`normalize_flixbd_resolution_keys`.

    Movies: ``download_links.qualities`` (comma-separated string or list).
    Series (FlixBD API v1): ``download_links.episodes_range`` (list of lines like
    ``S01: 1080p,480p,720p`` or ``S01 Episode 01-04: 1080p,480p,720p``).
    """
    dl = download_links if isinstance(download_links, dict) else {}
    qualities: list[str] = []
    qualities_raw = dl.get("qualities")
    if isinstance(qualities_raw, str):
        qualities = [q.strip() for q in qualities_raw.split(",") if q.strip()]
    elif isinstance(qualities_raw, list):
        qualities = [str(q).strip() for q in qualities_raw if str(q).strip()]
    if qualities:
        return qualities
    ep_raw = dl.get("episodes_range")
    if isinstance(ep_raw, list):
        return [str(x).strip() for x in ep_raw if str(x).strip()]
    if isinstance(ep_raw, str) and ep_raw.strip():
        return [ep_raw.strip()]
    return []


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

    seasons = out.get("seasons")
    if isinstance(seasons, list):
        kept_seasons = []
        for season in seasons:
            if not isinstance(season, dict):
                continue
            kept_items = []
            for item in season.get("download_items", []) if isinstance(season.get("download_items"), list) else []:
                if not isinstance(item, dict):
                    continue
                kept_resolutions = {}
                for quality, entries in (item.get("resolutions") or {}).items():
                    if not isinstance(entries, list):
                        continue
                    kept_entries = [entry for entry in entries if isinstance(entry, dict) and is_drive_link(_entry_link(entry))]
                    if kept_entries:
                        kept_resolutions[quality] = kept_entries
                if kept_resolutions:
                    item_copy = dict(item)
                    item_copy["resolutions"] = kept_resolutions
                    item_copy.pop("download_filenames", None)
                    kept_items.append(item_copy)
            if kept_items:
                season_copy = dict(season)
                season_copy["download_items"] = kept_items
                kept_seasons.append(season_copy)
        out["seasons"] = kept_seasons

    out.pop("download_filenames", None)
    return out


def flixbd_search_query(name: str, year: str | int | None = None) -> str:
    """
    Build FlixBD search API ``q`` string: cleaned title, plus year when present
    and not already redundant at the end of the title (e.g. avoid ``Bandi 2026 2026``).
    """
    n = (name or "").strip()
    if not n:
        return ""
    if year is None:
        return n
    ys = str(year).strip()
    if not ys:
        return n
    if n.endswith(ys):
        return n
    return f"{n} {ys}"


def _flixbd_title_fuzzy_score(name: str, year: str | int | None, title: str) -> int:
    """partial_ratio of API ``title`` vs extracted name and (when distinct) name+year query."""
    from rapidfuzz import fuzz

    t = (title or "").lower()
    if not t:
        return 0
    qn = (name or "").strip().lower()
    s = fuzz.partial_ratio(qn, t) if qn else 0
    ys = str(year).strip() if year is not None else ""
    if ys:
        qy = flixbd_search_query(name, year).strip().lower()
        if qy and qy != qn:
            s = max(s, fuzz.partial_ratio(qy, t))
    return int(s)


def _flixbd_merge_two_phase_raw(
    name: str,
    year: str | int | None,
    *,
    per_page: int,
    api_url: str,
    api_key: str,
) -> tuple[list[dict], list[str], int]:
    """
    Run FlixBD search twice when useful (name-only, then name+year), merge rows by ``id``.

    Returns ``(merged_items, queries_used, phases_without_payload_count)``.
    """
    from upload.service.flixbd_api_base import flixbd_search_response_dict

    merged: list[dict] = []
    seen: set[int | str] = set()
    queries_run: list[str] = []
    phases_no_payload = 0

    def _phase(q: str) -> None:
        nonlocal phases_no_payload
        q = (q or "").strip()
        if not q:
            return
        if queries_run and queries_run[-1] == q:
            return
        queries_run.append(q)
        body = flixbd_search_response_dict(
            api_url, api_key, {"q": q, "type": "all", "per_page": per_page, "page": 1}
        )
        if not body:
            phases_no_payload += 1
            return
        for item in body.get("data", []) or []:
            if not isinstance(item, dict):
                continue
            nk = _normalize_flixbd_row_id(item.get("id"))
            if nk is None:
                continue
            if nk in seen:
                continue
            seen.add(nk)
            row = dict(item)
            row["id"] = nk
            merged.append(row)

    q_name = (name or "").strip()
    _phase(q_name)
    q_year = flixbd_search_query(name, year).strip()
    if q_year and q_year != q_name:
        _phase(q_year)

    return merged, queries_run, phases_no_payload


def fetch_flixbd_results(name: str, *, year: str | int | None = None, fetch_debug: dict | None = None) -> list:
    """
    FlixBD: two API phases (``q`` = name only, then ``q`` = name+year when distinct), merged by ``id``,
    then fuzzy filter on titles (>= ``FLIXBD_FUZZY_THRESHOLD``), best scores first, capped at
    ``FLIXBD_LLM_MAX_RESULTS``. Slim rows for the LLM (no match_score field).

    If ``fetch_debug`` is a dict, it is cleared and filled with ``name``, ``year``, ``queries``,
    ``merged_raw_count``, ``after_fuzzy_count``, ``status``, optional ``message``.
    """
    def _mark(status: str, message: str | None = None) -> None:
        if fetch_debug is not None:
            fetch_debug["status"] = status
            if message:
                fetch_debug["message"] = message

    if fetch_debug is not None:
        fetch_debug.clear()
        fetch_debug["name"] = name
        fetch_debug["year"] = str(year).strip() if year is not None and str(year).strip() else None

    q_label = (name or "").strip()
    if not q_label:
        _mark("skipped", "empty search query")
        return []

    try:
        from upload.service import flixbd_client as fx

        api_url, api_key = fx._get_config()

        raw_merged, queries_run, phases_no_payload = _flixbd_merge_two_phase_raw(
            name,
            year,
            per_page=FLIXBD_SEARCH_PER_PAGE,
            api_url=api_url,
            api_key=api_key,
        )

        if fetch_debug is not None:
            fetch_debug["queries"] = list(queries_run)
            fetch_debug["per_phase_per_page"] = FLIXBD_SEARCH_PER_PAGE
            fetch_debug["merged_raw_count"] = len(raw_merged)
            fetch_debug["llm_max_flixbd_rows"] = FLIXBD_LLM_MAX_RESULTS
            fetch_debug["fuzzy_threshold"] = FLIXBD_FUZZY_THRESHOLD

        if not raw_merged:
            if queries_run and phases_no_payload >= len(queries_run):
                logger.debug("FlixBD search: no usable JSON for phases %s", queries_run)
                _mark("no_payload", "No usable JSON from search API")
            else:
                logger.info("FlixBD search: no merged rows for %r (queries=%s)", name, queries_run)
                _mark("empty", "API returned no rows for merged phases")
            return []

        scored: list[tuple[int, dict]] = []
        for item in raw_merged:
            fid = item.get("id")
            item_title = item.get("title", "") or ""
            if fid is None:
                logger.debug("FlixBD search: skipping hit without id: %r", item_title[:80])
                continue
            download_links = item.get("download_links") or {}

            row: dict = {
                "id": fid,
                "title": item_title,
            }
            # Pass through API download_links only (qualities / episodes_range). Omit derived
            # resolution_keys — same info is parseable from download_links; saves LLM tokens.
            if download_links:
                row["download_links"] = dict(download_links)
            rd = item.get("release_date")
            if rd is not None and rd != "":
                row["release_date"] = rd

            fs = _flixbd_title_fuzzy_score(name, year, item_title)
            if fs >= FLIXBD_FUZZY_THRESHOLD:
                scored.append((fs, row))

        scored.sort(key=lambda x: x[0], reverse=True)
        if fetch_debug is not None:
            fetch_debug["passed_fuzzy_count"] = len(scored)
        results = [r for _, r in scored[:FLIXBD_LLM_MAX_RESULTS]]

        if fetch_debug is not None:
            fetch_debug["after_fuzzy_count"] = len(results)

        if not results:
            if raw_merged:
                _mark("empty", "No merged row passed fuzzy threshold")
                logger.info(
                    "FlixBD search: 0 rows after fuzzy (threshold=%s) for %r; merged=%s",
                    FLIXBD_FUZZY_THRESHOLD,
                    name,
                    len(raw_merged),
                )
            else:
                _mark("parsed_empty", "Merged list empty after parse")
        else:
            _mark("ok")

        logger.info(
            "FlixBD search: %s result(s) (cap=%s, fuzzy>=%s) for %r queries=%s",
            len(results),
            FLIXBD_LLM_MAX_RESULTS,
            FLIXBD_FUZZY_THRESHOLD,
            name,
            queries_run,
        )
        return results

    except RuntimeError as e:
        logger.debug("FlixBD search skipped: %s", e)
        _mark("skipped", str(e))
        return []
    except Exception as e:
        logger.warning("FlixBD search error for %r: %s", name, e)
        _mark("error", str(e))
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
                carried = [
                    _json_clone(entry)
                    for entry in (old_entries if isinstance(old_entries, list) else [])
                    if is_drive_link(_entry_link(entry))
                ]
                if carried:
                    new_dl[res] = carried
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
    new_seasons = {s.get("season_number"): s for s in new_data.get("seasons", [])}

    for snum, old_season in old_seasons.items():
        if snum not in new_seasons:
            carried_items = []
            for item in old_season.get("download_items", []):
                if not isinstance(item, dict):
                    continue
                if _tv_download_item_has_any_drive_link(item):
                    carried_items.append(_json_clone(item))
            if carried_items:
                new_data.setdefault("seasons", []).append(
                    {"season_number": snum, "download_items": carried_items}
                )

    for new_season in new_data.get("seasons", []):
        snum = new_season.get("season_number")
        old_season = old_seasons.get(snum)
        if not old_season:
            continue

        old_items = {}
        old_items_full = {}
        for item in old_season.get("download_items", []):
            key = tv_item_key(item)
            old_items[key] = item.get("resolutions", {})
            old_items_full[key] = item

        new_item_keys = {
            tv_item_key(item)
            for item in new_season.get("download_items", [])
            if isinstance(item, dict)
        }
        for old_key, old_item in old_items_full.items():
            if old_key in new_item_keys:
                continue
            if _tv_download_item_has_any_drive_link(old_item):
                new_season.setdefault("download_items", []).append(_json_clone(old_item))

        for new_item in new_season.get("download_items", []):
            label = new_item.get("label", "")
            key = tv_item_key(new_item)
            old_res = old_items.get(key, {})
            new_res = new_item.get("resolutions", {})

            for res, old_entries in old_res.items():
                if res not in new_res:
                    carried = [
                        _json_clone(entry)
                        for entry in (old_entries if isinstance(old_entries, list) else [])
                        if is_drive_link(_entry_link(entry))
                    ]
                    if carried:
                        new_res[res] = carried
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

    if isinstance(new_data.get("seasons"), list):
        new_data["seasons"].sort(key=lambda s: s.get("season_number") or 0)

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
    is_tvshow = task.content_type == "tvshow" if task.content_type else bool(
        result_data.get("seasons")
    )
    existing_resolutions = (
        [] if is_tvshow else _get_existing_resolutions(task)
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
        "type": "tvshow" if is_tvshow else "movie",
    }
    if not is_tvshow:
        candidate["resolutions"] = existing_resolutions

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
    """Drive metadata from another completed MediaTask row only."""
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
    return {}
