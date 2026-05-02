"""
LLM duplicate-check helpers: DB candidate rows, namespace fixes, donor results.
"""

from __future__ import annotations

import logging

from upload.models import MediaTask
from upload.service.duplicate_checker import (
    _get_existing_resolutions,
    coerce_matched_task_pk,
    coerce_target_site_row_id,
)
from llm.schema.blocked_names import SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY

from .entry_helpers import _entry_link
from .site_sync_snapshot import extract_site_sync_snapshot_result, overlay_site_sync_snapshot

logger = logging.getLogger(__name__)


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
        candidate["episode_count"] = len(episodes)
        candidate["episodes"] = episodes

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
