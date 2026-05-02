"""
Panel-only URL debug: scrape → presearch → DB/FlixBD search context → combined LLM extract.
Does not run Drive link resolution, site upload, or movie/tvshow pipelines.
Does not persist LLMUsage rows or duplicate-check snapshots on LLMUsage (persist_usage=False).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from typing import Any

from upload.service.duplicate_checker import _search_db
from upload.service.info import detect_and_extract
from upload.tasks.runtime_helpers import build_db_match_candidates, fetch_flixbd_results
from upload.utils.web_scrape import WebScrapeService, normalize_http_url
from llm.utils.presearch_extract import PRESEARCH_MARKDOWN_MAX, extract_presearch_from_markdown

logger = logging.getLogger(__name__)

_JSON = {"indent": 2, "ensure_ascii": False}

PIPELINE_STEPS = (
    "page_scrape",
    "presearch_llm",
    "db_search",
    "flixbd_search",
    "combined_llm",
)


def _json_default(o: Any) -> str:
    return str(o)


def _rollup_capture_usage(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "calls": 0}
    p = sum(int(e.get("prompt_tokens") or 0) for e in events)
    ctot = sum(int(e.get("completion_tokens") or 0) for e in events)
    t_lines = sum(int(e.get("total_tokens") or 0) for e in events)
    total = t_lines if t_lines > 0 else p + ctot
    return {"prompt_tokens": p, "completion_tokens": ctot, "total_tokens": total, "calls": len(events)}


def iter_panel_url_extract_events(url: str) -> Iterator[dict[str, Any]]:
    """
    Yield progressive events for the debug pipeline.

    Final event type is ``complete`` (ok) or ``error``:
    ``{"type":"complete","report": { ... }}`` report is formatted for templates/API.
    """
    raw = (url or "").strip()
    if not raw:
        yield {
            "type": "error",
            "report": format_report_for_display(
                {"ok": False, "error": "empty_url", "message": "URL is required."}
            ),
        }
        return

    normalized = normalize_http_url(raw)
    report: dict[str, Any] = {
        "ok": True,
        "url_input": raw,
        "url_normalized": normalized,
        "steps": [],
    }
    capture_presearch: list[dict[str, Any]] = []
    capture_combined_llm: list[dict[str, Any]] = []

    yield {
        "type": "meta",
        "url_input": raw,
        "url_normalized": normalized,
        "total_steps": len(PIPELINE_STEPS),
        "steps_order": list(PIPELINE_STEPS),
    }

    yield {
        "type": "step_begin",
        "index": 1,
        "total": len(PIPELINE_STEPS),
        "step": "page_scrape",
    }
    try:
        page_md = WebScrapeService.get_page_content(normalized)
    except Exception as exc:
        logger.exception("panel extract debug: scrape failed")
        report["ok"] = False
        report["error"] = "scrape_exception"
        report["message"] = str(exc)
        yield {"type": "error", "report": format_report_for_display(report)}
        return

    if not page_md or not str(page_md).strip():
        report["ok"] = False
        report["error"] = "empty_page"
        report["message"] = "No markdown returned from page scrape."
        yield {"type": "error", "report": format_report_for_display(report)}
        return

    step_scrape = {
        "step": "page_scrape",
        "markdown_total_chars": len(page_md),
        "presearch_snippet_chars": min(len(page_md), PRESEARCH_MARKDOWN_MAX),
        "markdown": page_md,
    }
    report["steps"].append(step_scrape)
    yield {"type": "step_end", "index": 1, "step": "page_scrape", "data": step_scrape}

    yield {
        "type": "step_begin",
        "index": 2,
        "total": len(PIPELINE_STEPS),
        "step": "presearch_llm",
    }
    presearch_llm_debug: dict = {}
    try:
        pre = extract_presearch_from_markdown(
            page_md[:PRESEARCH_MARKDOWN_MAX],
            persist_usage=False,
            debug_capture=presearch_llm_debug,
            capture_usage_events=capture_presearch,
        )
    except Exception as exc:
        logger.exception("panel extract debug: presearch failed")
        report["ok"] = False
        report["error"] = "presearch_failed"
        report["message"] = str(exc)
        yield {"type": "error", "report": format_report_for_display(report)}
        return

    step_pre = {
        "step": "presearch_llm",
        "content_type": pre.content_type,
        "primary_name": pre.primary_name,
        "alt_name": pre.alt_name,
        "year": pre.year,
        "season_tag": pre.season_tag,
        "llm_raw_response": presearch_llm_debug.get("raw_response", ""),
        "llm_system_prompt": presearch_llm_debug.get("system_prompt", ""),
        "llm_user_prompt": presearch_llm_debug.get("user_prompt", ""),
        "token_usage": _rollup_capture_usage(capture_presearch),
    }
    report["steps"].append(step_pre)
    yield {"type": "step_end", "index": 2, "step": "presearch_llm", "data": step_pre}

    yield {
        "type": "step_begin",
        "index": 3,
        "total": len(PIPELINE_STEPS),
        "step": "db_search",
    }
    db_search_debug: dict = {}
    matches = _search_db(
        pre.primary_name,
        pre.year,
        season_tag=pre.season_tag,
        exclude_pk=None,
        search_debug=db_search_debug,
        alt_name=pre.alt_name,
    )
    db_match_candidates = build_db_match_candidates(matches) if matches else None

    step_db = {
        "step": "db_search",
        "match_count": len(matches),
        "debug": db_search_debug,
        "candidate_preview": [{"pk": t.pk, "title": t.title} for t in (matches or [])[:15]],
    }
    report["steps"].append(step_db)
    yield {"type": "step_end", "index": 3, "step": "db_search", "data": step_db}

    yield {
        "type": "step_begin",
        "index": 4,
        "total": len(PIPELINE_STEPS),
        "step": "flixbd_search",
    }
    flixbd_search_debug: dict = {}
    flixbd_results = fetch_flixbd_results(
        pre.primary_name,
        year=pre.year,
        season_tag=pre.season_tag,
        alt_name=pre.alt_name,
        fetch_debug=flixbd_search_debug,
    )

    step_flix = {
        "step": "flixbd_search",
        "result_count": len(flixbd_results or []),
        "results": list(flixbd_results or []),
        "debug": flixbd_search_debug,
    }
    report["steps"].append(step_flix)
    yield {"type": "step_end", "index": 4, "step": "flixbd_search", "data": step_flix}

    search_query_json = {
        "extract": {
            "source": "markdown_presearch",
            "markdown_chars": len(page_md),
            "snippet_chars": min(len(page_md), PRESEARCH_MARKDOWN_MAX),
            "content_type": pre.content_type,
            "name": pre.primary_name,
            "alt_name": pre.alt_name,
            "year": pre.year,
            "season_tag": pre.season_tag,
        },
        "db_search": db_search_debug,
        "flixbd_search": flixbd_search_debug,
    }

    yield {
        "type": "step_begin",
        "index": 5,
        "total": len(PIPELINE_STEPS),
        "step": "combined_llm",
    }
    debug_capture: dict = {}
    try:
        content_type, data, dup_result = detect_and_extract(
            page_md,
            locked_content_type=pre.content_type,
            db_match_candidates=db_match_candidates,
            flixbd_results=flixbd_results,
            search_query_json=search_query_json,
            debug_capture=debug_capture,
            persist_usage=False,
            capture_usage_events=capture_combined_llm,
        )
    except Exception as exc:
        logger.exception("panel extract debug: combined LLM failed")
        report["ok"] = False
        report["error"] = "combined_llm_failed"
        report["message"] = str(exc)
        report["search_query_json"] = search_query_json
        report["partial_debug_capture"] = debug_capture
        yield {"type": "error", "report": format_report_for_display(report)}
        return

    _evs = debug_capture.get("llm_events") or []
    step_llm = {
        "step": "combined_llm",
        "status": "ok",
        "llm_system_prompt": debug_capture.get("system_prompt") or "",
        "llm_user_prompt": page_md,
        "llm_final_response_text": debug_capture.get("final_response_text") or "",
        "llm_events": [dict(e) for e in _evs],
        "token_usage": _rollup_capture_usage(capture_combined_llm),
    }
    report["steps"].append(step_llm)

    merged_calls = [*capture_presearch, *capture_combined_llm]
    report["token_usage_summary"] = {
        "by_phase": {
            "presearch_llm": _rollup_capture_usage(capture_presearch),
            "combined_llm": _rollup_capture_usage(capture_combined_llm),
        },
        "grand": _rollup_capture_usage(merged_calls),
        "calls_detail": merged_calls,
    }
    yield {"type": "step_end", "index": 5, "step": "combined_llm", "data": step_llm}

    report["search_query_json"] = search_query_json
    report["debug_capture"] = debug_capture
    report["user_prompt_text"] = page_md
    report["result"] = {
        "content_type": content_type,
        "data": data,
        "duplicate_check": dup_result,
    }
    report["result_json"] = json.dumps(
        {"content_type": content_type, "data": data, "duplicate_check": dup_result},
        **_JSON,
        default=_json_default,
    )

    yield {
        "type": "complete",
        "report": format_report_for_display(report),
    }


def run_panel_url_extract_debug(url: str) -> dict[str, Any]:
    """
    Run the same search + combined LLM path as ``process_media_task`` up to and including
    ``detect_and_extract`` (resolution policy + pass-2 included; no link resolution / upload).
    """
    for ev in iter_panel_url_extract_events(url):
        if ev["type"] == "complete":
            return ev["report"]
        if ev["type"] == "error":
            return ev["report"]
    return {
        "ok": False,
        "error": "no_result",
        "message": "Pipeline produced no outcome.",
    }


def format_report_for_display(report: dict) -> dict:
    """Add JSON string helpers for templates (no logic change)."""
    out = dict(report)
    sq = report.get("search_query_json")
    if sq is not None:
        out["search_query_json_text"] = json.dumps(sq, **_JSON, default=str)
    dc = report.get("debug_capture") or {}
    sp = dc.get("system_prompt")
    if sp is not None:
        out["system_prompt_text"] = sp
    if dc:
        out["combined_request_meta_json"] = json.dumps(
            {
                "purpose": dc.get("purpose"),
                "locked_content_type": dc.get("locked_content_type"),
                "user_prompt_char_length": dc.get("user_prompt_char_length"),
                "system_prompt_char_length": len(str(sp)) if sp is not None else 0,
                "resolution_settings": dc.get("resolution_settings"),
            },
            **_JSON,
            default=str,
        )
    tus = report.get("token_usage_summary")
    if tus is not None:
        out["token_usage_summary_compact_json"] = json.dumps(
            {
                "grand": tus.get("grand"),
                "by_phase": tus.get("by_phase"),
            },
            **_JSON,
            default=str,
        )
    partial = report.get("partial_debug_capture")
    if partial:
        out["partial_debug_capture_json"] = json.dumps(partial, **_JSON, default=str)
    return out
