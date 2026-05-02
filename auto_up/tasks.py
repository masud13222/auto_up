"""
Auto-upload tasks for Django-Q.

Main entry point: auto_scrape_and_queue()
- Scrapes CineFreak homepage
- Extracts clean names from titles
- Enforces daily limit (max 2 process per URL per day), except during the
  Bangladesh bypass window (see _daily_limit_bypass_window_active)
- Searches DB for existing entries (fuzzy matching)
- Sends everything to LLM for filtering
- Queues approved items via process_media_task
- Logs everything to ScrapeRun + ScrapeItem models
- Skips URLs listed in ``AutoUpSkipUrl`` (manual blocklist) before LLM and again before queue
- Auto-cleans logs older than LOG_RETENTION_DAYS (see below)
"""

import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

from django.utils import timezone
from django_q.tasks import async_task

from auto_up.scraper import CineFreakScraper
from auto_up.db_search import search_existing
from auto_up.llm_filter import filter_items_with_llm
from auto_up.models import AutoUpSkipUrl, ScrapeItem, ScrapeRun
from auto_up.url_match import canonical_skip_url
from constant import (
    AUTO_UP_FLIXBD_LLM_MAX_RESULTS,
    FLIXBD_FUZZY_THRESHOLD,
    FLIXBD_SEARCH_PER_PAGE,
)
from llm.utils.name_extractor import extract_title_info
from llm.utils.presearch_extract import PRESEARCH_MARKDOWN_MAX, extract_presearch_from_markdown
from upload.models import MediaTask
from upload.utils.web_scrape import WebScrapeService

logger = logging.getLogger(__name__)


# Maximum times the same URL can be sent to process in a single day
DAILY_PROCESS_LIMIT = 2

# Outside this window (Asia/Dhaka local clock), the daily cap applies.
# Inside: 22:00–23:59 and 00:00–00:59 (i.e. "10 PM–1 AM" with 01:00 excluded).
_DAILY_LIMIT_BYPASS_TZ = ZoneInfo("Asia/Dhaka")
_DAILY_LIMIT_BYPASS_START_HOUR = 22
_DAILY_LIMIT_BYPASS_END_HOUR = 1

# Days to keep ScrapeRun / ScrapeItem logs in admin
LOG_RETENTION_DAYS = 3


def _daily_limit_bypass_window_active(at=None) -> bool:
    """
    True when auto-scrape should not enforce DAILY_PROCESS_LIMIT per URL.

    Fixed window in Bangladesh time (Asia/Dhaka): 22:00 through 00:59 inclusive
    (01:00 local is outside the window; cap applies again from 01:00 onward).
    """
    if at is None:
        at = timezone.now()
    if timezone.is_naive(at):
        at = timezone.make_aware(at, timezone.utc)
    local = at.astimezone(_DAILY_LIMIT_BYPASS_TZ)
    h = local.hour
    return h >= _DAILY_LIMIT_BYPASS_START_HOUR or h < _DAILY_LIMIT_BYPASS_END_HOUR


def _fetch_flixbd_top(
    name: str,
    year: str | None = None,
    season_tag: str | None = None,
    alt_name: str | None = None,
    max_results: int = None,
    fetch_debug: dict | None = None,
) -> list:
    """
    FlixBD: prioritized phases (name, name+year, name+year+season when present), merged by ``id``, fuzzy on titles,
    then up to ``max_results`` rows (same shape as before for auto_up LLM).
    """
    from upload.service import flixbd_client as fx
    from upload.tasks.runtime_helpers import (
        _flixbd_merge_two_phase_raw,
        _flixbd_title_fuzzy_score,
    )

    if max_results is None:
        max_results = AUTO_UP_FLIXBD_LLM_MAX_RESULTS
    max_results = min(int(max_results), AUTO_UP_FLIXBD_LLM_MAX_RESULTS)
    if fetch_debug is not None:
        fetch_debug.clear()
        fetch_debug["name"] = (name or "").strip()
        fetch_debug["alt_name"] = (alt_name or "").strip() or None
        fetch_debug["year"] = str(year).strip() if year is not None and str(year).strip() else None
        fetch_debug["season_tag"] = (season_tag or "").strip() or None
        fetch_debug["llm_max_flixbd_rows"] = max_results

    if not (name or "").strip():
        return []

    try:
        fx._get_config()
        api_url, api_key = fx._get_config()

        raw_merged, queries_run, _ = _flixbd_merge_two_phase_raw(
            name,
            year,
            season_tag=season_tag,
            alt_name=alt_name,
            per_page=FLIXBD_SEARCH_PER_PAGE,
            api_url=api_url,
            api_key=api_key,
        )
        if not raw_merged:
            if fetch_debug is not None:
                fetch_debug["queries"] = list(queries_run)
                fetch_debug["merged_raw_count"] = 0
            return []

        scored: list[tuple[int, dict]] = []
        for item in raw_merged:
            fid = item.get("id")
            if fid is None:
                continue
            item_title = item.get("title", "") or ""
            fs = _flixbd_title_fuzzy_score(
                name, year, item_title, season_tag=season_tag, alt_name=alt_name
            )
            if fs < FLIXBD_FUZZY_THRESHOLD:
                continue
            download_links = item.get("download_links") or {}
            entry: dict = {
                "id": fid,
                "title": item_title,
                "download_links": download_links,
            }
            rd = item.get("release_date")
            if rd is not None and rd != "":
                entry["release_date"] = rd
            scored.append((fs, entry))

        scored.sort(key=lambda x: x[0], reverse=True)
        top = [e for _, e in scored[:max_results]]
        if fetch_debug is not None:
            fetch_debug["queries"] = list(queries_run)
            fetch_debug["merged_raw_count"] = len(raw_merged)
            fetch_debug["after_fuzzy_count"] = len(top)

        if top:
            logger.debug(
                "FlixBD auto_up search name=%r queries=%s: %s result(s) after fuzzy",
                name,
                queries_run,
                len(top),
            )
        return top

    except RuntimeError:
        return []
    except Exception as e:
        logger.warning("FlixBD auto_up search error for %r: %s", name, e)
        return []


def _cleanup_old_logs():
    """Delete ScrapeRun (and cascading ScrapeItem) older than LOG_RETENTION_DAYS."""
    cutoff = timezone.now() - timedelta(days=LOG_RETENTION_DAYS)
    deleted_count, _ = ScrapeRun.objects.filter(started_at__lt=cutoff).delete()
    if deleted_count:
        logger.info(f"Cleaned up {deleted_count} old scrape log records (>{LOG_RETENTION_DAYS} days)")


def _get_daily_process_count(url: str) -> int:
    """
    Count how many times this URL was sent to process TODAY.
    Checks ScrapeItem records with action='process' from today.
    """
    today_start = timezone.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return ScrapeItem.objects.filter(
        url=url,
        action='process',
        created_at__gte=today_start,
    ).count()


def auto_scrape_and_queue() -> str:
    """
    Full auto-scrape pipeline:
    1. Cleanup old logs (older than ``LOG_RETENTION_DAYS``)
    2. Scrape CineFreak homepage
    3. Daily limit check: max 2 process per URL per day (skipped 22:00–01:00 Asia/Dhaka)
    4. Skip URLs in the manual ``AutoUpSkipUrl`` list (canonical match)
    5. For each remaining entry: extract clean name + year
    6. Search DB for existing matches (fuzzy matching)
    7. Send all items + DB results to LLM for filtering
    8. Queue approved items (skip list re-checked); log to ScrapeRun + ScrapeItem

    Returns:
        Summary string of what happened.
    """
    run_start = timezone.now()

    # Create scrape run log
    scrape_run = ScrapeRun.objects.create(status='running')

    logger.info("=" * 60)
    logger.info(f"AUTO-SCRAPE started (run #{scrape_run.pk}) at {run_start.isoformat()}")
    logger.info("=" * 60)

    try:
        # ── Step 0: Cleanup old logs ──
        _cleanup_old_logs()

        # ── Step 1: Scrape homepage ──
        entries = CineFreakScraper.scrape_homepage()
        scrape_run.total_scraped = len(entries)

        if not entries:
            _finish_run(scrape_run, run_start, "No entries found on homepage")
            return scrape_run.error_message or "No entries found"

        logger.info(f"Found {len(entries)} entries on homepage")

        # ── Step 2: Daily limit check ──
        daily_filtered = []
        daily_limit_skipped = 0
        limit_bypass = _daily_limit_bypass_window_active(run_start)
        if limit_bypass:
            logger.info(
                "Daily per-URL cap bypassed (Asia/Dhaka 22:00–01:00 window, run time %s)",
                run_start.isoformat(),
            )

        for entry in entries:
            if limit_bypass:
                daily_filtered.append(entry)
                continue
            count = _get_daily_process_count(entry["url"])
            if count >= DAILY_PROCESS_LIMIT:
                daily_limit_skipped += 1
                ScrapeItem.objects.create(
                    run=scrape_run,
                    raw_title=entry["raw_title"],
                    url=entry["url"],
                    action='skip_daily_limit',
                    reason=f'Already processed {count}x today (limit={DAILY_PROCESS_LIMIT})',
                )
                logger.debug(f"Daily limit reached for: {entry['raw_title'][:50]}")
            else:
                daily_filtered.append(entry)

        scrape_run.daily_limit_skipped = daily_limit_skipped

        if daily_limit_skipped:
            logger.info(f"Skipped {daily_limit_skipped} entries (daily limit of {DAILY_PROCESS_LIMIT} reached)")

        if not daily_filtered:
            _finish_run(scrape_run, run_start, "All entries hit daily limit")
            return "All entries hit daily process limit."

        # ── Step 2a: Manual skip list (URLs never auto-queued) ──
        skip_normalized = frozenset(
            AutoUpSkipUrl.objects.exclude(normalized_url="").values_list("normalized_url", flat=True)
        )
        skip_list_filtered = []
        skip_list_skipped = 0

        for entry in daily_filtered:
            canon = canonical_skip_url(entry["url"])
            if canon and canon in skip_normalized:
                skip_list_skipped += 1
                ScrapeItem.objects.create(
                    run=scrape_run,
                    raw_title=entry["raw_title"],
                    url=entry["url"],
                    action="skip_skip_list",
                    reason="URL is in the manual auto-up skip list",
                )
                logger.debug("Skip list: %s", entry["raw_title"][:50])
                continue
            skip_list_filtered.append(entry)

        scrape_run.skip_list_skipped = skip_list_skipped

        if skip_list_skipped:
            logger.info("Skipped %s entries (manual auto-up skip list)", skip_list_skipped)

        if not skip_list_filtered:
            _finish_run(scrape_run, run_start, "All entries matched the manual skip list")
            return "All entries are in the manual skip list."

        # ── Step 2b: Skip URLs currently pending/processing in MediaTask DB ──
        # This prevents wasting LLM tokens on URLs already queued/processing
        url_filtered = []
        url_exists_skipped = 0

        for entry in skip_list_filtered:
            # Check both primary url and extra_urls for this entry
            url_val = entry["url"]
            already_queued = (
                MediaTask.objects.filter(url=url_val, status__in=['pending', 'processing']).exists()
                or MediaTask.objects.filter(
                    extra_urls__contains=url_val, status__in=['pending', 'processing']
                ).exists()
            )
            if already_queued:
                url_exists_skipped += 1
                ScrapeItem.objects.create(
                    run=scrape_run,
                    raw_title=entry["raw_title"],
                    url=url_val,
                    action='skip_url_exists',
                    reason='URL already queued or processing',
                )
                continue
            url_filtered.append(entry)

        scrape_run.url_skipped = url_exists_skipped

        if url_exists_skipped:
            logger.info(f"Skipped {url_exists_skipped} entries (URL already in DB)")

        if not url_filtered:
            _finish_run(scrape_run, run_start, "All entries already in DB")
            return "All entries already in DB."

        # ── Step 3: Extract names + search DB + FlixBD ──
        enriched_items = []

        for entry in url_filtered:
            raw_title = entry["raw_title"]
            url = entry["url"]

            alt_name = None
            title_info = None
            page_md = WebScrapeService.get_page_content(url)
            if page_md and page_md.strip():
                try:
                    pre = extract_presearch_from_markdown(page_md[:PRESEARCH_MARKDOWN_MAX])
                    title_info = pre.as_title_info()
                    alt_name = pre.alt_name
                except Exception as exc:
                    logger.warning(
                        "auto_up presearch failed url=%s: %s",
                        url,
                        exc,
                    )

            if title_info is None or not title_info.title:
                title_info = extract_title_info(raw_title)
                alt_name = None

            if not title_info.title:
                logger.warning(f"Could not extract title from: {raw_title}")
                ScrapeItem.objects.create(
                    run=scrape_run,
                    raw_title=raw_title,
                    url=url,
                    action='skip_no_title',
                    reason='Name extraction returned empty title',
                )
                continue

            # Search our DB
            db_results = search_existing(
                title_info.title,
                title_info.year,
                season_tag=title_info.season_tag,
                alt_name=alt_name,
            )

            flixbd_search_debug: dict = {}
            flixbd_results = _fetch_flixbd_top(
                title_info.title,
                year=title_info.year,
                season_tag=title_info.season_tag,
                alt_name=alt_name,
                fetch_debug=flixbd_search_debug,
            )

            enriched_items.append({
                "raw_title": raw_title,
                "clean_name": title_info.title,
                "year": title_info.year,
                "season_tag": title_info.season_tag,
                "url": url,
                "db_results": db_results,
                "flixbd_results": flixbd_results,
                "search_query_json": {
                    "extract": {
                        "raw_title": raw_title,
                        "name": title_info.title,
                        "alt_name": alt_name,
                        "year": title_info.year,
                        "season_tag": title_info.season_tag,
                    },
                    "db_search": (db_results or {}).get("search_debug", {}),
                    "flixbd_search": flixbd_search_debug,
                },
            })

        logger.info(f"Enriched {len(enriched_items)} items with DB + FlixBD search results")

        if not enriched_items:
            _finish_run(scrape_run, run_start, "No valid items after name extraction")
            return "No valid items to filter."

        # ── Step 4: LLM filtering ──
        to_process = filter_items_with_llm(enriched_items)

        # Log LLM skipped items
        process_urls = {item["url"] for item in to_process}
        llm_skipped = 0

        for item in enriched_items:
            if item["url"] not in process_urls:
                llm_skipped += 1
                ScrapeItem.objects.create(
                    run=scrape_run,
                    raw_title=item["raw_title"],
                    clean_name=item["clean_name"],
                    year=item.get("year", ""),
                    url=item["url"],
                    action='skip_llm',
                    reason='LLM decided to skip',
                )

        scrape_run.llm_approved = len(to_process)
        scrape_run.llm_skipped = llm_skipped

        if not to_process:
            _finish_run(scrape_run, run_start, "LLM decided all items should be skipped")
            return "LLM skipped everything."

        # ── Step 5: Queue approved items ──
        skip_normalized = frozenset(
            AutoUpSkipUrl.objects.exclude(normalized_url="").values_list("normalized_url", flat=True)
        )
        queued_count = 0

        for item in to_process:
            url = item["url"]
            raw_title = item.get("raw_title", url[:50])
            priority = item.get("priority", "normal")

            late_canon = canonical_skip_url(url)
            if late_canon and late_canon in skip_normalized:
                logger.info("Skip list (late check): not queueing %s", url[:80])
                ScrapeItem.objects.create(
                    run=scrape_run,
                    raw_title=raw_title,
                    url=url,
                    action="skip_skip_list",
                    reason="URL was added to the manual skip list before queueing",
                )
                continue

            # Race condition guard — only block if pending/processing
            # Check both primary url and extra_urls
            race_exists = (
                MediaTask.objects.filter(url=url, status__in=['pending', 'processing']).exists()
                or MediaTask.objects.filter(
                    extra_urls__contains=url, status__in=['pending', 'processing']
                ).exists()
            )
            if race_exists:
                logger.info(f"Race condition guard: URL already queued/processing, skipping: {url}")
                ScrapeItem.objects.create(
                    run=scrape_run,
                    raw_title=raw_title,
                    url=url,
                    action='skip_race',
                    reason='URL is currently pending/processing',
                )
                continue

            # Reuse existing completed/failed task, or create new
            # Also check extra_urls so we don't create a duplicate for a known source URL
            existing = (
                MediaTask.objects.filter(url=url, status__in=['completed', 'failed'])
                .order_by('-updated_at').first()
                or MediaTask.objects.filter(
                    extra_urls__contains=url, status__in=['completed', 'failed']
                ).order_by('-updated_at').first()
            )

            if existing:
                media_task = existing
                media_task.status = 'pending'
                media_task.error_message = ''
                media_task.save(update_fields=['status', 'error_message', 'updated_at'])
                logger.info(f"Reusing existing task (pk={media_task.pk}) for: {raw_title[:60]}")
            else:
                media_task = MediaTask.objects.create(url=url)

            # Queue at default ORM priority (LLM "priority" is still stored on ScrapeItem only)
            q_task_id = async_task(
                "upload.tasks.process_media_task",
                media_task.pk,
                task_name=f"Auto: {raw_title[:50]}",
                q_options={"q_priority": 0},
            )

            media_task.task_id = q_task_id or ""
            media_task.save(update_fields=["task_id", "updated_at"])

            # Log the item
            # Find clean_name from enriched_items
            clean_name = ""
            year = ""
            for enriched in enriched_items:
                if enriched["url"] == url:
                    clean_name = enriched.get("clean_name", "")
                    year = enriched.get("year", "") or ""
                    break

            ScrapeItem.objects.create(
                run=scrape_run,
                raw_title=raw_title,
                clean_name=clean_name,
                year=year,
                url=url,
                action='process',
                reason=item.get("reason", "LLM approved"),
                llm_priority=priority,
                media_task_pk=media_task.pk,
            )

            queued_count += 1
            logger.info(f"Queued [{priority}]: {raw_title[:60]} (pk={media_task.pk})")

        scrape_run.queued = queued_count

        # ── Finish ──
        summary = _finish_run(scrape_run, run_start)
        return summary

    except Exception as e:
        logger.error(f"Auto-scrape failed: {e}", exc_info=True)
        scrape_run.status = 'failed'
        scrape_run.error_message = str(e)
        scrape_run.finished_at = timezone.now()
        scrape_run.duration_seconds = (scrape_run.finished_at - run_start).total_seconds()
        scrape_run.save()
        return f"Auto-scrape failed: {e}"


def _finish_run(scrape_run: ScrapeRun, run_start, message: str = None) -> str:
    """Mark a scrape run as completed and generate summary."""
    scrape_run.status = 'completed'
    scrape_run.finished_at = timezone.now()
    scrape_run.duration_seconds = (scrape_run.finished_at - run_start).total_seconds()

    if message:
        scrape_run.error_message = message

    scrape_run.save()

    summary = (
        f"Auto-scrape #{scrape_run.pk} done in {scrape_run.duration_seconds:.1f}s: "
        f"scraped={scrape_run.total_scraped}, "
        f"skip_list_skip={scrape_run.skip_list_skipped}, "
        f"url_exists_skip={scrape_run.url_skipped}, "
        f"daily_limit_skip={scrape_run.daily_limit_skipped}, "
        f"llm_approved={scrape_run.llm_approved}, "
        f"llm_skip={scrape_run.llm_skipped}, "
        f"queued={scrape_run.queued}"
    )
    logger.info(summary)
    logger.info("=" * 60)

    return summary
