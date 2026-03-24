"""
Auto-upload tasks for Django-Q.

Main entry point: auto_scrape_and_queue()
- Scrapes CineFreak homepage
- Extracts clean names from titles
- Enforces daily limit (max 2 process per URL per day)
- Searches DB for existing entries (fuzzy matching)
- Sends everything to LLM for filtering
- Queues approved items via process_media_task
- Logs everything to ScrapeRun + ScrapeItem models
- Auto-cleans logs older than 7 days
"""

import logging
from datetime import timedelta

from django.utils import timezone
from django_q.tasks import async_task

from auto_up.scraper import CineFreakScraper
from auto_up.db_search import search_existing
from auto_up.llm_filter import filter_items_with_llm
from auto_up.models import ScrapeRun, ScrapeItem
from llm.utils.name_extractor import extract_title_info
from upload.models import MediaTask

logger = logging.getLogger(__name__)

# Maximum times the same URL can be sent to process in a single day
DAILY_PROCESS_LIMIT = 2

# Days to keep scrape logs
LOG_RETENTION_DAYS = 7


def _fetch_flixbd_top(name: str, max_results: int = 2) -> list:
    """
    Search FlixBD for existing content by name.
    Returns top `max_results` hits scored with rapidfuzz, sorted best first.
    Returns [] if FlixBD is not configured, disabled, or no results found.

    Used by auto_up to pass site context to LLM for smarter filtering.
    """
    try:
        from upload.service import flixbd_client as fx
        import httpx
        import re
        from rapidfuzz import fuzz

        fx._get_config()  # Raises RuntimeError if not configured/disabled

        api_url, api_key = fx._get_config()
        params = {"q": name, "type": "all", "per_page": 5, "page": 1}

        with httpx.Client(timeout=fx._TIMEOUT) as client:
            resp = client.get(
                f"{api_url}/api/v1/search",
                params=params,
                headers=fx._headers(api_key),
            )

        if resp.status_code != 200:
            return []

        raw = resp.json().get("data", [])
        if not raw:
            return []

        _year_re = re.compile(r'\b(19|20)\d{2}\b')
        name_lower = name.lower().strip()

        scored = []
        for item in raw:
            item_title = item.get("title", "")
            year_match = _year_re.search(item_title)
            clean = item_title[:year_match.start()].strip() if year_match else item_title
            score = fuzz.ratio(name_lower, clean.lower())
            scored.append({"id": item["id"], "title": item_title, "match_score": score})

        # Sort best first, return top N
        scored.sort(key=lambda x: x["match_score"], reverse=True)
        top = scored[:max_results]

        if top:
            logger.debug(
                f"FlixBD auto_up search '{name}': top {len(top)} results "
                f"(scores: {[r['match_score'] for r in top]})"
            )
        return top

    except RuntimeError:
        return []
    except Exception as e:
        logger.warning(f"FlixBD auto_up search error for '{name}': {e}")
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
    1. Cleanup old logs (>7 days)
    2. Scrape CineFreak homepage
    3. Daily limit check: max 2 process per URL per day
    4. For each remaining entry: extract clean name + year
    5. Search DB for existing matches (fuzzy matching)
    6. Send all items + DB results to LLM for filtering
    7. Queue approved items for processing
    8. Log everything to ScrapeRun + ScrapeItem

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

        for entry in entries:
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

        # ── Step 2b: Skip URLs currently pending/processing in MediaTask DB ──
        # This prevents wasting LLM tokens on URLs already queued/processing
        url_filtered = []
        url_exists_skipped = 0

        for entry in daily_filtered:
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

            # Extract clean name + year
            title_info = extract_title_info(raw_title)

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
            db_results = search_existing(title_info.title, title_info.year)

            # Search FlixBD (top 2 per title — enough context for LLM, limits API load)
            flixbd_results = _fetch_flixbd_top(title_info.title, max_results=2)

            enriched_items.append({
                "raw_title": raw_title,
                "clean_name": title_info.title,
                "year": title_info.year,
                "season_tag": title_info.season_tag,
                "url": url,
                "db_results": db_results,
                "flixbd_results": flixbd_results,
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
        queued_count = 0

        for item in to_process:
            url = item["url"]
            raw_title = item.get("raw_title", url[:50])
            priority = item.get("priority", "normal")

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

            # Queue for processing
            q_task_id = async_task(
                "upload.tasks.process_media_task",
                media_task.pk,
                task_name=f"Auto: {raw_title[:50]}",
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
        f"daily_limit_skip={scrape_run.daily_limit_skipped}, "
        f"llm_approved={scrape_run.llm_approved}, "
        f"llm_skip={scrape_run.llm_skipped}, "
        f"queued={scrape_run.queued}"
    )
    logger.info(summary)
    logger.info("=" * 60)

    return summary
