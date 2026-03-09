"""
Test script to see the exact LLM query and response
for the auto_up filtering pipeline.

Usage:
    python manage.py shell < auto_up/test.py
"""

import os
import sys
import json
import django

# Add project root to path so Django can find 'config' module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from auto_up.scraper import CineFreakScraper
from auto_up.db_search import search_existing
from auto_up.schema import AUTO_FILTER_SYSTEM_PROMPT
from llm.utils.name_extractor import extract_title_info
from llm.services import LLMService
from llm.json_repair import repair_json
from upload.models import MediaTask


DIVIDER = "=" * 70
THIN_DIVIDER = "-" * 70


def main():
    print(f"\n{DIVIDER}")
    print("  AUTO-UP LLM QUERY TEST")
    print(DIVIDER)

    # ── Step 1: Scrape homepage ──
    print("\n[1] Scraping CineFreak homepage...")
    entries = CineFreakScraper.scrape_homepage()

    if not entries:
        print("No entries found! Exiting.")
        return

    print(f"    Found {len(entries)} entries\n")

    for i, e in enumerate(entries, 1):
        print(f"    {i}. {e['raw_title'][:70]}")
        print(f"       {e['url']}")

    # ── Step 2: URL pre-filter ──
    print(f"\n{THIN_DIVIDER}")
    print("[2] Checking URLs against DB...")

    all_urls = [e["url"] for e in entries]
    existing_urls = set(
        MediaTask.objects.filter(url__in=all_urls).values_list("url", flat=True)
    )

    new_entries = []
    for e in entries:
        if e["url"] in existing_urls:
            print(f"    SKIP (URL exists): {e['raw_title'][:50]}")
        else:
            new_entries.append(e)

    print(f"    After URL filter: {len(new_entries)} remaining")

    if not new_entries:
        print("All URLs already in DB. Nothing to send to LLM.")
        return

    # ── Step 3: Extract names + search DB ──
    print(f"\n{THIN_DIVIDER}")
    print("[3] Extracting names + searching DB...\n")

    enriched_items = []
    for e in new_entries:
        info = extract_title_info(e["raw_title"])
        db_results = search_existing(info.title, info.year) if info.title else {"results": [], "has_matches": False}

        print(f"    Title: {e['raw_title'][:70]}")
        print(f"    Clean: {info.title}  |  Year: {info.year}  |  Season: {info.season_tag}")
        print(f"    DB matches: {len(db_results['results'])} unique result(s)")

        # Show rich info for each DB match
        for r in db_results['results']:
            print(f"      → DB[{r['task_pk']}] {r['title']} ({r['status']}) matched_by={r.get('matched_by', [])}")
            if r.get("website_title"):
                print(f"        website_title: {r['website_title'][:70]}")
            if r.get("resolutions"):
                print(f"        resolutions: {', '.join(r['resolutions'])}")
            if r.get("total_episodes"):
                print(f"        total_episodes: {r['total_episodes']}")
            if r.get("episodes"):
                # Show first 3 + last 1 to keep output manageable
                eps = r["episodes"]
                if len(eps) <= 5:
                    for ep in eps:
                        print(f"          {ep}")
                else:
                    for ep in eps[:3]:
                        print(f"          {ep}")
                    print(f"          ... ({len(eps) - 4} more)")
                    print(f"          {eps[-1]}")
            if r.get("season_numbers"):
                print(f"        seasons: {r['season_numbers']}")
        print()

        if info.title:
            enriched_items.append({
                "raw_title": e["raw_title"],
                "clean_name": info.title,
                "year": info.year,
                "season_tag": info.season_tag,
                "url": e["url"],
                "db_results": db_results,
            })

    if not enriched_items:
        print("No valid items after extraction.")
        return

    # ── Step 4: Build LLM payload ──
    print(f"\n{DIVIDER}")
    print("  LLM QUERY — SYSTEM PROMPT")
    print(DIVIDER)
    print(AUTO_FILTER_SYSTEM_PROMPT)

    # Build user prompt — using same logic as llm_filter.py
    from auto_up.llm_filter import _build_db_result_entry

    payload = []
    for item in enriched_items:
        db_results = item.get("db_results", {})
        payload.append({
            "raw_title": item["raw_title"],
            "clean_name": item["clean_name"],
            "year": item.get("year"),
            "season_tag": item.get("season_tag"),
            "url": item["url"],
            "db_results": {
                "results": [
                    _build_db_result_entry(r)
                    for r in db_results.get("results", [])
                ],
                "has_matches": db_results.get("has_matches", False),
            },
        })

    user_prompt = json.dumps(payload, ensure_ascii=False, indent=2)

    print(f"\n{DIVIDER}")
    print("  LLM QUERY — USER PROMPT (what gets sent)")
    print(DIVIDER)
    print(user_prompt)

    # ── Step 5: Call LLM ──
    print(f"\n{DIVIDER}")
    print("  CALLING LLM...")
    print(DIVIDER)

    try:
        raw_response = LLMService.generate_completion(
            prompt=user_prompt,
            system_prompt=AUTO_FILTER_SYSTEM_PROMPT,
        )

        print(f"\n{DIVIDER}")
        print("  LLM RAW RESPONSE")
        print(DIVIDER)
        print(raw_response)

        # Parse response
        parsed = repair_json(raw_response)

        print(f"\n{DIVIDER}")
        print("  LLM PARSED RESPONSE")
        print(DIVIDER)
        print(json.dumps(parsed, ensure_ascii=False, indent=2))

        # Summary
        decisions = parsed.get("decisions", [])
        process_count = sum(1 for d in decisions if d.get("action") == "process")
        skip_count = sum(1 for d in decisions if d.get("action") == "skip")

        print(f"\n{DIVIDER}")
        print("  SUMMARY")
        print(DIVIDER)
        print(f"  Total decisions: {len(decisions)}")
        print(f"  Process: {process_count}")
        print(f"  Skip:    {skip_count}")
        print()

        for d in decisions:
            icon = "✅" if d.get("action") == "process" else "❌"
            print(f"  {icon} [{d.get('action', '?').upper()}] {d.get('url', '?')[:60]}")
            print(f"     Reason: {d.get('reason', '?')}")
            print(f"     Priority: {d.get('priority', 'N/A')}")
            print()

    except Exception as e:
        print(f"\nLLM CALL FAILED: {e}")

    print(DIVIDER)
    print("  TEST COMPLETE (nothing was actually queued)")
    print(DIVIDER)


if __name__ == "__main__":
    main()
