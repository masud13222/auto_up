"""
LLM Multi-Provider Tester
=========================
Test LLM configs with fallback and combined detect+extract pipeline.

Usage:
  python scripts/llm_check_response.py                          # default URL
  python scripts/llm_check_response.py --config 2               # force specific config ID
  python scripts/llm_check_response.py --list                   # list all configs
  python scripts/llm_check_response.py --save result.json       # save output to file
"""

import os
import sys
import json
import argparse
import django
import logging

# Add project root to path so Django can find 'config' module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from upload.utils.web_scrape import WebScrapeService
from llm.models import LLMConfig
from llm.services import LLMService, _try_one_config, _get_ordered_configs
from llm.json_repair import repair_json
from llm.schema import COMBINED_SYSTEM_PROMPT

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(name)s: %(message)s')

# Colors
G = '\033[92m'; Y = '\033[93m'; C = '\033[96m'; R = '\033[91m'; B = '\033[1m'; X = '\033[0m'


def section(text):
    print(f"\n{B}{Y}▶ {text}{X}")
    print(f"{Y}{'─' * 55}{X}")


def show_configs():
    """List all LLM configs."""
    configs = LLMConfig.objects.all().order_by('-is_primary', 'pk')
    if not configs:
        print(f"{R}No LLM configs found! Add one in admin panel.{X}")
        return

    print(f"\n{B}{'═' * 55}{X}")
    print(f"{B}{C}  LLM Configs{X}")
    print(f"{B}{'═' * 55}{X}")
    for c in configs:
        primary = f" {Y}★ PRIMARY{X}" if c.is_primary else ""
        active = f" {R}(disabled){X}" if not c.is_active else ""
        print(f"  [{c.pk}] {B}{c.name}{X}{primary}{active}")
        print(f"       SDK: {c.sdk}  Model: {C}{c.model_name}{X}")
        if c.base_url:
            print(f"       URL: {c.base_url[:60]}")
    print(f"{'═' * 55}")


def call_llm(prompt, system_prompt, config_id=None):
    """Call LLM — either specific config or fallback chain."""
    if config_id:
        config = LLMConfig.objects.get(pk=config_id)
        print(f"  Config: [{config.pk}] {config.name} ({config.sdk}:{config.model_name})")
        return _try_one_config(config, prompt, system_prompt)
    else:
        return LLMService.generate_completion(prompt, system_prompt)


def main():
    parser = argparse.ArgumentParser(description="Test LLM Multi-Provider Pipeline")
    parser.add_argument('--url', type=str,
        default='https://www.cinefreak.net/sa-re-ga-ma-pa-legends-2025-season-22-zee5-bengali-reality-show-download-watch-online-480p-720p-1080p-gdrive-esub-cinefreak/')
    parser.add_argument('--config', type=int, default=None, help='Force specific config ID')
    parser.add_argument('--list', action='store_true', help='List all configs')
    parser.add_argument('--save', type=str, default=None, help='Save result to JSON file')
    args = parser.parse_args()

    if args.list:
        show_configs()
        return

    print(f"\n{B}{'═' * 55}{X}")
    print(f"{B}{C}  LLM Pipeline Tester (Combined Detect + Extract){X}")
    print(f"{B}{'═' * 55}{X}")
    print(f"  URL:    {args.url[:65]}...")
    print(f"  Config: {args.config or 'auto (primary → fallback)'}")

    # Show configs order
    try:
        configs = _get_ordered_configs()
        print(f"  Chain:  {' → '.join(f'{c.name}' for c in configs)}")
    except Exception as e:
        print(f"  {R}No active configs: {e}{X}")
        return
    print(f"{'═' * 55}")

    # Fetch HTML
    section("Fetching page content...")
    html = WebScrapeService.get_page_content(args.url)
    if not html:
        print(f"{R}ERROR: Could not fetch page!{X}")
        sys.exit(1)
    print(f"  Content: {len(html)} chars")

    # Combined detect + extract (1 LLM call)
    section("Combined Detect + Extract (1 API call)...")
    try:
        raw = call_llm(html, COMBINED_SYSTEM_PROMPT, args.config)
    except Exception as e:
        print(f"  {R}❌ LLM call failed: {e}{X}")
        sys.exit(1)

    if not raw or not raw.strip():
        print(f"  {R}❌ Empty response!{X}")
        sys.exit(1)

    print(f"  {G}✅ Response: {len(raw)} chars{X}")

    result = repair_json(raw)
    content_type = result.get("content_type", "unknown")
    data = result.get("data", {})

    print(f"\n  Type:  {B}{content_type}{X}")
    title = data.get("title", "?")
    print(f"  Title: {B}{title}{X}")

    # Show details
    if content_type == "tvshow":
        seasons = data.get("seasons", [])
        items = sum(len(s.get("download_items", [])) for s in seasons)
        print(f"  Seasons: {len(seasons)}  Items: {items}")
        for s in seasons:
            sn = s.get("season_number")
            for item in s.get("download_items", []):
                label = item.get("label", "?")
                res = list(item.get("resolutions", {}).keys())
                print(f"    S{sn} [{item.get('type','?')}] {label} → {res}")
    else:
        links = list(data.get("download_links", {}).keys())
        print(f"  Links: {links}")
        dfn = data.get("download_filenames") or {}
        if isinstance(dfn, dict) and dfn:
            print(f"  download_filenames keys: {list(dfn.keys())}")

    # Full JSON
    print(f"\n{G}═══ Full Data ═══{X}")
    print(json.dumps(result, indent=2, ensure_ascii=False)[:4000])
    print(f"{G}{'═' * 50}{X}")

    if args.save:
        with open(args.save, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"\n  Saved to: {args.save}")

    print(f"\n{B}{G}✅ Done! (1 API call instead of 2){X}\n")


if __name__ == '__main__':
    main()
