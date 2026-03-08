"""
LLM Multi-Provider Tester
=========================
Test LLM configs with fallback, different providers, and URL extraction.

Usage:
  python test.py                          # default URL, uses primary config
  python test.py --step detect            # only content type detection
  python test.py --step extract           # only data extraction
  python test.py --step all               # full pipeline
  python test.py --config 2               # force specific config ID
  python test.py --list                   # list all configs
"""

import os
import sys
import json
import argparse
import django
import logging

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from upload.utils.web_scrape import WebScrapeService
from llm.models import LLMConfig
from llm.services import LLMService, _try_one_config, _get_ordered_configs
from llm.json_repair import repair_json
from llm.schema import SYSTEM_PROMPT as MOVIE_PROMPT
from llm.tvshow_schema import TVSHOW_SYSTEM_PROMPT
from llm.content_type_detector import CONTENT_TYPE_DETECTION_PROMPT

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


def test_single_config(config_id, prompt, system_prompt):
    """Test a specific config directly."""
    config = LLMConfig.objects.get(pk=config_id)
    section(f"Testing config [{config.pk}] {config.name}")
    print(f"  SDK:   {C}{config.sdk}{X}")
    print(f"  Model: {C}{config.model_name}{X}")
    print(f"  Prompt: {len(prompt)} chars")

    try:
        content = _try_one_config(config, prompt, system_prompt)
        print(f"  {G}✅ Response: {len(content)} chars{X}")
        print(f"  {C}Preview: {content[:200]}...{X}")
        return content
    except Exception as e:
        print(f"  {R}❌ Failed: {e}{X}")
        return None


def test_fallback(prompt, system_prompt):
    """Test the full fallback chain."""
    section("Testing Fallback Chain")
    configs = _get_ordered_configs()
    print(f"  Order: {' → '.join(f'{c.name}({c.sdk})' for c in configs)}")

    try:
        content = LLMService.generate_completion(prompt, system_prompt)
        print(f"  {G}✅ Response: {len(content)} chars{X}")
        return content
    except Exception as e:
        print(f"  {R}❌ All configs failed: {e}{X}")
        return None


def step_detect(html_content, config_id=None):
    """Detect content type."""
    section("STEP 1: Content Type Detection")

    if config_id:
        raw = test_single_config(config_id, html_content, CONTENT_TYPE_DETECTION_PROMPT)
    else:
        raw = test_fallback(html_content, CONTENT_TYPE_DETECTION_PROMPT)

    if not raw:
        return None

    result = repair_json(raw)
    ct = result.get("content_type", "unknown")
    conf = result.get("confidence", 0)
    reason = result.get("reason", "N/A")
    print(f"\n  Type:       {B}{ct}{X}")
    print(f"  Confidence: {conf}")
    print(f"  Reason:     {reason}")
    return ct


def step_extract(html_content, content_type, config_id=None):
    """Extract structured data."""
    if content_type == "tvshow":
        section("STEP 2: TV Show Extraction")
        sys_prompt = TVSHOW_SYSTEM_PROMPT
    else:
        section("STEP 2: Movie Extraction")
        sys_prompt = MOVIE_PROMPT

    if config_id:
        raw = test_single_config(config_id, html_content, sys_prompt)
    else:
        raw = test_fallback(html_content, sys_prompt)

    if not raw:
        print(f"  {R}Empty response!{X}")
        return None

    result = repair_json(raw)

    # Summary
    if content_type == "tvshow":
        title = result.get("title", "?")
        seasons = result.get("seasons", [])
        items = sum(len(s.get("download_items", [])) for s in seasons)
        print(f"\n  Title:   {B}{title}{X}")
        print(f"  Seasons: {len(seasons)}  Items: {items}")
        for s in seasons:
            sn = s.get("season_number")
            for item in s.get("download_items", []):
                label = item.get("label", "?")
                res = list(item.get("resolutions", {}).keys())
                print(f"    S{sn} [{item.get('type','?')}] {label} → {res}")
    else:
        title = result.get("title", "?")
        links = list(result.get("download_links", {}).keys())
        print(f"\n  Title: {B}{title}{X}")
        print(f"  Links: {links}")

    # Full JSON
    print(f"\n{G}═══ Full Data ═══{X}")
    print(json.dumps(result, indent=2, ensure_ascii=False)[:3000])
    print(f"{G}{'═' * 50}{X}")
    return result


def main():
    parser = argparse.ArgumentParser(description="Test LLM Multi-Provider Pipeline")
    parser.add_argument('--url', type=str,
        default='https://www.cinefreak.net/bachelor-point-5-2025-season-5-bengali-web-series-download-watch-online-480p-720p-1080p-bongo-gdrive-esub-cinefreak-copy/')
    parser.add_argument('--config', type=int, default=None, help='Force specific config ID')
    parser.add_argument('--type', type=str, choices=['movie', 'tvshow'], default=None)
    parser.add_argument('--step', type=str, choices=['detect', 'extract', 'all'], default='all')
    parser.add_argument('--list', action='store_true', help='List all configs')
    parser.add_argument('--save', type=str, default=None, help='Save result to JSON file')
    args = parser.parse_args()

    if args.list:
        show_configs()
        return

    print(f"\n{B}{'═' * 55}{X}")
    print(f"{B}{C}  LLM Pipeline Tester (Multi-Provider){X}")
    print(f"{B}{'═' * 55}{X}")
    print(f"  URL:    {args.url[:65]}...")
    print(f"  Config: {args.config or 'auto (primary → fallback)'}")
    print(f"  Step:   {args.step}")

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

    # Detect
    if args.type:
        content_type = args.type
        print(f"\n  {Y}Forced type: {content_type}{X}")
    else:
        content_type = step_detect(html, args.config)
        if not content_type:
            sys.exit(1)

    if args.step == 'detect':
        print(f"\n{G}Done!{X}")
        return

    # Extract
    data = step_extract(html, content_type, args.config)
    if not data:
        sys.exit(1)

    if args.save:
        with open(args.save, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"\n  Saved to: {args.save}")

    print(f"\n{B}{G}✅ Done!{X}\n")


if __name__ == '__main__':
    main()
