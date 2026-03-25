"""
Scrape a CineFreak page (same path as upload pipeline) and write LLM-ready markdown to m.txt.

  uv run python test.py
  uv run python test.py "https://..."
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from upload.utils.web_scrape import WebScrapeService

DEFAULT_URL = (
    "https://www.cinefreak.net/single-papa-2025-season-1-hindi-netflix-web-series-"
    "download-watch-online-480p-720p-1080p-gdrive-esub-cinefreak/"
)
OUT_PATH = Path(__file__).resolve().parent / "m.txt"


def main() -> int:
    url = (sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL).strip()
    md = WebScrapeService.get_page_content(url)
    if not md:
        print("Scrape failed (no content).", file=sys.stderr)
        return 1
    OUT_PATH.write_text(md, encoding="utf-8")
    print(f"Wrote {len(md):,} chars to {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
