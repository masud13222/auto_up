"""
Check download link resolution (cinecloud /f/ → R2 or video URLs).

  uv run python test.py
  uv run python test.py "https://..."

Uses WebScrapeService.get_url (same path as movie/tvshow pipelines).
"""
from __future__ import annotations

import json
import os
import sys

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from upload.utils.web_scrape import WebScrapeService

DEFAULT_URL = "https://new5.cinecloud.site/f/5eb2182c"


def main() -> int:
    raw = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    url = raw.strip()
    print(f"Resolving: {url!r}\n")

    result = WebScrapeService.get_url(url)

    if result is None:
        print("Result: None (no R2 / video links found)")
        return 1

    if isinstance(result, list):
        print(f"Result: {len(result)} URL(s)")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(f"Result: {result!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
