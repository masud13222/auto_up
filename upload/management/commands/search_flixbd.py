"""
Run FlixBD title search using the same path as upload duplicate-check / LLM context.

Uses ``fetch_flixbd_results`` (FlixBD search API; slim rows for the LLM).

Usage:
    python manage.py search_flixbd "The Boys"
"""

import json

from django.core.management.base import BaseCommand

from upload.tasks.runtime_helpers import fetch_flixbd_results


class Command(BaseCommand):
    help = "Search FlixBD by title (same payload shape as fetch_flixbd_results)."

    def add_arguments(self, parser):
        parser.add_argument(
            "query",
            type=str,
            help="Search query (e.g. movie or series title)",
        )

    def handle(self, *args, **options):
        query = (options["query"] or "").strip()
        if not query:
            self.stderr.write(self.style.ERROR("Empty query."))
            return

        results = fetch_flixbd_results(query)

        if not results:
            self.stdout.write(
                self.style.WARNING(
                    "No results (API error, disabled settings, or empty API response)."
                )
            )
            return

        self.stdout.write(
            json.dumps(results, indent=2, ensure_ascii=False)
        )
        self.stdout.write(
            self.style.SUCCESS(f"\n{len(results)} hit(s) for query={query!r}")
        )
