"""
Contract tests: prompt JSON schemas must be well-formed and accept minimal valid payloads.

Catches schema drift (e.g. typos in required keys) before runtime.
"""

from __future__ import annotations

from typing import Any

from django.test import SimpleTestCase
from jsonschema import validators

from llm.schema.blocked_names import SITE_NAME, TARGET_SITE_ROW_ID_JSON_KEY
from llm.schema.duplicate_schema import duplicate_schema
from llm.schema.json_schema_validate import assert_schemas_well_formed, validate_combined_extract
from llm.schema.movie_schema import movie_schema
from llm.schema.tvshow_schema import tvshow_schema


def _assert_validates(schema: dict[str, Any], instance: dict[str, Any]) -> None:
    validator_cls = validators.validator_for(schema)
    validator_cls(schema).validate(instance)


def _minimal_movie_data() -> dict:
    return {
        "website_movie_title": f"Test Movie - {SITE_NAME}",
        "title": "Test Movie",
        "year": 2024,
        "is_adult": False,
        "download_links": {
            "720p": [{"u": "https://example.com/f.mkv", "l": "English", "f": "Test.2024.720p.mkv"}],
            "4k": [{"u": "https://example.com/4k.mkv", "l": "English", "f": "Test.2024.4k.mkv"}],
        },
    }


def _minimal_tv_data() -> dict:
    return {
        "website_tvshow_title": f"Test Show - {SITE_NAME}",
        "title": "Test Show",
        "year": 2024,
        "is_adult": False,
        "seasons": [
            {
                "season_number": 1,
                "download_items": [
                    {
                        "type": "single_episode",
                        "label": "EP01",
                        "episode_range": "01",
                        "resolutions": {
                            "1080p": [
                                {
                                    "u": "https://example.com/s01e01.mkv",
                                    "l": "English",
                                    "f": "Show.S01E01.1080p.mkv",
                                }
                            ]
                        },
                    }
                ],
            }
        ],
    }


def _minimal_duplicate_check() -> dict:
    return {
        "is_duplicate": False,
        "matched_task_id": None,
        TARGET_SITE_ROW_ID_JSON_KEY: None,
        "action": "process",
        "reason": "Contract test minimal payload.",
        "detected_new_type": "movie",
        "missing_resolutions": [],
        "has_new_episodes": False,
        "updated_website_title": False,
    }


class SchemaContractTests(SimpleTestCase):
    def test_meta_schemas_are_valid(self) -> None:
        assert_schemas_well_formed()

    def test_movie_schema_accepts_minimal(self) -> None:
        _assert_validates(movie_schema, _minimal_movie_data())

    def test_tvshow_schema_accepts_minimal(self) -> None:
        _assert_validates(tvshow_schema, _minimal_tv_data())

    def test_duplicate_schema_accepts_minimal(self) -> None:
        _assert_validates(duplicate_schema, _minimal_duplicate_check())

    def test_validate_combined_extract_movie_no_dup(self) -> None:
        root = {
            "content_type": "movie",
            "data": _minimal_movie_data(),
        }
        out = validate_combined_extract(
            root,
            locked_content_type="movie",
            require_duplicate_check=False,
        )
        self.assertEqual(out["content_type"], "movie")
        self.assertIn("download_links", out["data"])

    def test_validate_combined_extract_requires_duplicate_when_flag(self) -> None:
        root = {
            "content_type": "movie",
            "data": _minimal_movie_data(),
        }
        with self.assertRaises(ValueError):
            validate_combined_extract(
                root,
                locked_content_type="movie",
                require_duplicate_check=True,
            )

    def test_validate_combined_extract_with_duplicate(self) -> None:
        root = {
            "content_type": "tvshow",
            "data": _minimal_tv_data(),
            "duplicate_check": _minimal_duplicate_check(),
        }
        out = validate_combined_extract(
            root,
            locked_content_type="tvshow",
            require_duplicate_check=True,
        )
        self.assertIn("duplicate_check", out)
