"""
Validate combined extract JSON against the same dicts used in LLM prompts (movie/tvshow/duplicate).

Runs light coercion (year, ids) then jsonschema validation against the same dicts embedded in prompts.
"""

from __future__ import annotations

import copy
from typing import Any, Literal

from jsonschema import Draft202012Validator, validators

from llm.schema.blocked_names import TARGET_SITE_ROW_ID_JSON_KEY
from llm.schema.duplicate_schema import duplicate_schema
from llm.schema.movie_schema import movie_schema
from llm.schema.tvshow_schema import tvshow_schema


def _coerce_int_from_json(v: Any, *, field_label: str = "value") -> int:
    """Coerce LLM output to int (year, season_number, etc.)."""
    if v is None or isinstance(v, bool):
        raise ValueError(f"{field_label} must be a number")
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            raise ValueError(f"{field_label} must not be empty")
        return int(s)
    if isinstance(v, float):
        return int(v)
    raise ValueError(f"{field_label} must be a number")


def _coerce_int_or_none(v: Any) -> int | None:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        raise ValueError("expected integer or null, not boolean")
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    raise ValueError("expected integer or null")


def _coerce_movie_data(d: dict[str, Any]) -> None:
    if "year" in d:
        d["year"] = _coerce_int_from_json(d["year"], field_label="year")


def _coerce_tv_data(d: dict[str, Any]) -> None:
    if "year" in d:
        d["year"] = _coerce_int_from_json(d["year"], field_label="year")
    seasons = d.get("seasons")
    if not isinstance(seasons, list):
        return
    for season in seasons:
        if not isinstance(season, dict):
            continue
        if "season_number" in season:
            season["season_number"] = _coerce_int_from_json(
                season["season_number"],
                field_label="season_number",
            )


def _coerce_duplicate_check(d: dict[str, Any]) -> None:
    if "matched_task_id" in d:
        d["matched_task_id"] = _coerce_int_or_none(d.get("matched_task_id"))
    tid_key = TARGET_SITE_ROW_ID_JSON_KEY
    if tid_key in d:
        d[tid_key] = _coerce_int_or_none(d.get(tid_key))
    uwt = d.get("updated_website_title")
    if uwt is True:
        raise ValueError("updated_website_title must be a string or false, not true")


def _validate(instance: Any, schema: dict[str, Any]) -> None:
    cls = validators.validator_for(schema)
    cls(schema).validate(instance)


def _strip_none(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if v is None:
                continue
            out[k] = _strip_none(v)
        return out
    if isinstance(obj, list):
        return [_strip_none(i) for i in obj]
    return obj


def validate_combined_extract(
    data: Any,
    *,
    locked_content_type: Literal["movie", "tvshow"],
    require_duplicate_check: bool,
) -> dict[str, Any]:
    """
    Validate repaired JSON for combined extract. Returns a plain dict for the rest of the pipeline.

    Raises ``jsonschema.exceptions.ValidationError`` or ``ValueError`` on failure.
    """
    if not isinstance(data, dict):
        raise ValueError("LLM response must be a JSON object")

    root = copy.deepcopy(data)

    ct = root.get("content_type")
    if ct != locked_content_type:
        raise ValueError(
            f"content_type must be {locked_content_type!r}, got {ct!r}"
        )

    inner = root.get("data")
    if not isinstance(inner, dict):
        raise ValueError("'data' must be a JSON object")

    if locked_content_type == "movie":
        _coerce_movie_data(inner)
        _validate(inner, movie_schema)
    else:
        _coerce_tv_data(inner)
        _validate(inner, tvshow_schema)

    dup = root.get("duplicate_check")
    if require_duplicate_check and dup is None:
        raise ValueError(
            "duplicate_check is required when DB/FlixBD candidates were sent with the prompt"
        )

    if dup is not None:
        if not isinstance(dup, dict):
            raise ValueError("duplicate_check must be a JSON object or omitted")
        _coerce_duplicate_check(dup)
        _validate(dup, duplicate_schema)

    return _strip_none(root)


def assert_schemas_well_formed() -> None:
    """Used by contract tests to catch invalid schema dicts early."""
    for name, schema in (
        ("movie_schema", movie_schema),
        ("tvshow_schema", tvshow_schema),
        ("duplicate_schema", duplicate_schema),
    ):
        try:
            Draft202012Validator.check_schema(schema)
        except Exception as e:
            raise ValueError(f"Invalid JSON Schema in {name}") from e
