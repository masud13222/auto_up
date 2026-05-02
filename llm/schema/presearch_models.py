"""
Pydantic models for presearch LLM output (matches ``presearch_response_schema``).

Use for validation after ``repair_json``; ``extra="forbid"`` mirrors ``additionalProperties: false``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PresearchOutput(BaseModel):
    """Validated presearch JSON: content type, titles, year, season tag."""

    model_config = ConfigDict(extra="forbid")

    content_type: Literal["movie", "tvshow"] = Field(
        ...,
        description="movie = single film; tvshow = series with seasons/episodes",
    )
    name: str = Field(..., description="Primary title (no quality, no site branding)")
    alt_name: str = Field(
        default="",
        description="Alternate title if clearly present, else empty string",
    )
    year: str = Field(
        default="",
        description="Release year as four digits, or empty string if unknown",
    )
    season_tag: str = Field(
        default="",
        description="For tvshow: season label as on the page; empty for movies",
    )

    @field_validator("name", mode="before")
    @classmethod
    def _coerce_name(cls, v):
        if v is None:
            return ""
        return str(v).strip()

    @field_validator("name")
    @classmethod
    def _name_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("name must be a non-empty string")
        return v

    @field_validator("alt_name", "year", "season_tag", mode="before")
    @classmethod
    def _coerce_optional_strings(cls, v):
        if v is None:
            return ""
        return str(v).strip()
