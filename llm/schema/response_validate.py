"""
Structured-output validation and instructor-style retry hints for LLM JSON.

Flow: parse/repair JSON → validate (Pydantic for presearch, jsonschema for combined
extract) → on failure, append a short fix instruction and re-call the model.
"""

from __future__ import annotations

from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError

# Aligns with upload.service.info._LLM_JSON_RETRY_MAX (extra validation rounds after first response).
LLM_SCHEMA_RETRY_MAX = 2

VALIDATION_RETRY_SUFFIX = (
    "\n\nYour previous reply did not satisfy the required JSON shape. "
    "Return ONE JSON object only (no markdown fences). Fix these issues:\n{detail}\n"
    "Return the corrected JSON object now."
)


def format_validation_detail(exc: Exception) -> str:
    """Human/model-readable issue list; capped to avoid huge prompts."""
    if isinstance(exc, ValidationError):
        lines: list[str] = []
        for err in exc.errors()[:20]:
            loc = ".".join(str(x) for x in err.get("loc", ()))
            msg = err.get("msg", "")
            if loc:
                lines.append(f"- {loc}: {msg}")
            else:
                lines.append(f"- {msg}")
        return "\n".join(lines) if lines else str(exc)[:1200]
    if isinstance(exc, JsonSchemaValidationError):
        lines_js: list[str] = []
        errors = [exc]
        errors.extend(getattr(exc, "context", ()) or ())
        for err in errors[:20]:
            path = "/".join(str(p) for p in err.absolute_path) if err.absolute_path else ""
            msg = err.message
            if path:
                lines_js.append(f"- {path}: {msg}")
            else:
                lines_js.append(f"- {msg}")
        return "\n".join(lines_js) if lines_js else str(exc)[:1200]
    return str(exc)[:1500]
