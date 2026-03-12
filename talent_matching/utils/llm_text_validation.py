"""Helpers for validating required text before sending it to LLMs or embeddings."""

from typing import Any


class InsufficientNarrativeDataError(ValueError):
    """Raised when required narrative fields (experience, domain, personality, impact, technical) are empty.

    Used by candidate_vectors to fail fast without retries: empty narratives indicate
    missing or unusable CV data (e.g. no CV text, or LLM returned empty). Tag runs with
    error_type=insufficient_narrative_data for filtering.
    """


class MissingDesiredJobCategoryError(ValueError):
    """Raised when a candidate has no desired job category (Desired Job Category empty or blank).

    Such candidates are erroneous for matchmaking: we require at least one desired role so that
    job_category filtering can exclude them from jobs they did not opt into. Fail at normalization
    so they are not materialized and never included in matchmaking.
    """


def require_meaningful_text(
    value: Any,
    *,
    field_name: str,
    min_length: int = 1,
    invalid_values: set[str] | None = None,
) -> str:
    """Return stripped text or raise when the value is empty/placeholder/too short."""
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is empty.")
    if len(text) < min_length:
        raise ValueError(f"{field_name} is too short ({len(text)} chars; minimum {min_length}).")
    if invalid_values and text in invalid_values:
        raise ValueError(f"{field_name} contains placeholder text: {text!r}.")
    return text


def require_meaningful_text_fields(
    fields: dict[str, Any],
    *,
    context: str,
    min_lengths: dict[str, int] | None = None,
    invalid_values: dict[str, set[str]] | None = None,
) -> dict[str, str]:
    """Validate multiple required text fields and return stripped values."""
    cleaned: dict[str, str] = {}
    errors: list[str] = []

    for field_name, value in fields.items():
        text = str(value or "").strip()
        min_length = (min_lengths or {}).get(field_name, 1)
        invalid_for_field = (invalid_values or {}).get(field_name) or set()

        if not text:
            errors.append(f"{field_name} is empty.")
            continue
        if len(text) < min_length:
            errors.append(f"{field_name} is too short ({len(text)} chars; minimum {min_length}).")
            continue
        if text in invalid_for_field:
            errors.append(f"{field_name} contains placeholder text: {text!r}.")
            continue

        cleaned[field_name] = text

    if errors:
        raise ValueError(f"{context}: " + "; ".join(errors))

    return cleaned
