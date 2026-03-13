"""Airtable field mapping utilities.

This module provides functions for mapping Airtable field formats to our
internal data model. These are used by the AirtableResource but can also
be used independently for testing and data transformation.

Strict field access: use require_airtable_field() so that missing fields
(schema drift, wrong table) fail explicitly instead of silent .get() -> None.
"""

import hashlib
import json
import re
from datetime import datetime
from typing import Any, cast


class AirtableFieldMissingError(Exception):
    """Raised when an expected Airtable field is not present in the record.

    Missing = key not in record["fields"]. Empty value (None, "", []) is allowed.
    """

    def __init__(
        self,
        field_name: str,
        *,
        record_id: str | None = None,
        table_hint: str | None = None,
    ) -> None:
        self.field_name = field_name
        self.record_id = record_id
        self.table_hint = table_hint
        parts = [f"Airtable field {field_name!r} is missing from the record"]
        if record_id:
            parts.append(f"record_id={record_id}")
        if table_hint:
            parts.append(f"table={table_hint}")
        super().__init__("; ".join(parts))


def require_airtable_field(
    fields: dict[str, Any],
    field_name: str,
    *,
    record_id: str | None = None,
    table_hint: str | None = None,
) -> Any:
    """Return the value for an Airtable field; raise if the field is missing.

    Missing = field_name not in fields (schema drift, wrong table). Empty value
    (None, "", []) is allowed and returned as-is.

    Args:
        fields: The record["fields"] dict from an Airtable API response.
        field_name: Exact Airtable column name (e.g. "Desired Job Category").
        record_id: Optional record id for error context.
        table_hint: Optional table name for error context.

    Returns:
        fields[field_name] (may be None or empty).

    Raises:
        AirtableFieldMissingError: If field_name not in fields.
    """
    if field_name not in fields:
        raise AirtableFieldMissingError(field_name, record_id=record_id, table_hint=table_hint)
    return fields[field_name]


def require_airtable_record_fields(
    record: dict[str, Any],
    required_field_names: list[str],
    *,
    table_hint: str | None = None,
) -> dict[str, Any]:
    """Ensure record has "id" and "fields" and all required field names; return fields.

    Raises AirtableFieldMissingError (or KeyError for "id"/"fields") if any are missing.
    """
    if "id" not in record:
        raise AirtableFieldMissingError("id", record_id=None, table_hint=table_hint or "record")
    if "fields" not in record:
        raise AirtableFieldMissingError("fields", record_id=record.get("id"), table_hint=table_hint)
    fields = record["fields"]
    record_id = str(record["id"])
    for name in required_field_names:
        if name not in fields:
            raise AirtableFieldMissingError(name, record_id=record_id, table_hint=table_hint)
    return fields


def require_airtable_field_one_of(
    fields: dict[str, Any],
    field_names: list[str],
    *,
    record_id: str | None = None,
    table_hint: str | None = None,
) -> Any:
    """Return the value for the first Airtable field that exists; raise if none exist.

    Use when the schema may use one of several names (e.g. "Job Category" or
    "Desired Job Category"). Missing = none of field_names in fields.
    """
    for name in field_names:
        if name in fields:
            return fields[name]
    raise AirtableFieldMissingError(
        " or ".join(repr(n) for n in field_names),
        record_id=record_id,
        table_hint=table_hint,
    )


# ATS (Jobs) table: Airtable field names required for map_ats_record_to_raw_job.
# If any are missing from a record, we raise AirtableFieldMissingError.
ATS_REQUIRED_FIELD_NAMES = [
    "Company",
    "Level",
    "Work Set Up Preference",
    "Job Description Link",
    "Open Position (Job Title)",
    "Job Description Text",
    "Job Status",
    "Non Negotiables",
    "Nice-to-have",
    "Projected Salary",
]
# Location: at least one of these must exist (trailing space variant exists in some bases).
ATS_LOCATION_FIELD_NAMES = ["Preferred Location ", "Preferred Location"]
# Job category: at least one of these must exist (allow "Job Category" or "Desired Job Category").
ATS_JOB_CATEGORY_FIELD_NAMES = ["Desired Job Category", "Job Category"]


# Prefix for normalized candidate columns when writing back to Airtable
NORMALIZED_COLUMN_PREFIX = "(N) "

# RawCandidate fields that feed into the normalization LLM (candidate_pipeline sensor
# uses a hash of only these to avoid retriggering when only (N) write-back columns change).
NORMALIZATION_INPUT_FIELDS = [
    "full_name",
    "professional_summary",
    "skills_raw",
    "work_experience_raw",
    "cv_text",
    "location_raw",
    "proof_of_work",
    "desired_job_categories_raw",
    "salary_range_raw",
    "github_url",
    "linkedin_url",
    "x_profile_url",
    "earn_profile_url",
    "cv_url",
]


def compute_normalization_input_hash(mapped_record: dict[str, Any]) -> str:
    """Hash only the RawCandidate fields that feed into normalization.

    Used by the candidate pipeline sensor to skip runs when the only change
    was (N) write-back or other non-input columns. Same serialization as
    AirtableResource._compute_record_hash for stability.
    """
    content = {k: mapped_record.get(k) for k in NORMALIZATION_INPUT_FIELDS}
    content_str = str(sorted(content.items()))
    return hashlib.sha256(content_str.encode()).hexdigest()[:16]


# Syncable NormalizedCandidate fields (exclude id, airtable_record_id, raw_candidate_id, verified_by, normalized_json)
NORMALIZED_CANDIDATE_SYNCABLE_FIELDS = [
    "full_name",
    "email",
    "phone",
    "location_city",
    "location_country",
    "location_region",
    "timezone",
    "professional_summary",
    "current_role",
    "seniority_level",
    "years_of_experience",
    "desired_job_categories",
    "skills_summary",
    "companies_summary",
    "notable_achievements",
    "verified_communities",
    "compensation_min",
    "compensation_max",
    "compensation_currency",
    "job_count",
    "job_switches_count",
    "average_tenure_months",
    "longest_tenure_months",
    "education_highest_degree",
    "education_field",
    "education_institution",
    "hackathon_wins_count",
    "hackathon_total_prize_usd",
    "solana_hackathon_wins",
    "x_handle",
    "linkedin_handle",
    "github_handle",
    "social_followers_total",
    "verification_status",
    "verification_notes",
    "verified_at",
    "prompt_version",
    "model_version",
    "confidence_score",
    "skill_verification_score",
    "normalized_at",
]


def _snake_to_title(name: str) -> str:
    """Convert snake_case to Title Case (e.g. full_name -> Full Name)."""
    return name.replace("_", " ").title()


def _normalized_column_name(snake_name: str) -> str:
    """Return Airtable column name for a normalized field: (N) Title Case."""
    return NORMALIZED_COLUMN_PREFIX + _snake_to_title(snake_name)


# Mapping: NormalizedCandidate attribute name -> Airtable column name (N) prefix
AIRTABLE_CANDIDATES_WRITEBACK_FIELDS: dict[str, str] = {
    name: _normalized_column_name(name) for name in NORMALIZED_CANDIDATE_SYNCABLE_FIELDS
}


def _value_for_airtable(value: Any) -> Any:
    """Coerce a NormalizedCandidate field value for Airtable API (strings, numbers, list, enum, datetime).

    Lists are sent as comma-separated strings so they can be stored in Long text columns and
    deserialized reliably (items may contain newlines or other whitespace).
    """
    if value is None:
        return None
    if hasattr(value, "value"):  # Enum
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        if not value:
            return None
        # Comma-separated for Long text columns; easy to split back (strip each item)
        return ", ".join(str(v).strip() for v in value)
    return value


def normalized_candidate_to_airtable_fields(candidate: dict[str, Any]) -> dict[str, Any]:
    """Build Airtable PATCH fields dict from a NormalizedCandidate row (dict).

    Uses AIRTABLE_CANDIDATES_WRITEBACK_FIELDS. Skips None values. Coerces enums to .value,
    datetimes to ISO 8601 strings, arrays to list of strings.
    """
    fields: dict[str, Any] = {}
    for our_key, airtable_col in AIRTABLE_CANDIDATES_WRITEBACK_FIELDS.items():
        raw = candidate.get(our_key)
        if raw is None:
            continue
        coerced = _value_for_airtable(raw)
        if coerced is None:
            continue
        fields[airtable_col] = coerced
    return fields


# Column name mapping from Airtable to RawCandidate model fields
AIRTABLE_COLUMN_MAPPING: dict[str, str] = {
    "Full Name": "full_name",
    "Location": "location_raw",
    "Desired Job Category": "desired_job_categories_raw",
    "Skills": "skills_raw",
    "CV": "cv_url",
    "Professional summary": "professional_summary",
    "Proof of Work": "proof_of_work",
    "Salary Range": "salary_range_raw",
    "X Profile Link": "x_profile_url",
    "LinkedIn Profile": "linkedin_url",
    "Earn Profile": "earn_profile_url",
    "Git Hub Profile": "github_url",
    "Work Experience": "work_experience_raw",
    "Job Status": "job_status_raw",
}

# Talent (Candidates) table: all mapped columns required so schema drift fails.
TALENT_REQUIRED_FIELD_NAMES = list(AIRTABLE_COLUMN_MAPPING.keys())


def extract_cv_url(cv_field: Any) -> str | None:
    """Extract URL from CV field in various formats.

    Supports:
    1. Airtable API format: List of attachment objects with 'url' key
    2. CSV export format: "filename.pdf (https://...)"
    3. Plain URL string

    Args:
        cv_field: The CV field value from Airtable

    Returns:
        The extracted URL string, or None if no URL found.

    Examples:
        >>> extract_cv_url([{"url": "https://example.com/cv.pdf"}])
        'https://example.com/cv.pdf'

        >>> extract_cv_url("resume.pdf (https://example.com/cv.pdf)")
        'https://example.com/cv.pdf'

        >>> extract_cv_url("https://example.com/cv.pdf")
        'https://example.com/cv.pdf'
    """
    if cv_field is None:
        return None

    # Airtable API format: list of attachment objects
    if isinstance(cv_field, list) and cv_field:
        first_attachment = cv_field[0]
        if isinstance(first_attachment, dict):
            return cast(str | None, first_attachment.get("url"))

    # String formats
    if isinstance(cv_field, str):
        cv_field = cv_field.strip()

        # CSV export format: "filename.pdf (https://...)"
        match = re.search(r"\((https?://[^)]+)\)", cv_field)
        if match:
            return str(match.group(1))

        # Plain URL
        if cv_field.startswith(("http://", "https://")):
            return cv_field

    return None


def is_airtable_error_value(value: Any) -> bool:
    """Return True if value is an Airtable formula/link error payload (treat as empty).

    Airtable can return formula or lookup fields as JSON like
    {"state": "error", "errorType": "emptyDependency", "value": null, "isStale": false}
    when a linked record is missing or the formula fails. We treat these as empty
    so they are not used as real content (e.g. work experience).
    """
    if value is None:
        return False
    if isinstance(value, dict):
        return value.get("state") == "error" and "errorType" in value
    if isinstance(value, str) and value.strip().startswith("{"):
        try:
            parsed = json.loads(value)
            return (
                isinstance(parsed, dict)
                and parsed.get("state") == "error"
                and "errorType" in parsed
            )
        except (json.JSONDecodeError, TypeError):
            return False
    return False


def parse_comma_separated(field_value: str | None) -> list[str]:
    """Parse a comma-separated string into a list of trimmed values.

    Args:
        field_value: Comma-separated string (e.g., "Python,JavaScript,Rust")

    Returns:
        List of trimmed non-empty strings.

    Examples:
        >>> parse_comma_separated("Python, JavaScript, Rust")
        ['Python', 'JavaScript', 'Rust']

        >>> parse_comma_separated(None)
        []

        >>> parse_comma_separated("  One  ,  Two  ,  ")
        ['One', 'Two']
    """
    if not field_value:
        return []

    items = field_value.split(",")
    return [item.strip() for item in items if item.strip()]


# ═══════════════════════════════════════════════════════════════════════════
# NORMALIZED JOB AIRTABLE SYNC (write-back to ATS table)
# ═══════════════════════════════════════════════════════════════════════════

NORMALIZED_JOB_SYNCABLE_FIELDS = [
    "job_title",
    "job_category",
    "role_type",
    "company_name",
    "company_stage",
    "company_size",
    "role_summary",
    "responsibilities",
    "nice_to_haves",
    "benefits",
    "team_context",
    "seniority_level",
    "education_required",
    "domain_experience",
    "tech_stack",
    "location_type",
    "locations",
    "timezone_requirements",
    "employment_type",
    "min_years_experience",
    "max_years_experience",
    "salary_min",
    "salary_max",
    "salary_currency",
    "has_equity",
    "has_token_compensation",
    "narrative_experience",
    "narrative_domain",
    "narrative_personality",
    "narrative_impact",
    "narrative_technical",
    "narrative_role",
    "must_have_skills",
    "nice_to_have_skills",
    "prompt_version",
    "model_version",
    "confidence_score",
    "normalized_at",
]

AIRTABLE_JOBS_WRITEBACK_FIELDS: dict[str, str] = {
    name: _normalized_column_name(name) for name in NORMALIZED_JOB_SYNCABLE_FIELDS
}

# Reverse lookup: Airtable (N) column name -> snake_case DB field name
_AIRTABLE_JOBS_REVERSE_FIELDS: dict[str, str] = {
    v: k for k, v in AIRTABLE_JOBS_WRITEBACK_FIELDS.items()
}

# Fields that should be parsed as integers when reading back from Airtable
_JOB_INT_FIELDS = {"min_years_experience", "max_years_experience", "salary_min", "salary_max"}
# Fields that should be parsed as booleans
_JOB_BOOL_FIELDS = {"has_equity", "has_token_compensation"}
# Fields that should be parsed as floats
_JOB_FLOAT_FIELDS = {"confidence_score"}
# Valid choices for singleSelect fields in the Airtable jobs table.
# Values not in these sets will be dropped to avoid 422 errors.
_JOB_SENIORITY_CHOICES = {"junior", "mid", "senior", "lead", "principal"}
_JOB_LOCATION_TYPE_CHOICES = {"remote", "hybrid", "onsite"}

_JOB_SINGLE_SELECT_CHOICES: dict[str, set[str]] = {
    "seniority_level": _JOB_SENIORITY_CHOICES,
    "location_type": _JOB_LOCATION_TYPE_CHOICES,
}

# Fields that are comma-separated lists in Airtable
_JOB_LIST_FIELDS = {
    "responsibilities",
    "nice_to_haves",
    "benefits",
    "domain_experience",
    "tech_stack",
    "locations",
    "employment_type",
    "must_have_skills",
    "nice_to_have_skills",
}

# Airtable column filled from normalized job narratives (not (N)-prefixed)
SMART_IDEAL_CANDIDATE_PROFILE_FIELD = "SMART IDEAL CANDIDATE PROFILE"

# Order of narrative sections in the combined profile
_JOB_PROFILE_NARRATIVE_KEYS = [
    "narrative_role",
    "narrative_experience",
    "narrative_domain",
    "narrative_technical",
    "narrative_personality",
    "narrative_impact",
]


def build_smart_ideal_candidate_profile(job: dict[str, Any]) -> str | None:
    """Build SMART IDEAL CANDIDATE PROFILE text from normalized job narrative prose fields.

    Concatenates role, experience, domain, technical, personality, and impact narratives
    with section headers. Returns None if no narrative content is present.
    """
    sections: list[str] = []
    labels = {
        "narrative_role": "Role",
        "narrative_experience": "Experience",
        "narrative_domain": "Domain",
        "narrative_technical": "Technical",
        "narrative_personality": "Personality",
        "narrative_impact": "Impact",
    }
    for key in _JOB_PROFILE_NARRATIVE_KEYS:
        text = job.get(key)
        if isinstance(text, str) and text.strip():
            sections.append(f"{labels[key]}\n{text.strip()}")
    if not sections:
        return None
    return "\n\n".join(sections)


def normalized_job_to_airtable_fields(job: dict[str, Any]) -> dict[str, Any]:
    """Build Airtable PATCH fields dict from a NormalizedJob row (dict).

    Uses AIRTABLE_JOBS_WRITEBACK_FIELDS. Skips None values. Coerces enums, datetimes,
    lists (comma-separated), and booleans for Airtable API. Also populates
    SMART IDEAL CANDIDATE PROFILE from the narrative prose fields.

    Values for singleSelect fields that aren't in the Airtable-configured choices
    are silently dropped to avoid 422 errors.
    """
    fields: dict[str, Any] = {}
    for our_key, airtable_col in AIRTABLE_JOBS_WRITEBACK_FIELDS.items():
        raw = job.get(our_key)
        if raw is None:
            continue
        coerced = _value_for_airtable(raw)
        if coerced is None:
            continue
        valid_choices = _JOB_SINGLE_SELECT_CHOICES.get(our_key)
        if valid_choices is not None and coerced not in valid_choices:
            continue
        fields[airtable_col] = coerced

    profile = build_smart_ideal_candidate_profile(job)
    if profile is not None:
        fields[SMART_IDEAL_CANDIDATE_PROFILE_FIELD] = profile

    return fields


def airtable_normalized_job_fields_to_db(airtable_fields: dict[str, Any]) -> dict[str, Any]:
    """Convert Airtable (N)-prefixed fields back to normalized_jobs DB column names and types.

    Parses comma-separated lists, booleans, numbers, and dates from Airtable string
    representations back to Python types suitable for DB storage.
    """
    result: dict[str, Any] = {}
    for airtable_col, value in airtable_fields.items():
        if not airtable_col.startswith(NORMALIZED_COLUMN_PREFIX):
            continue
        db_field = _AIRTABLE_JOBS_REVERSE_FIELDS.get(airtable_col)
        if db_field is None:
            continue
        if value is None or value == "":
            result[db_field] = None
            continue

        if db_field in _JOB_INT_FIELDS:
            result[db_field] = int(value) if value else None
        elif db_field in _JOB_BOOL_FIELDS:
            result[db_field] = bool(value) if not isinstance(value, bool) else value
        elif db_field in _JOB_FLOAT_FIELDS:
            result[db_field] = float(value) if value else None
        elif db_field in _JOB_LIST_FIELDS:
            result[db_field] = parse_comma_separated(str(value)) if value else []
        elif db_field == "normalized_at":
            if isinstance(value, str):
                result[db_field] = datetime.fromisoformat(value)
            else:
                result[db_field] = value
        else:
            result[db_field] = str(value) if not isinstance(value, str) else value
    return result


def map_airtable_row_to_raw_candidate(
    record: dict[str, Any],
    column_mapping: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Map an Airtable record to RawCandidate model fields.

    This function handles the transformation from Airtable's field names
    (e.g., "Full Name", "Professional summary") to our database column names
    (e.g., "full_name", "professional_summary").

    Args:
        record: Airtable record with 'id', 'fields', and 'createdTime' keys
        column_mapping: Optional custom column mapping. Defaults to AIRTABLE_COLUMN_MAPPING.

    Returns:
        Dictionary with mapped field names suitable for creating a RawCandidate.

    Example:
        >>> record = {
        ...     "id": "recXYZ123",
        ...     "createdTime": "2024-01-15T10:30:00.000Z",
        ...     "fields": {
        ...         "Full Name": "John Doe",
        ...         "Skills": "Python,Rust",
        ...         "CV": [{"url": "https://example.com/cv.pdf"}],
        ...     }
        ... }
        >>> mapped = map_airtable_row_to_raw_candidate(record)
        >>> mapped["full_name"]
        'John Doe'
        >>> mapped["airtable_record_id"]
        'recXYZ123'
    """
    if column_mapping is None:
        column_mapping = AIRTABLE_COLUMN_MAPPING

    fields = require_airtable_record_fields(
        record, TALENT_REQUIRED_FIELD_NAMES, table_hint="Talent"
    )
    record_id = str(record["id"])

    # Start with metadata fields
    mapped: dict[str, Any] = {
        "airtable_record_id": record_id,
        "source": "airtable",
        "source_id": record_id,
    }

    # Fields that may contain Airtable formula/link error payloads; treat as empty
    airtable_errorable_fields = frozenset({"work_experience_raw"})

    # Map each Airtable column to our model field (all keys present after require_airtable_record_fields)
    for airtable_col, model_field in column_mapping.items():
        value = fields[airtable_col]

        # Apply field-specific transformations
        if model_field == "cv_url":
            value = extract_cv_url(value)
        if model_field in airtable_errorable_fields and is_airtable_error_value(value):
            value = None

        mapped[model_field] = value

    return mapped
