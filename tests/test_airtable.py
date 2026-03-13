"""Tests for Airtable integration components.

This module tests:
- AirtableResource API interactions (mocked)
- Field mapping utilities
- CV URL extraction
- Data versioning
"""

import os
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from talent_matching.resources.airtable import AirtableATSResource, AirtableResource
from talent_matching.utils.airtable_mapper import (
    AIRTABLE_CANDIDATES_WRITEBACK_FIELDS,
    ATS_JOB_CATEGORY_FIELD_NAMES,
    ATS_KNOWN_FIELD_NAMES,
    ATS_LOCATION_FIELD_NAMES,
    ATS_REQUIRED_FIELD_NAMES,
    NORMALIZATION_INPUT_FIELDS,
    TALENT_REQUIRED_FIELD_NAMES,
    AirtableFieldMissingError,
    compute_normalization_input_hash,
    extract_cv_url,
    is_airtable_error_value,
    map_airtable_row_to_raw_candidate,
    normalized_candidate_to_airtable_fields,
    parse_comma_separated,
    require_airtable_record_fields,
)


class TestExtractCvUrl:
    """Tests for CV URL extraction from various formats."""

    def test_extract_from_airtable_attachment_list(self):
        """Test extracting URL from Airtable API attachment format."""
        cv_field = [{"url": "https://example.com/cv.pdf", "filename": "resume.pdf"}]
        result = extract_cv_url(cv_field)
        assert result == "https://example.com/cv.pdf"

    def test_extract_from_csv_format(self):
        """Test extracting URL from CSV export format."""
        cv_field = "Mayank-Rawat-Fullstack.pdf (https://v5.airtableusercontent.com/v3/u/50/50/123)"
        result = extract_cv_url(cv_field)
        assert result == "https://v5.airtableusercontent.com/v3/u/50/50/123"

    def test_extract_from_plain_url(self):
        """Test extracting plain URL without filename prefix."""
        cv_field = "https://example.com/cv.pdf"
        result = extract_cv_url(cv_field)
        assert result == "https://example.com/cv.pdf"

    def test_extract_from_http_url(self):
        """Test extracting HTTP (non-HTTPS) URL."""
        cv_field = "http://example.com/cv.pdf"
        result = extract_cv_url(cv_field)
        assert result == "http://example.com/cv.pdf"

    def test_returns_none_for_empty_list(self):
        """Test that empty attachment list returns None."""
        result = extract_cv_url([])
        assert result is None

    def test_returns_none_for_none_input(self):
        """Test that None input returns None."""
        result = extract_cv_url(None)
        assert result is None

    def test_returns_none_for_invalid_string(self):
        """Test that string without URL returns None."""
        result = extract_cv_url("just a filename.pdf")
        assert result is None

    def test_handles_whitespace(self):
        """Test that whitespace is handled correctly."""
        cv_field = "  https://example.com/cv.pdf  "
        result = extract_cv_url(cv_field)
        assert result == "https://example.com/cv.pdf"


class TestIsAirtableErrorValue:
    """Tests for Airtable formula/link error payload detection."""

    def test_empty_dependency_string(self):
        """emptyDependency error as string (e.g. from API) is detected."""
        value = (
            '{"state": "error", "errorType": "emptyDependency", "value": null, "isStale": false}'
        )
        assert is_airtable_error_value(value) is True

    def test_empty_dependency_dict(self):
        """emptyDependency error as dict is detected."""
        value = {"state": "error", "errorType": "emptyDependency", "value": None}
        assert is_airtable_error_value(value) is True

    def test_normal_string_not_detected(self):
        """Normal text is not treated as error."""
        assert is_airtable_error_value("Real work experience here.") is False
        assert is_airtable_error_value("") is False
        assert is_airtable_error_value(None) is False

    def test_valid_json_not_error_not_detected(self):
        """JSON that is not an error payload is not detected."""
        assert is_airtable_error_value('{"key": "value"}') is False

    def test_invalid_json_string_not_detected(self):
        """Malformed JSON string is not treated as error (no crash)."""
        assert is_airtable_error_value("{ not valid json") is False


class TestParseCommaSeparated:
    """Tests for comma-separated field parsing."""

    def test_basic_parsing(self):
        """Test basic comma-separated parsing."""
        result = parse_comma_separated("Python,JavaScript,Rust")
        assert result == ["Python", "JavaScript", "Rust"]

    def test_handles_whitespace(self):
        """Test that whitespace around items is trimmed."""
        result = parse_comma_separated("Python , JavaScript , Rust")
        assert result == ["Python", "JavaScript", "Rust"]

    def test_filters_empty_items(self):
        """Test that empty items are filtered out."""
        result = parse_comma_separated("Python,,Rust,")
        assert result == ["Python", "Rust"]

    def test_returns_empty_for_none(self):
        """Test that None input returns empty list."""
        result = parse_comma_separated(None)
        assert result == []

    def test_returns_empty_for_empty_string(self):
        """Test that empty string returns empty list."""
        result = parse_comma_separated("")
        assert result == []


class TestNormalizationInputHash:
    """Tests for NORMALIZATION_INPUT_FIELDS and compute_normalization_input_hash (sensor skip logic)."""

    def test_normalization_input_fields_excludes_job_status(self):
        """NORMALIZATION_INPUT_FIELDS must not include job_status_raw (not used in normalization)."""
        assert "job_status_raw" not in NORMALIZATION_INPUT_FIELDS

    def test_normalization_input_fields_are_raw_inputs_only(self):
        """NORMALIZATION_INPUT_FIELDS are exactly the raw fields that feed the normalization LLM."""
        # From normalized_candidates asset: cv_text_airtable + cv_url (for PDF) + cv_text (Airtable)
        expected = {
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
        }
        assert set(NORMALIZATION_INPUT_FIELDS) == expected

    def test_hash_stable_for_same_input(self):
        """Same dict produces same hash."""
        record = {"full_name": "Jane", "skills_raw": "Python", "cv_url": "https://x.com/cv.pdf"}
        h1 = compute_normalization_input_hash(record)
        h2 = compute_normalization_input_hash(record)
        assert h1 == h2
        assert len(h1) == 16
        assert all(c in "0123456789abcdef" for c in h1)

    def test_hash_ignores_extra_keys(self):
        """Adding keys not in NORMALIZATION_INPUT_FIELDS does not change hash."""
        record = {"full_name": "Jane", "skills_raw": "Python"}
        h1 = compute_normalization_input_hash(record)
        record["(N) Full Name"] = "Jane Doe"  # write-back column
        record["_data_version"] = "abc123"
        h2 = compute_normalization_input_hash(record)
        assert h1 == h2

    def test_hash_changes_when_input_field_changes(self):
        """Changing any normalization input field changes the hash."""
        record = {"full_name": "Jane", "skills_raw": "Python"}
        h1 = compute_normalization_input_hash(record)
        record["skills_raw"] = "Python,Rust"
        h2 = compute_normalization_input_hash(record)
        assert h1 != h2


def _full_talent_fields(overrides: dict | None = None) -> dict:
    """All required Talent Airtable keys present (None by default). Merge overrides for tests."""
    fields = {k: None for k in TALENT_REQUIRED_FIELD_NAMES}
    if overrides:
        fields.update(overrides)
    return fields


class TestMapAirtableRowToRawCandidate:
    """Tests for Airtable record mapping."""

    def test_maps_basic_fields(self):
        """Test that basic fields are mapped correctly."""
        record = {
            "id": "recXYZ123",
            "createdTime": "2024-01-15T10:30:00.000Z",
            "fields": _full_talent_fields(
                {
                    "Full Name": "John Doe",
                    "Location": "San Francisco, CA",
                    "Skills": "Python,Rust,Solana",
                }
            ),
        }

        result = map_airtable_row_to_raw_candidate(record)

        assert result["airtable_record_id"] == "recXYZ123"
        assert result["full_name"] == "John Doe"
        assert result["location_raw"] == "San Francisco, CA"
        assert result["skills_raw"] == "Python,Rust,Solana"
        assert result["source"] == "airtable"
        assert result["source_id"] == "recXYZ123"

    def test_maps_cv_url_from_attachment(self):
        """Test that CV URL is extracted from attachment format."""
        record = {
            "id": "recABC",
            "fields": _full_talent_fields(
                {
                    "Full Name": "Jane Doe",
                    "CV": [{"url": "https://example.com/cv.pdf"}],
                }
            ),
        }

        result = map_airtable_row_to_raw_candidate(record)

        assert result["cv_url"] == "https://example.com/cv.pdf"

    def test_handles_empty_required_fields_as_none(self):
        """Test that required fields present but empty are mapped to None."""
        record = {
            "id": "recMinimal",
            "fields": _full_talent_fields({"Full Name": "Minimal Candidate"}),
        }

        result = map_airtable_row_to_raw_candidate(record)

        assert result["full_name"] == "Minimal Candidate"
        assert result["location_raw"] is None
        assert result["skills_raw"] is None
        assert result["cv_url"] is None
        assert result["linkedin_url"] is None

    def test_raises_when_required_field_missing_and_not_in_schema(self):
        """When known_schema is not set, missing required field raises (wrong table / schema drift)."""
        record = {
            "id": "recPartial",
            "fields": {"Full Name": "No Category"},
        }
        with pytest.raises(AirtableFieldMissingError) as exc_info:
            require_airtable_record_fields(record, TALENT_REQUIRED_FIELD_NAMES, table_hint="Talent")
        assert exc_info.value.field_name in TALENT_REQUIRED_FIELD_NAMES

    def test_talent_omitted_fields_treated_as_empty(self):
        """When required fields are omitted (Airtable empty), mapping succeeds with None."""
        record = {"id": "recMin", "fields": {"Full Name": "Jane"}}
        result = map_airtable_row_to_raw_candidate(record)
        assert result["full_name"] == "Jane"
        assert result["airtable_record_id"] == "recMin"
        assert result["desired_job_categories_raw"] is None
        assert result["skills_raw"] is None
        assert result["cv_url"] is None

    def test_maps_all_profile_links(self):
        """Test that all profile links are mapped."""
        record = {
            "id": "recProfiles",
            "fields": _full_talent_fields(
                {
                    "Full Name": "Profile Person",
                    "X Profile Link": "https://x.com/profile",
                    "LinkedIn Profile": "https://linkedin.com/in/profile",
                    "Git Hub Profile": "https://github.com/profile",
                    "Earn Profile": "https://earn.superteam.fun/profile",
                }
            ),
        }

        result = map_airtable_row_to_raw_candidate(record)

        assert result["x_profile_url"] == "https://x.com/profile"
        assert result["linkedin_url"] == "https://linkedin.com/in/profile"
        assert result["github_url"] == "https://github.com/profile"
        assert result["earn_profile_url"] == "https://earn.superteam.fun/profile"

    def test_work_experience_airtable_error_mapped_to_none(self):
        """Work Experience formula/link error payload is mapped to None (not stored as content)."""
        record = {
            "id": "rec95yW2hzAVnQuMX",
            "fields": _full_talent_fields(
                {
                    "Full Name": "Mike Hukiewitz",
                    "Work Experience": '{"state": "error", "errorType": "emptyDependency", "value": null, "isStale": false}',
                }
            ),
        }
        result = map_airtable_row_to_raw_candidate(record)
        assert result["work_experience_raw"] is None
        assert result["full_name"] == "Mike Hukiewitz"


class TestAirtableATSResourceMapRecord:
    """Tests for Airtable ATS record mapping (map_ats_record_to_raw_job)."""

    @pytest.fixture
    def ats_resource(self):
        return AirtableATSResource(
            base_id="appTEST",
            table_id="tblATS",
            api_key="pat_test",
        )

    def test_maps_ats_recruiter_guidance_fields(self, ats_resource):
        """Test that ATS Non Negotiables and Nice-to-have are mapped."""
        record = {
            "id": "recATS123",
            "fields": {
                "Open Position (Job Title)": "Staff Backend Engineer",
                "Company": ["Acme Corp"],
                "Job Description Link": "https://notion.so/Job-abc",
                "Job Description Text": "Build APIs.",
                "Job Status": "Matchmaking Ready",
                "Non Negotiables": "5+ years Node.js",
                "Nice-to-have": "Rust, Solana",
                "Projected Salary": "$150k–$200k",
                "Preferred Location": ["Remote", "Europe"],
                "Level": ["Senior"],
                "Desired Job Category": ["Engineering"],
                "Work Set Up Preference": ["Remote"],
            },
        }
        result = ats_resource.map_ats_record_to_raw_job(record)
        assert result["airtable_record_id"] == "recATS123"
        assert result["source"] == "airtable_ats"
        assert result["job_title"] == "Staff Backend Engineer"
        assert result["company_name"] == "Acme Corp"
        assert result["non_negotiables"] == "5+ years Node.js"
        assert result["nice_to_have"] == "Rust, Solana"
        assert result["projected_salary"] == "$150k–$200k"
        assert result["location_raw"] == "Remote, Europe"
        assert result["experience_level_raw"] == "Senior"
        assert result["job_category_raw"] == "Engineering"

    def test_maps_ats_preferred_location_with_trailing_space(self, ats_resource):
        """Test that ATS 'Preferred Location ' (trailing space) maps to location_raw."""
        record = {
            "id": "recATS456",
            "fields": {
                "Company": [],
                "Level": [],
                "Work Set Up Preference": [],
                "Job Description Link": None,
                "Open Position (Job Title)": "Growth Analyst",
                "Job Description Text": "",
                "Job Status": None,
                "Non Negotiables": None,
                "Nice-to-have": None,
                "Projected Salary": None,
                "Preferred Location ": ["Middle East", "Europe", "India"],
                "Desired Job Category": [],
            },
        }
        result = ats_resource.map_ats_record_to_raw_job(record)
        assert result["location_raw"] == "Middle East, Europe, India"


class TestAirtableFieldContract:
    """Integration-style tests: lock required vs optional Airtable fields so schema drift is caught.

    If you add a field to ATS_REQUIRED_FIELD_NAMES or TALENT_REQUIRED_FIELD_NAMES, these tests
    ensure a record without that field raises AirtableFieldMissingError. If you make a field
    optional (remove from required), a record without it must still map successfully.
    """

    @pytest.fixture
    def ats_resource(self):
        return AirtableATSResource(
            base_id="appTEST",
            table_id="tblATS",
            api_key="pat_test",
        )

    def _minimal_ats_fields(self, overrides=None):
        """Minimal ATS record: only required fields + one of location + one of job category."""
        overrides = overrides or {}
        fields = {k: None for k in ATS_REQUIRED_FIELD_NAMES}
        fields["Preferred Location"] = []
        fields["Desired Job Category"] = []
        for k, v in overrides.items():
            fields[k] = v
        return fields

    def test_ats_minimal_required_fields_succeed(self, ats_resource):
        """Record with only ATS_REQUIRED_FIELD_NAMES + location + category maps without error."""
        record = {
            "id": "recMinimal",
            "fields": self._minimal_ats_fields(
                {
                    "Company": ["Acme"],
                    "Job Description Link": "https://example.com",
                    "Open Position (Job Title)": "Test Role",
                    "Job Description Text": "Description",
                    "Job Status": "Matchmaking Ready",
                }
            ),
        }
        result = ats_resource.map_ats_record_to_raw_job(record)
        assert result["airtable_record_id"] == "recMinimal"
        assert result["job_title"] == "Test Role"
        assert result["job_description"] == "Description"

    def test_ats_missing_required_field_raises_when_not_in_schema(self):
        """When known_schema is not set, missing any required field raises (wrong table / schema drift)."""
        minimal = self._minimal_ats_fields(
            {
                "Company": ["Acme"],
                "Job Description Link": "https://x.com",
                "Open Position (Job Title)": "Role",
                "Job Description Text": "Desc",
                "Job Status": "Matchmaking Ready",
            }
        )
        for required in ATS_REQUIRED_FIELD_NAMES:
            fields = {k: v for k, v in minimal.items() if k != required}
            record = {"id": "recX", "fields": fields}
            with pytest.raises(AirtableFieldMissingError) as exc_info:
                require_airtable_record_fields(record, ATS_REQUIRED_FIELD_NAMES, table_hint="ATS")
            assert exc_info.value.field_name == required

    def test_ats_required_field_empty_treated_as_empty(self, ats_resource):
        """When field is in known_schema but omitted (Airtable empty), we treat as empty, no raise."""
        # Record missing "Company" and "Job Status" (omitted = empty in Airtable)
        fields = {
            "Job Description Link": "https://example.com",
            "Open Position (Job Title)": "Role",
            "Job Description Text": "Desc",
            "Preferred Location": [],
            "Desired Job Category": [],
        }
        record = {"id": "recEmpty", "fields": fields}
        result = ats_resource.map_ats_record_to_raw_job(record)
        assert result["airtable_record_id"] == "recEmpty"
        assert result["company_name"] is None
        assert result["job_title"] == "Role"
        assert result["status_raw"] is None

    def test_ats_optional_fields_may_be_missing(self, ats_resource):
        """Record without Level, Work Set Up Preference, Non Negotiables, Nice-to-have, Projected Salary still maps."""
        record = {
            "id": "recNoOptional",
            "fields": self._minimal_ats_fields(
                {
                    "Company": [],
                    "Job Description Link": None,
                    "Open Position (Job Title)": "Compliance Lead",
                    "Job Description Text": "Text",
                    "Job Status": None,
                }
            ),
        }
        # No Level, Work Set Up Preference, etc. - must not raise
        result = ats_resource.map_ats_record_to_raw_job(record)
        assert result["job_title"] == "Compliance Lead"
        assert result["experience_level_raw"] is None
        assert result["work_setup_raw"] is None
        assert result["non_negotiables"] is None
        assert result["nice_to_have"] is None
        assert result["projected_salary"] is None

    def test_ats_require_airtable_record_fields_fails_on_missing_required(self):
        """require_airtable_record_fields raises when any required field is missing (no known_schema)."""
        record = {
            "id": "recY",
            "fields": {
                "Company": [],
                "Open Position (Job Title)": "X",
                # missing Job Description Link, Job Description Text, Job Status
            },
        }
        with pytest.raises(AirtableFieldMissingError):
            require_airtable_record_fields(record, ATS_REQUIRED_FIELD_NAMES, table_hint="ATS")

    def test_ats_field_not_in_known_schema_raises(self):
        """When a required field is absent and not in known_schema, we raise (schema drift)."""
        # Schema that doesn't include "Job Status" (e.g. wrong table)
        wrong_schema = [n for n in ATS_KNOWN_FIELD_NAMES if n != "Job Status"]
        record = {
            "id": "recZ",
            "fields": {
                "Company": [],
                "Job Description Link": "https://x.com",
                "Open Position (Job Title)": "Role",
                "Job Description Text": "Desc",
                "Preferred Location": [],
                "Desired Job Category": [],
                # Job Status omitted
            },
        }
        with pytest.raises(AirtableFieldMissingError) as exc_info:
            require_airtable_record_fields(
                record,
                ATS_REQUIRED_FIELD_NAMES,
                table_hint="ATS",
                known_schema=wrong_schema,
            )
        assert exc_info.value.field_name == "Job Status"

    def test_talent_required_fields_documented(self):
        """TALENT_REQUIRED_FIELD_NAMES is non-empty and contains key fields."""
        assert len(TALENT_REQUIRED_FIELD_NAMES) > 0
        assert "Full Name" in TALENT_REQUIRED_FIELD_NAMES
        assert "Desired Job Category" in TALENT_REQUIRED_FIELD_NAMES

    def test_ats_required_and_optional_lists_disjoint(self):
        """ATS optional field names are not in ATS_REQUIRED_FIELD_NAMES (contract clarity)."""
        optional = {
            "Level",
            "Work Set Up Preference",
            "Non Negotiables",
            "Nice-to-have",
            "Projected Salary",
        }
        required_set = set(ATS_REQUIRED_FIELD_NAMES)
        assert (
            required_set & optional == set()
        ), "Optional fields must not be in ATS_REQUIRED_FIELD_NAMES"


def _fetch_airtable_table_field_names(base_id: str, table_id: str, token: str) -> set[str]:
    """Fetch table schema from Airtable Meta API and return field names. Raises on HTTP errors."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    for t in data.get("tables", []):
        if t.get("id") == table_id:
            return {f.get("name") for f in t.get("fields", []) if f.get("name")}
    return set()


@pytest.mark.integration
class TestAirtableSchemaIntegration:
    """Integration tests: assert our required/optional field names exist in the live Airtable schema.

    Skipped unless AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, AIRTABLE_TABLE_ID (Talent),
    and AIRTABLE_API_KEY (or AIRTABLE_SCHEMA_TOKEN) are set. Run with:
      set -a && source .env && set +a && poetry run pytest tests/test_airtable.py -m integration -v
    """

    @pytest.fixture(scope="class")
    def _airtable_env(self):
        base_id = os.getenv("AIRTABLE_BASE_ID")
        ats_table_id = os.getenv("AIRTABLE_ATS_TABLE_ID")
        talent_table_id = os.getenv("AIRTABLE_TABLE_ID")
        token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")
        if not base_id or not ats_table_id or not talent_table_id or not token:
            pytest.skip(
                "Set AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, AIRTABLE_TABLE_ID, and "
                "AIRTABLE_API_KEY (or AIRTABLE_SCHEMA_TOKEN) to run Airtable schema integration tests"
            )
        return {
            "base_id": base_id,
            "ats_table_id": ats_table_id,
            "talent_table_id": talent_table_id,
            "token": token,
        }

    def test_ats_known_fields_exist_in_schema(self, _airtable_env):
        """Required ATS fields and at least one of location/category names exist in the ATS table schema."""
        schema_names = _fetch_airtable_table_field_names(
            _airtable_env["base_id"],
            _airtable_env["ats_table_id"],
            _airtable_env["token"],
        )
        missing_required = [n for n in ATS_REQUIRED_FIELD_NAMES if n not in schema_names]
        assert not missing_required, (
            f"ATS table schema is missing required fields: {missing_required}. "
            "Rename/restore in Airtable or update ATS_REQUIRED_FIELD_NAMES."
        )
        has_location = any(n in schema_names for n in ATS_LOCATION_FIELD_NAMES)
        assert has_location, (
            f"ATS table schema must have one of location fields {ATS_LOCATION_FIELD_NAMES}. "
            "Rename in Airtable or update ATS_LOCATION_FIELD_NAMES."
        )
        has_category = any(n in schema_names for n in ATS_JOB_CATEGORY_FIELD_NAMES)
        assert has_category, (
            f"ATS table schema must have one of job category fields {ATS_JOB_CATEGORY_FIELD_NAMES}. "
            "Rename in Airtable or update ATS_JOB_CATEGORY_FIELD_NAMES."
        )

    def test_talent_required_fields_exist_in_schema(self, _airtable_env):
        """Every Talent required field exists in the Talent table schema."""
        schema_names = _fetch_airtable_table_field_names(
            _airtable_env["base_id"],
            _airtable_env["talent_table_id"],
            _airtable_env["token"],
        )
        missing = [n for n in TALENT_REQUIRED_FIELD_NAMES if n not in schema_names]
        assert not missing, (
            f"Talent table schema is missing fields we require: {missing}. "
            "Rename/restore in Airtable or update TALENT_REQUIRED_FIELD_NAMES / AIRTABLE_COLUMN_MAPPING."
        )


class TestAirtableResource:
    """Tests for the AirtableResource class."""

    @pytest.fixture
    def resource(self):
        """Create a test AirtableResource instance."""
        return AirtableResource(
            base_id="appTEST123456789",
            table_id="tblTEST123456789",
            api_key="pat_test_key",
        )

    def test_base_url_construction(self, resource):
        """Test that base URL is constructed correctly."""
        assert resource._base_url == "https://api.airtable.com/v0/appTEST123456789/tblTEST123456789"

    def test_headers_include_auth(self, resource):
        """Test that headers include authorization."""
        headers = resource._headers
        assert headers["Authorization"] == "Bearer pat_test_key"
        assert headers["Content-Type"] == "application/json"

    def test_record_hash_changes_on_content_change(self, resource):
        """Test that data version hash changes when content changes."""
        record1 = {
            "id": "rec1",
            "fields": _full_talent_fields({"Full Name": "John Doe", "Skills": "Python"}),
        }
        record2 = {
            "id": "rec1",
            "fields": _full_talent_fields({"Full Name": "John Doe", "Skills": "Python,Rust"}),
        }

        mapped1 = resource._map_record(record1)
        mapped2 = resource._map_record(record2)

        assert mapped1["_data_version"] != mapped2["_data_version"]

    def test_record_hash_same_for_same_content(self, resource):
        """Test that data version hash is consistent for same content."""
        record = {
            "id": "rec1",
            "fields": _full_talent_fields({"Full Name": "John Doe", "Skills": "Python"}),
        }

        mapped1 = resource._map_record(record)
        mapped2 = resource._map_record(record)

        assert mapped1["_data_version"] == mapped2["_data_version"]

    @patch("talent_matching.resources.airtable.httpx.Client")
    def test_fetch_record_by_id(self, mock_client_class, resource):
        """Test fetching a single record by ID."""
        mock_client = MagicMock()
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "recTEST",
            "createdTime": "2024-01-15T10:30:00.000Z",
            "fields": _full_talent_fields({"Full Name": "Test User", "Skills": "Python,Rust"}),
        }
        mock_client.get.return_value = mock_response

        result = resource.fetch_record_by_id("recTEST")

        assert result["airtable_record_id"] == "recTEST"
        assert result["full_name"] == "Test User"
        assert result["skills_raw"] == "Python,Rust"
        mock_client.get.assert_called_once()

    @patch("talent_matching.resources.airtable.httpx.Client")
    def test_fetch_all_records_handles_pagination(self, mock_client_class, resource):
        """Test that pagination is handled correctly."""
        mock_client = MagicMock()
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)

        # First page with offset
        page1_response = MagicMock()
        page1_response.json.return_value = {
            "records": [
                {"id": "rec1", "fields": _full_talent_fields({"Full Name": "User 1"})},
            ],
            "offset": "page2_cursor",
        }

        # Second page without offset (last page)
        page2_response = MagicMock()
        page2_response.json.return_value = {
            "records": [
                {"id": "rec2", "fields": _full_talent_fields({"Full Name": "User 2"})},
            ],
        }

        mock_client.get.side_effect = [page1_response, page2_response]

        result = resource.fetch_all_records()

        assert len(result) == 2
        assert result[0]["airtable_record_id"] == "rec1"
        assert result[1]["airtable_record_id"] == "rec2"
        assert mock_client.get.call_count == 2

    @patch("talent_matching.resources.airtable.httpx.Client")
    def test_get_all_record_ids(self, mock_client_class, resource):
        """Test getting all record IDs."""
        mock_client = MagicMock()
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "records": [
                {"id": "rec1"},
                {"id": "rec2"},
                {"id": "rec3"},
            ],
        }
        mock_client.get.return_value = mock_response

        result = resource.get_all_record_ids()

        assert result == ["rec1", "rec2", "rec3"]

    @patch("talent_matching.resources.airtable.httpx.Client")
    def test_update_record(self, mock_client_class, resource):
        """Test PATCH update_record."""
        mock_client = MagicMock()
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"id": "rec1", "fields": {"(N) Full Name": "Jane"}}
        mock_client.patch.return_value = mock_response

        result = resource.update_record("rec1", {"(N) Full Name": "Jane"})

        assert result["id"] == "rec1"
        assert result["fields"]["(N) Full Name"] == "Jane"
        mock_client.patch.assert_called_once()
        call_kwargs = mock_client.patch.call_args[1]
        assert call_kwargs["json"] == {"fields": {"(N) Full Name": "Jane"}}


class TestNormalizedCandidateWriteback:
    """Tests for (N)-prefixed normalized candidate → Airtable field mapping."""

    def test_airtable_candidates_writeback_fields_has_n_prefix(self):
        """All writeback column names use (N) prefix."""
        for airtable_col in AIRTABLE_CANDIDATES_WRITEBACK_FIELDS.values():
            assert airtable_col.startswith("(N) "), f"Expected (N) prefix: {airtable_col}"

    def test_airtable_candidates_writeback_fields_contains_expected_keys(self):
        """Mapping includes expected syncable fields."""
        assert "full_name" in AIRTABLE_CANDIDATES_WRITEBACK_FIELDS
        assert AIRTABLE_CANDIDATES_WRITEBACK_FIELDS["full_name"] == "(N) Full Name"
        assert "professional_summary" in AIRTABLE_CANDIDATES_WRITEBACK_FIELDS
        assert "years_of_experience" in AIRTABLE_CANDIDATES_WRITEBACK_FIELDS
        assert "verification_status" in AIRTABLE_CANDIDATES_WRITEBACK_FIELDS

    def test_normalized_candidate_to_airtable_fields_skips_none(self):
        """None values are omitted from the payload."""
        candidate = {"full_name": "Jane", "email": None, "phone": None}
        out = normalized_candidate_to_airtable_fields(candidate)
        assert out == {"(N) Full Name": "Jane"}
        assert "(N) Email" not in out

    def test_normalized_candidate_to_airtable_fields_coerces_list(self):
        """List/array fields are serialized for Airtable (e.g. comma-separated or list)."""
        candidate = {"skills_summary": ["Python", "Rust"]}
        out = normalized_candidate_to_airtable_fields(candidate)
        # Mapper may output list or comma-separated string depending on Airtable field type
        val = out["(N) Skills Summary"]
        assert val == ["Python", "Rust"] or val == "Python, Rust"

    def test_normalized_candidate_to_airtable_fields_coerces_enum(self):
        """Enum values are serialized with .value."""

        class StubEnum:
            value = "verified"

        candidate = {"verification_status": StubEnum()}
        out = normalized_candidate_to_airtable_fields(candidate)
        assert out["(N) Verification Status"] == "verified"

    def test_normalized_candidate_to_airtable_fields_coerces_datetime(self):
        """Datetime values become ISO 8601 strings."""
        dt = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
        candidate = {"normalized_at": dt}
        out = normalized_candidate_to_airtable_fields(candidate)
        assert "(N) Normalized At" in out
        assert "2025-01-15" in out["(N) Normalized At"]
