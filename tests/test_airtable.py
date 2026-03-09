"""Tests for Airtable integration components.

This module tests:
- AirtableResource API interactions (mocked)
- Field mapping utilities
- CV URL extraction
- Data versioning
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from talent_matching.resources.airtable import AirtableATSResource, AirtableResource
from talent_matching.utils.airtable_mapper import (
    AIRTABLE_CANDIDATES_WRITEBACK_FIELDS,
    extract_cv_url,
    map_airtable_row_to_raw_candidate,
    normalized_candidate_to_airtable_fields,
    parse_comma_separated,
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


class TestMapAirtableRowToRawCandidate:
    """Tests for Airtable record mapping."""

    def test_maps_basic_fields(self):
        """Test that basic fields are mapped correctly."""
        record = {
            "id": "recXYZ123",
            "createdTime": "2024-01-15T10:30:00.000Z",
            "fields": {
                "Full Name": "John Doe",
                "Location": "San Francisco, CA",
                "Skills": "Python,Rust,Solana",
            },
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
            "fields": {
                "Full Name": "Jane Doe",
                "CV": [{"url": "https://example.com/cv.pdf"}],
            },
        }

        result = map_airtable_row_to_raw_candidate(record)

        assert result["cv_url"] == "https://example.com/cv.pdf"

    def test_handles_missing_fields(self):
        """Test that missing fields are set to None."""
        record = {
            "id": "recMinimal",
            "fields": {
                "Full Name": "Minimal Candidate",
            },
        }

        result = map_airtable_row_to_raw_candidate(record)

        assert result["full_name"] == "Minimal Candidate"
        assert result["location_raw"] is None
        assert result["skills_raw"] is None
        assert result["cv_url"] is None
        assert result["linkedin_url"] is None

    def test_maps_all_profile_links(self):
        """Test that all profile links are mapped."""
        record = {
            "id": "recProfiles",
            "fields": {
                "Full Name": "Profile Person",
                "X Profile Link": "https://x.com/profile",
                "LinkedIn Profile": "https://linkedin.com/in/profile",
                "Git Hub Profile": "https://github.com/profile",
                "Earn Profile": "https://earn.superteam.fun/profile",
            },
        }

        result = map_airtable_row_to_raw_candidate(record)

        assert result["x_profile_url"] == "https://x.com/profile"
        assert result["linkedin_url"] == "https://linkedin.com/in/profile"
        assert result["github_url"] == "https://github.com/profile"
        assert result["earn_profile_url"] == "https://earn.superteam.fun/profile"


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
                "Open Position (Job Title)": "Growth Analyst",
                "Preferred Location ": ["Middle East", "Europe", "India"],
            },
        }
        result = ats_resource.map_ats_record_to_raw_job(record)
        assert result["location_raw"] == "Middle East, Europe, India"


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
            "fields": {
                "Full Name": "John Doe",
                "Skills": "Python",
            },
        }
        record2 = {
            "id": "rec1",
            "fields": {
                "Full Name": "John Doe",
                "Skills": "Python,Rust",  # Changed
            },
        }

        mapped1 = resource._map_record(record1)
        mapped2 = resource._map_record(record2)

        assert mapped1["_data_version"] != mapped2["_data_version"]

    def test_record_hash_same_for_same_content(self, resource):
        """Test that data version hash is consistent for same content."""
        record = {
            "id": "rec1",
            "fields": {
                "Full Name": "John Doe",
                "Skills": "Python",
            },
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
            "fields": {
                "Full Name": "Test User",
                "Skills": "Python,Rust",
            },
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
                {"id": "rec1", "fields": {"Full Name": "User 1"}},
            ],
            "offset": "page2_cursor",
        }

        # Second page without offset (last page)
        page2_response = MagicMock()
        page2_response.json.return_value = {
            "records": [
                {"id": "rec2", "fields": {"Full Name": "User 2"}},
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
