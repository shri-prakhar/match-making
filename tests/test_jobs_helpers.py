"""Tests for job asset helpers (e.g. _build_job_description_for_scoring)."""

from talent_matching.assets.jobs import _build_job_description_for_scoring


class TestBuildJobDescriptionForScoring:
    """Tests for _build_job_description_for_scoring."""

    def test_prefers_raw_when_substantial(self):
        raw = {"job_description": "Full job posting text with many details about the role."}
        norm = {"role_summary": "Role summary", "narratives": {"role": "Role narrative"}}
        result, source = _build_job_description_for_scoring(raw, norm)
        assert result == raw["job_description"]
        assert source == "raw"

    def test_uses_normalized_desc_when_raw_empty(self):
        raw = {"job_description": ""}
        norm = {"job_description": "Stored at normalization time, full text from Notion."}
        result, source = _build_job_description_for_scoring(raw, norm)
        assert result == norm["job_description"]
        assert source == "normalized"

    def test_builds_from_normalized_content_when_both_empty(self):
        raw = {"job_description": ""}
        norm = {
            "role_summary": "Backend engineer for DeFi",
            "narratives": {"role": "Day-to-day coding.", "domain": "DeFi protocols."},
            "normalized_json": {
                "requirements": {
                    "must_have_skills": [{"name": "Python"}, {"name": "Rust"}],
                    "nice_to_have_skills": [{"name": "Solidity"}],
                },
                "responsibilities": ["Build APIs", "Review code"],
            },
        }
        result, source = _build_job_description_for_scoring(raw, norm)
        assert source == "synthesized"
        assert "Role Summary: Backend engineer for DeFi" in result
        assert "Role Description" in result
        assert "Day-to-day coding" in result
        assert "Domain Expertise" in result
        assert "DeFi protocols" in result
        assert "Must-have skills: Python, Rust" in result
        assert "Nice-to-have skills: Solidity" in result
        assert "Responsibilities:" in result
        assert "Build APIs" in result

    def test_returns_placeholder_when_all_empty(self):
        raw = {"job_description": ""}
        norm = {}
        result, source = _build_job_description_for_scoring(raw, norm)
        assert result == "(No job description available)"
        assert source == "empty"
