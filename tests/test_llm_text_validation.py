import pytest

from talent_matching.utils.llm_text_validation import (
    require_meaningful_text,
    require_meaningful_text_fields,
)


def test_require_meaningful_text_rejects_empty_string():
    with pytest.raises(ValueError, match="job_description is empty"):
        require_meaningful_text("", field_name="job_description")


def test_require_meaningful_text_rejects_too_short_text():
    with pytest.raises(ValueError, match="minimum 5"):
        require_meaningful_text("abcd", field_name="job_description", min_length=5)


def test_require_meaningful_text_fields_reports_multiple_invalid_fields():
    with pytest.raises(ValueError, match="experience is empty"):
        require_meaningful_text_fields(
            {
                "experience": "",
                "domain": "No domain narrative",
            },
            context="candidate_vectors record_id=rec123",
            invalid_values={
                "domain": {"No domain narrative"},
            },
        )


def test_require_meaningful_text_fields_returns_cleaned_values():
    result = require_meaningful_text_fields(
        {
            "experience": "  Built systems  ",
            "domain": "DeFi and infra",
        },
        context="job_vectors record_id=rec123",
    )

    assert result == {
        "experience": "Built systems",
        "domain": "DeFi and infra",
    }
