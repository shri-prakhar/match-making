"""Tests for score_candidate_job_fit (LLM candidate-job scoring)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from talent_matching.llm.operations.score_candidate_job_fit import score_candidate_job_fit


@pytest.fixture
def mock_openrouter():
    """Mock OpenRouter that returns a valid JSON response."""
    mock = MagicMock()
    mock.complete = AsyncMock(
        return_value={
            "choices": [
                {
                    "message": {
                        "content": '{"fit_score": 7, "pros": ["Strong skills"], "cons": [], "fulfills_all_must_haves": true}'
                    }
                }
            ]
        }
    )
    return mock


@pytest.fixture
def minimal_candidate():
    """Candidate dict; serialized JSON must be >= 200 chars (score_candidate_job_fit validation)."""
    return {
        "full_name": "Test Candidate",
        "skills_summary": ["Python", "Rust", "Distributed systems"],
        "location_region": "Europe",
        "professional_summary": (
            "Senior engineer with 6+ years building backend services and APIs. "
            "Experience with Python, PostgreSQL, and cloud infrastructure."
        ),
    }


# job_description must be at least 100 chars (score_candidate_job_fit validation)
MIN_JOB_DESC = (
    "We are looking for a senior backend engineer with 5+ years of experience in Python "
    "and distributed systems. You will work on our core API and data pipelines. Remote-friendly."
)


@pytest.fixture
def minimal_job():
    return {"job_title": "Engineer", "job_category": "Backend"}


class TestScoreCandidateJobFit:
    """Tests for score_candidate_job_fit signature and behavior."""

    def test_accepts_non_negotiables_nice_to_have_location_raw(
        self, mock_openrouter, minimal_candidate, minimal_job
    ):
        """Verify the function accepts the kwargs passed by llm_refined_shortlist."""
        result = asyncio.run(
            score_candidate_job_fit(
                mock_openrouter,
                minimal_candidate,
                MIN_JOB_DESC,
                minimal_job,
                [{"skill_name": "Python", "requirement_type": "must_have"}],
                non_negotiables="Must be in Europe",
                nice_to_have="Rust experience",
                location_raw="Europe, North America",
            )
        )
        assert result["fit_score"] == 7
        assert result["fulfills_all_must_haves"] is True
        mock_openrouter.complete.assert_called_once()
        call_args = mock_openrouter.complete.call_args
        user_content = call_args.kwargs["messages"][1]["content"]
        assert "RECRUITER NON-NEGOTIABLES" in user_content
        assert "Must be in Europe" in user_content
        assert "NICE-TO-HAVES" in user_content
        assert "Rust experience" in user_content
        assert "REQUIRED LOCATION/REGION" in user_content
        assert "Europe, North America" in user_content

    def test_works_without_optional_kwargs(self, mock_openrouter, minimal_candidate, minimal_job):
        """Verify backward compatibility when kwargs are omitted."""
        result = asyncio.run(
            score_candidate_job_fit(
                mock_openrouter,
                minimal_candidate,
                MIN_JOB_DESC,
                minimal_job,
                [],
            )
        )
        assert result["fit_score"] == 7
        mock_openrouter.complete.assert_called_once()
        user_content = mock_openrouter.complete.call_args.kwargs["messages"][1]["content"]
        assert "RECRUITER NON-NEGOTIABLES" not in user_content
        assert "REQUIRED LOCATION/REGION" not in user_content
