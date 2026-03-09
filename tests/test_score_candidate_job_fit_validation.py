import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from talent_matching.llm.operations.score_candidate_job_fit import score_candidate_job_fit


@pytest.fixture
def mock_openrouter():
    mock = MagicMock()
    mock.complete = AsyncMock()
    return mock


def test_score_candidate_job_fit_rejects_short_job_description(mock_openrouter):
    with pytest.raises(ValueError, match="job_description is too short"):
        asyncio.run(
            score_candidate_job_fit(
                mock_openrouter,
                {"id": "cand-1", "summary": "x" * 300},
                "too short",
                {"job_title": "Engineer"},
                [{"skill_name": "Python", "requirement_type": "must_have"}],
            )
        )

    mock_openrouter.complete.assert_not_called()
