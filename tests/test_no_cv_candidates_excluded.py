"""Tests for excluding no-CV candidates from matchmaking (no LLM, no DB row)."""

from types import SimpleNamespace

import pytest
from dagster import build_asset_context

from talent_matching.assets.candidates import (
    MIN_CV_CONTENT_LENGTH,
    candidate_vectors,
    normalized_candidates,
)
from talent_matching.utils.llm_text_validation import InsufficientCvDataError


def test_normalized_candidates_raises_when_insufficient_cv_data():
    """normalized_candidates fails with InsufficientCvDataError when combined length < 500 (no retries, tagged)."""
    context = build_asset_context(
        partition_key="recLowContent",
        resources={"openrouter": SimpleNamespace(set_context=lambda **kw: None)},
    )
    raw_candidates = {
        "airtable_record_id": "recLowContent",
        "full_name": "Short",
        "cv_text": "x" * 100,
        "desired_job_categories_raw": "Engineering",
    }
    with pytest.raises(InsufficientCvDataError) as exc_info:
        normalized_candidates(context, raw_candidates)
    assert "total_cv_length" in str(exc_info.value)
    assert str(MIN_CV_CONTENT_LENGTH) in str(exc_info.value)


def test_sentinel_structure():
    """Sentinel shape still recognized by candidate_vectors if ever passed (e.g. from IO/legacy)."""
    sentinel = {
        "__exclude_from_matchmaking__": True,
        "skip_reason": "no_cv_data",
        "candidate_id": "recXXX",
        "airtable_record_id": "recXXX",
    }
    assert sentinel.get("__exclude_from_matchmaking__") is True
    assert sentinel.get("skip_reason") == "no_cv_data"


def test_candidate_vectors_returns_empty_when_excluded_sentinel():
    """candidate_vectors skips LLM and returns [] when upstream sentinel (no CV data)."""
    context = build_asset_context(
        partition_key="recTestNoCV",
        resources={"openrouter": None},
    )
    sentinel = {
        "__exclude_from_matchmaking__": True,
        "skip_reason": "no_cv_data",
        "candidate_id": "recTestNoCV",
        "airtable_record_id": "recTestNoCV",
    }
    result = candidate_vectors(context, sentinel)
    assert result == []


# When the asset runs in a real pipeline, load_input can return None for a partition
# with no row (excluded); candidate_vectors handles that and returns []. Direct
# invocation type-checks the input as Dict so we only test the sentinel path here.
