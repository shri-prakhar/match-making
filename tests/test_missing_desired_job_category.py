"""Tests for failing normalization when candidate has no desired job category."""

import os
from types import SimpleNamespace

import pytest
from dagster import build_asset_context

from talent_matching.assets.candidates import normalized_candidates
from talent_matching.utils.llm_text_validation import MissingDesiredJobCategoryError


def _mock_openrouter():
    """Minimal openrouter resource so normalized_candidates can call set_context before our raise."""
    return SimpleNamespace(set_context=lambda **kw: None)


def test_normalized_candidates_raises_when_no_desired_job_category():
    """normalized_candidates fails with MissingDesiredJobCategoryError when Desired Job Category is empty."""
    context = build_asset_context(
        partition_key="recNoDesiredCategory",
        resources={"openrouter": _mock_openrouter()},
    )
    # Enough CV data to pass MIN_CV_CONTENT_LENGTH (500), but no desired job category
    raw_candidates = {
        "airtable_record_id": "recNoDesiredCategory",
        "full_name": "Test User",
        "cv_text": "x" * 500,
        "desired_job_categories_raw": "",
    }
    os.environ["OPENROUTER_API_KEY"] = "test-key"
    try:
        with pytest.raises(MissingDesiredJobCategoryError) as exc_info:
            normalized_candidates(context, raw_candidates)
        assert "no desired job category" in str(exc_info.value).lower()
        assert "recNoDesiredCategory" in str(exc_info.value)
    finally:
        os.environ.pop("OPENROUTER_API_KEY", None)


def test_normalized_candidates_raises_when_desired_job_category_blank_only():
    """normalized_candidates fails when desired_job_categories_raw is only whitespace/comma."""
    context = build_asset_context(
        partition_key="recBlankCategory",
        resources={"openrouter": _mock_openrouter()},
    )
    raw_candidates = {
        "airtable_record_id": "recBlankCategory",
        "full_name": "Test User",
        "cv_text": "y" * 500,
        "desired_job_categories_raw": "  ,  , ",
    }
    os.environ["OPENROUTER_API_KEY"] = "test-key"
    try:
        with pytest.raises(MissingDesiredJobCategoryError):
            normalized_candidates(context, raw_candidates)
    finally:
        os.environ.pop("OPENROUTER_API_KEY", None)
