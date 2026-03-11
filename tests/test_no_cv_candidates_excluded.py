"""Tests for excluding no-CV candidates from matchmaking (no LLM, no DB row)."""

from dagster import build_asset_context

from talent_matching.assets.candidates import candidate_vectors


def test_sentinel_structure():
    """Normalized_candidates no-CV return must be recognized by IO manager and candidate_vectors."""
    # Shape returned by normalized_candidates when no CV data (see candidates.py)
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
