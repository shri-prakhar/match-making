"""Tests for run failure sensor classification."""

import pytest

from talent_matching.sensors.run_failure_sensor import _classify_failure


def test_classify_insufficient_narrative_data():
    """InsufficientNarrativeDataError and empty-narrative message get INSUFFICIENT_NARRATIVE_DATA tag."""
    error_str = (
        "candidate_vectors record_id=rec00qir1klZ4pgqt: experience is empty.; "
        "domain is empty.; personality is empty.; impact is empty.; technical is empty."
    )
    tags = _classify_failure(error_str)
    assert "INSUFFICIENT_NARRATIVE_DATA" in tags

    # Exception type in traceback also matches
    tags2 = _classify_failure("InsufficientNarrativeDataError: candidate_vectors record_id=recX: ...")
    assert "INSUFFICIENT_NARRATIVE_DATA" in tags2
