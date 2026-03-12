"""Clear DB data for a candidate partition (e.g. when normalization fails).

Use when a candidate partition fails at normalized_candidates so downstream
tables do not keep stale data from a previous successful run.
"""

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from talent_matching.models import NormalizedCandidate, RawCandidate
from talent_matching.models.vectors import CandidateVector


def clear_candidate_partition_data(session: Session, partition_key: str) -> bool:
    """Remove normalized and vector data for one candidate partition.

    Deletes:
    - candidate_vectors for this candidate (keyed by raw_candidates.id)
    - normalized_candidates row (DB CASCADE removes matches, candidate_skills,
      candidate_experiences, candidate_projects, candidate_attributes,
      candidate_role_fitness, and other normalized_candidates children)

    Does not delete raw_candidates (input is preserved).

    Returns True if any row was deleted, False if partition had nothing to clear.
    """
    raw_id = session.execute(
        select(RawCandidate.id).where(RawCandidate.airtable_record_id == partition_key)
    ).scalar_one_or_none()
    if not raw_id:
        return False

    vec_result = session.execute(
        delete(CandidateVector).where(CandidateVector.candidate_id == raw_id)
    )
    vectors_deleted = vec_result.rowcount
    norm_result = session.execute(
        delete(NormalizedCandidate).where(NormalizedCandidate.airtable_record_id == partition_key)
    )
    norm_deleted = norm_result.rowcount
    return (vectors_deleted + norm_deleted) > 0
