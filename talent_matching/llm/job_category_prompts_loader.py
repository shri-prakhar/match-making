"""Load per-job-category prompts from DB with in-code default when no row exists.

Same pattern as get_weights_for_job_category: DB override when present,
else non-specialized default so callers always get usable text.
"""

from sqlalchemy import select

from talent_matching.db import get_session
from talent_matching.models.job_category_prompts import JobCategoryPromptsRecord

# Non-specialized defaults when a category has no DB row (first-time or new category).
DEFAULT_CV_EXTRACTION_PROMPT = (
    "Extract skills generously: include all abilities that support job matching for this role type. "
    "For each skill include clear evidence (outcomes, scale, ownership). "
    "Capture tech stack and tools where mentioned; also quota, partnerships, or growth metrics if present. More skills with evidence is better."
)
DEFAULT_REFINEMENT_PROMPT = (
    "Evaluate the candidate against the job's must-have requirements and overall fit. "
    "Consider skills match, experience level, and domain relevance."
)


def get_cv_extraction_prompt(job_category: str | None) -> str:
    """Return CV extraction prompt for the job category (DB or in-code default).

    When job_category is missing, empty, or has no DB row (or null column),
    returns DEFAULT_CV_EXTRACTION_PROMPT so callers always get usable text.
    """
    key = (job_category or "").strip()
    if not key:
        return DEFAULT_CV_EXTRACTION_PROMPT
    session = get_session()
    row = session.execute(
        select(JobCategoryPromptsRecord).where(JobCategoryPromptsRecord.job_category == key)
    ).scalar_one_or_none()
    session.close()
    if row is not None and row.cv_extraction_prompt and row.cv_extraction_prompt.strip():
        return row.cv_extraction_prompt.strip()
    return DEFAULT_CV_EXTRACTION_PROMPT


def get_refinement_prompt(job_category: str | None) -> str:
    """Return refinement prompt for the job category (DB or in-code default).

    When job_category is missing, empty, or has no DB row (or null column),
    returns DEFAULT_REFINEMENT_PROMPT so callers always get usable text.
    """
    key = (job_category or "").strip()
    if not key:
        return DEFAULT_REFINEMENT_PROMPT
    session = get_session()
    row = session.execute(
        select(JobCategoryPromptsRecord).where(JobCategoryPromptsRecord.job_category == key)
    ).scalar_one_or_none()
    session.close()
    if row is not None and row.refinement_prompt and row.refinement_prompt.strip():
        return row.refinement_prompt.strip()
    return DEFAULT_REFINEMENT_PROMPT
