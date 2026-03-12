"""Score a single candidate against full job description (1-10 scale, pros, cons).

Used by llm_refined_shortlist to evaluate each of the 30 algorithmic shortlist
candidates. Explicitly checks whether the candidate fulfills ALL must-have
requirements.

Bump PROMPT_VERSION when changing the prompt to trigger asset staleness.
"""

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from talent_matching.resources.openrouter import OpenRouterResource

from talent_matching.llm.job_category_prompts_loader import get_refinement_prompt
from talent_matching.utils.llm_text_validation import (
    require_meaningful_text,
    require_meaningful_text_fields,
)

# Bump this version when the prompt changes
PROMPT_VERSION = "1.3.0"  # v1.3.0: inject per-job-category refinement block (DB or default)

# Use a more capable model for reasoning/scoring tasks
DEFAULT_MODEL = "openai/gpt-4o"

SYSTEM_PROMPT = """You are an expert technical recruiter evaluating candidate fit for a specific role.

Analyze the candidate profile against the full job description and requirements. Compare skills, experience, domain expertise, and any other must-have criteria.

Return a single JSON object with this structure:
{
  "fit_score": <integer 1-10, where 1=worst fit, 10=best fit>,
  "pros": ["Strength 1", "Strength 2", ...],
  "cons": ["Gap or concern 1", "Gap or concern 2", ...],
  "fulfills_all_must_haves": <true or false>
}

CRITICAL: Set fulfills_all_must_haves to true ONLY if the candidate fulfills EVERY must-have requirement. Must-haves include:
- All skills listed in MUST-HAVE REQUIREMENTS
- All criteria in RECRUITER NON-NEGOTIABLES (when provided) — e.g. location/region, years of experience, domain, or any other hard filter the recruiter specified
- Location/region when the job has required locations or timezone requirements — candidates outside the required region must be excluded

If ANY must-have is missing or insufficient, set fulfills_all_must_haves to false.

Scoring guidelines:
- 9-10: Exceptional fit, exceeds all requirements
- 7-8: Strong fit, meets all must-haves and most nice-to-haves
- 5-6: Moderate fit, meets core requirements
- 3-4: Weak fit, significant gaps
- 1-2: Poor fit, missing critical requirements"""


async def score_candidate_job_fit(
    openrouter: "OpenRouterResource",
    normalized_candidate: dict[str, Any],
    job_description: str,
    normalized_job: dict[str, Any],
    must_have_requirements: list[dict[str, Any]],
    *,
    non_negotiables: str | None = None,
    nice_to_have: str | None = None,
    location_raw: str | None = None,
    job_category: str | None = None,
) -> dict[str, Any]:
    """Score a candidate against a job (1-10, pros, cons, fulfills_all_must_haves).

    Args:
        openrouter: OpenRouterResource instance for API calls
        normalized_candidate: Full normalized candidate dict from DB (including normalized_json)
        job_description: Raw job description text
        normalized_job: Normalized job dict (requirements, narratives, etc.)
        must_have_requirements: List of must-have skill/requirement dicts from get_job_required_skills
            (skill_name, requirement_type, min_years, expected_capability)
        non_negotiables: Recruiter-provided hard requirements from Airtable (Non Negotiables column)
        nice_to_have: Recruiter-provided nice-to-have preferences from Airtable (Nice-to-have column)
        location_raw: Recruiter-specified required/preferred locations from Airtable
        job_category: Job category for this role; when set, injects category-specific evaluation
            guidance (from DB or in-code default) into the user prompt.

    Returns:
        Dict with fit_score, pros, cons, fulfills_all_must_haves
    """
    job_description = require_meaningful_text(
        job_description,
        field_name="job_description",
        min_length=100,
    )
    must_haves_parts = []
    for r in must_have_requirements:
        name = r.get("skill_name", "")
        cap = r.get("expected_capability") or "required"
        years = r.get("min_years")
        part = f"- {name}: {cap}"
        if years is not None:
            part += f" (min {years} years)"
        must_haves_parts.append(part)
    must_haves_text = "\n".join(must_haves_parts)

    recruiter_parts: list[str] = []
    if (non_negotiables or "").strip():
        recruiter_parts.append(
            f"RECRUITER NON-NEGOTIABLES (hard filter — candidate must fulfill ALL of these):\n{non_negotiables.strip()}"
        )
    if (location_raw or "").strip():
        recruiter_parts.append(
            f"REQUIRED LOCATION/REGION:\n{location_raw.strip()}\n"
            "Candidate must be located in or able to work from these regions. Exclude candidates outside."
        )
    if (nice_to_have or "").strip():
        recruiter_parts.append(
            f"NICE-TO-HAVES (preferred but not required):\n{nice_to_have.strip()}"
        )
    recruiter_section = "\n\n".join(recruiter_parts) if recruiter_parts else ""

    candidate_json = json.dumps(normalized_candidate, indent=2, default=str)
    if len(candidate_json) < 200:
        raise ValueError(
            f"Candidate profile too thin ({len(candidate_json)} chars) for "
            f"candidate_id={normalized_candidate.get('id')}. "
            f"Check normalized_candidates.normalized_json in DB."
        )
    normalized_job_json = json.dumps(normalized_job, indent=2, default=str)
    require_meaningful_text_fields(
        {
            "candidate_json": candidate_json,
            "normalized_job_json": normalized_job_json,
        },
        context="score_candidate_job_fit input validation",
        min_lengths={
            "candidate_json": 200,
            "normalized_job_json": 20,
        },
        invalid_values={
            "normalized_job_json": {"{}", "null"},
        },
    )

    category_block = ""
    if job_category and str(job_category).strip():
        refinement_prompt = get_refinement_prompt(str(job_category).strip())
        category_block = (
            f"\n\nFOR THIS ROLE (job category: {job_category.strip()}):\n{refinement_prompt}\n"
        )

    user_content = f"""Candidate Profile (full, normalized):
{candidate_json}

Job Description (full text):
{job_description}

Normalized Job Requirements:
{normalized_job_json}

MUST-HAVE SKILLS (candidate must fulfill ALL of these):
{must_haves_text if must_haves_text else "(None specified)"}
{recruiter_section + chr(10) if recruiter_section else ""}
{category_block}
Evaluate the candidate against the job. Return JSON with fit_score (1-10), pros, cons, and fulfills_all_must_haves."""

    response = await openrouter.complete(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        model=DEFAULT_MODEL,
        operation="score_candidate_job_fit",
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    content = response["choices"][0]["message"]["content"]
    data = json.loads(content)

    fit_score = data.get("fit_score")
    if fit_score is not None:
        data["fit_score"] = int(fit_score)
    if not isinstance(data.get("pros"), list):
        data["pros"] = data.get("pros", []) or []
    if not isinstance(data.get("cons"), list):
        data["cons"] = data.get("cons", []) or []
    data["fulfills_all_must_haves"] = bool(data.get("fulfills_all_must_haves", False))

    return data
