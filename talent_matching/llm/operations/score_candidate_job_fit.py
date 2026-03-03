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

# Bump this version when the prompt changes
PROMPT_VERSION = "1.0.0"

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

CRITICAL: Set fulfills_all_must_haves to true ONLY if the candidate fulfills EVERY must-have requirement (skills, experience, domain, etc.). If ANY must-have is missing or insufficient, set it to false.

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
) -> dict[str, Any]:
    """Score a candidate against a job (1-10, pros, cons, fulfills_all_must_haves).

    Args:
        openrouter: OpenRouterResource instance for API calls
        normalized_candidate: Full normalized candidate dict from DB (including normalized_json)
        job_description: Raw job description text
        normalized_job: Normalized job dict (requirements, narratives, etc.)
        must_have_requirements: List of must-have skill/requirement dicts from get_job_required_skills
            (skill_name, requirement_type, min_years, expected_capability)

    Returns:
        Dict with fit_score, pros, cons, fulfills_all_must_haves
    """
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

    user_content = f"""Candidate Profile (full, normalized):
{json.dumps(normalized_candidate, indent=2, default=str)}

Job Description (full text):
{job_description}

Normalized Job Requirements:
{json.dumps(normalized_job, indent=2, default=str)}

MUST-HAVE REQUIREMENTS (candidate must fulfill ALL of these):
{must_haves_text if must_haves_text else "(None specified)"}

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
