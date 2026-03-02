"""Job description normalization LLM operation.

Extracts structured job requirements from raw job descriptions using an LLM.

Bump PROMPT_VERSION when changing the prompt to trigger asset staleness.
"""

import json
from typing import TYPE_CHECKING, Any

from talent_matching.models.enums import proficiency_scale_for_prompt

if TYPE_CHECKING:
    from talent_matching.resources.openrouter import OpenRouterResource

# Bump this version when the prompt changes
# Format: MAJOR.MINOR.PATCH
# - MAJOR: Breaking changes to output schema
# - MINOR: New fields or significant prompt improvements
# - PATCH: Minor wording tweaks or bug fixes
PROMPT_VERSION = "3.5.0"  # v3.5.0: Only extract skills explicitly in JD; do not infer from title

# Default model for job normalization (cost-effective for extraction)
DEFAULT_MODEL = "openai/gpt-4o-mini"

_SYSTEM_PROMPT_TEMPLATE = """You are a job description parser. Extract and normalize this job posting into structured JSON. The output is used for semantic matching with candidate profiles: write job narratives and structured fields so they align with how candidates describe themselves (same vocabulary, same concepts).

Output a single JSON object with this structure (use null when not mentioned):

{
  "title": "Job title in English (translate if the posting is in another language)",
  "company_name": "Company name if mentioned",
  "seniority_level": "JUNIOR|MID|SENIOR|STAFF|LEAD|PRINCIPAL|EXECUTIVE",
  "employment_type": ["full-time", "part-time"] (array: list every type offered; infer from 'Vollzeit/Teilzeit', 'full or part time'; use ["full-time", "part-time"] when both offered)",
  "requirements": {
    "must_have_skills": [{"name": "Canonical skill name", "min_years": "<number or null>", "min_level": "<1-10 proficiency or null>", "expected_capability": "1-2 sentences: what the ideal candidate must be able to do with this skill in this role"}],
    "nice_to_have_skills": [{"name": "Canonical skill name", "min_years": "<number or null>", "min_level": "<1-10 proficiency or null>", "expected_capability": "1-2 sentences: what the ideal candidate should be able to do with this skill"}],
    "years_of_experience_min": <number or null>,
    "years_of_experience_max": <number or null>,
    "education_required": "Degree requirement or null",
    "domain_experience": ["DeFi", "NFT", "Trading", etc.]
  },
  "soft_attribute_requirements": {
    "leadership": <min 1-5 or null>,
    "autonomy": <min 1-5 or null>,
    "technical_depth": <min 1-5 or null>,
    "communication": <min 1-5 or null>,
    "growth_trajectory": <min 1-5 or null>
  },
  "role_description": "2-3 sentence summary of what the role does",
  "team_context": "Team size, reporting structure if mentioned",
  "tech_stack": ["Canonical names: React, TypeScript, etc."],
  "compensation": {
    "salary_min": <number or null, always yearly>,
    "salary_max": <number or null, always yearly>,
    "currency": "USD",
    "equity": true|false|null
  },
  "contact": {
    "hiring_manager_name": "Name if given, else null",
    "hiring_manager_email": "Application or contact email if given (e.g. 'Apply to info@...')",
    "application_url": "URL for applications if given, else null"
  },
  "location": {
    "type": "remote|hybrid|onsite",
    "locations": ["Locations if onsite/hybrid..."],
    "timezone_requirements": "UTC offset range e.g. 'UTC-5 to UTC+1', single offset, or null"
  },
  "responsibilities": ["Bullet or sentence per responsibility"],
  "nice_to_haves": ["Optional bullets"],
  "benefits": ["Benefits if mentioned"],
  "narratives": {
    "experience": "Pure prose: career path and progression the ideal candidate would have (e.g. 'Progressed from X to Y', 'typical background', 'years and trajectory'). Same style as candidate CV narratives. 3-5 sentences, no bullet points.",
    "domain": "Pure prose: industries, protocols, ecosystems, problem spaces (e.g. DeFi, trading, infra). Use same vocabulary as candidate domain narratives. 3-5 sentences.",
    "personality": "Pure prose: work style, values, collaboration, culture. What kind of person thrives (e.g. async-first, ownership, builder mentality). Align with candidate personality narratives. 3-5 sentences.",
    "impact": "Pure prose: scope of ownership, scale, outcomes expected (team size, systems scale, measurable results). Same concepts as candidate impact narratives. 3-5 sentences.",
    "technical": "Pure prose: technical depth, stack, systems context (architecture, infra, deep expertise). Align with candidate technical narratives. 3-5 sentences.",
    "role": "Pure prose: day-to-day role and responsibilities in flowing prose. Single position-like description. No bullet points."
  }
}

Be factual. If information is not mentioned, use null.

**Matchmaking:** Narratives and structured fields are embedded for semantic matching with candidate profiles. Describe the ideal candidate in the same terms candidates use: career journey, domain expertise, work style, impact level, technical depth. Same vocabulary improves match quality.

**Seniority:** Use the same levels as candidate profiles (uppercase): JUNIOR, MID, SENIOR, STAFF (IC track), LEAD (management track), PRINCIPAL, EXECUTIVE.

**Skills:** Only include concrete, matchable **technical skills** that are explicitly mentioned or clearly implied in the job description text (e.g. in responsibilities, requirements, or tech stack). Do NOT infer skills from the job title alone — e.g. a "Growth Analyst" or "Marketing Manager" role with no mention of Java/SQL in the text should have empty or minimal must_have_skills/nice_to_have_skills. Programming languages, frameworks, tools, databases (e.g. "React", "TypeScript", "PostgreSQL", "GraphQL", "CI/CD") belong here; do NOT list domain/vertical requirements (e.g. "Web3", "DeFi") as skills — use role_description and domain narrative for those. Use canonical skill names. For each required skill, output an object with "name", "min_years", "min_level", and "expected_capability".

- **min_years** (int or null): minimum years of experience with this specific skill. Extract from phrases like "5+ years of React", "at least 3 years". Use null when no years requirement is stated or implied for this skill. Do NOT confuse overall years_of_experience_min with per-skill min_years.
- **min_level** (int 1-10 or null): minimum proficiency level expected. Use the following scale (same as candidate ratings): {proficiency_scale}. Infer from context: "production experience" suggests 7-8 (Advanced/Expert), "deep expertise" or "architect-level" suggests 9+ (Master), "familiarity" or "exposure" suggests 3-4 (Elementary/Developing). Use null when proficiency level is not implied.
- **expected_capability**: 1-2 sentences inferring from the job description what the ideal candidate must be capable of with that skill (e.g. "Design and implement REST APIs; integrate with internal services"). Use the same vocabulary as candidate skill evidence for better semantic matching.

Apply to must_have_skills and nice_to_have_skills. tech_stack remains a simple array of canonical names.

**Soft attribute requirements:** Infer minimum 1-5 scores from job description signals (e.g. "lead small team" -> leadership 3; "self-directed" -> autonomy 4). Use the same five dimensions as candidate profiles: leadership, autonomy, technical_depth, communication, growth_trajectory. Use null when not implied.

**Compensation:** Always normalize to yearly amounts for salary_min and salary_max. If the posting states hourly, daily, or monthly rates, convert to yearly (e.g. hourly × 2080 for full-time, × 1040 for half-time; monthly × 12). Keep the stated currency. Use null only when no numeric compensation is given.

**Timezone requirements:** Extract timezone constraints from phrases like "US time zones", "European hours", "EST to CET overlap". Convert named timezones to UTC offset ranges (e.g. "US time zones" -> "UTC-10 to UTC-5", "European hours" -> "UTC+0 to UTC+3"). Use null when no timezone constraint is mentioned. For remote roles, infer from context if possible (e.g. company HQ location, meeting overlap requirements).

**Job title:** Use English. If the posting is in another language (e.g. German "Frontend-Entwickler"), output the English equivalent (e.g. "Frontend Developer")."""

SYSTEM_PROMPT = _SYSTEM_PROMPT_TEMPLATE.replace(
    "{proficiency_scale}", proficiency_scale_for_prompt()
)


class NormalizeJobResult:
    """Result of job normalization with usage stats for Dagster metadata."""

    def __init__(self, data: dict[str, Any], usage: dict[str, Any], model: str):
        self.data = data
        self.usage = usage
        self.model = model

    @property
    def input_tokens(self) -> int:
        return self.usage.get("prompt_tokens", 0)

    @property
    def output_tokens(self) -> int:
        return self.usage.get("completion_tokens", 0)

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def cost_usd(self) -> float:
        return float(self.usage.get("cost", 0))


def _build_user_prompt(
    raw_job_text: str,
    non_negotiables: str | None = None,
    nice_to_have: str | None = None,
    location_raw: str | None = None,
    projected_salary: str | None = None,
) -> str:
    parts = [f"Parse this job description:\n\n{raw_job_text}"]

    recruiter_fields = [non_negotiables, nice_to_have, location_raw, projected_salary]
    if any(recruiter_fields):
        parts.append(
            "\n\n--- RECRUITER GUIDANCE ---\n"
            "The recruiter has provided additional context below. Use it to "
            "refine your classification of must_have_skills vs nice_to_have_skills, "
            "and to strengthen or add requirements that the job description alone "
            "may not make explicit."
        )
        if non_negotiables:
            parts.append(f"\n**Non-negotiables (hard requirements):**\n{non_negotiables}")
        if nice_to_have:
            parts.append(f"\n**Nice-to-haves (preferred but not required):**\n{nice_to_have}")
        if location_raw:
            parts.append(
                f"\n**Preferred candidate locations:**\n{location_raw}\n"
                "Use this to infer timezone_requirements as a UTC offset range "
                "covering these regions."
            )
        if projected_salary:
            parts.append(
                f"\n**Projected salary range:**\n{projected_salary}\n"
                "Use this for the compensation.salary_min and compensation.salary_max "
                "fields. Parse the range into numeric yearly values."
            )

    return "\n".join(parts)


async def normalize_job(
    openrouter: "OpenRouterResource",
    raw_job_text: str,
    *,
    non_negotiables: str | None = None,
    nice_to_have: str | None = None,
    location_raw: str | None = None,
    projected_salary: str | None = None,
) -> NormalizeJobResult:
    """Normalize a job description into structured format using LLM.

    Args:
        openrouter: OpenRouterResource instance for API calls
        raw_job_text: Raw job description text
        non_negotiables: Recruiter-provided hard requirements (free text)
        nice_to_have: Recruiter-provided nice-to-have preferences (free text)
        location_raw: Recruiter-specified preferred locations (e.g. "Singapore, Europe")
        projected_salary: Recruiter-specified salary range (e.g. "$120,000 - $170,000")

    Returns:
        NormalizeJobResult with data, usage stats, and model for metadata
    """
    model = DEFAULT_MODEL
    user_prompt = _build_user_prompt(
        raw_job_text, non_negotiables, nice_to_have, location_raw, projected_salary
    )
    response = await openrouter.complete(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=model,
        operation="normalize_job",
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    content = response["choices"][0]["message"]["content"]
    usage = response.get("usage", {})

    return NormalizeJobResult(data=json.loads(content), usage=usage, model=model)
