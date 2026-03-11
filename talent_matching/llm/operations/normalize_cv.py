"""CV normalization LLM operation.

Extracts structured candidate data from raw CV text using an LLM.

Bump PROMPT_VERSION when changing the prompt to trigger asset staleness.
"""

import json
from typing import TYPE_CHECKING, Any

from talent_matching.llm.job_category_prompts_loader import get_cv_extraction_prompt
from talent_matching.models.enums import proficiency_scale_for_prompt

if TYPE_CHECKING:
    from talent_matching.resources.openrouter import OpenRouterResource

# Bump this version when the prompt changes
# Format: MAJOR.MINOR.PATCH
# - MAJOR: Breaking changes to output schema
# - MINOR: New fields or significant prompt improvements
# - PATCH: Minor wording tweaks or bug fixes
PROMPT_VERSION = "5.3.0"  # v5.3.0: Category-aware CV extraction — concatenate per-category prompts for desired_job_categories

# Default model for CV normalization (cost-effective for extraction)
DEFAULT_MODEL = "openai/gpt-4o-mini"

_SYSTEM_PROMPT_TEMPLATE = """You are a CV parser. Extract and normalize the following CV into this exact JSON structure:

{
  "name": "Full name of the candidate",
  "email": "Email address or null",
  "phone": "Phone number or null",
  "years_of_experience": <total years as number or null>,
  "current_role": "Most recent job title",
  "summary": "2-3 sentence professional summary",
  "seniority_level": "JUNIOR|MID|SENIOR|STAFF|LEAD|PRINCIPAL|EXECUTIVE|null",

  "confidence_score": <0.0-1.0 float: how confident you are in the overall extraction quality, penalize sparse or ambiguous CVs>,

  "location": {
    "city": "City name or null",
    "country": "Country name or null",
    "region": "Geographic region (e.g., 'Europe', 'North America', 'Southeast Asia') or null",
    "timezone": "Timezone like 'America/New_York' or null"
  },

  "skills": [
    {
      "name": "Skill name (e.g., 'Python', 'React', 'PostgreSQL')",
      "years": <estimated years of experience with this skill or null>,
      "proficiency": <1-10 rating based on evidence: 1-3=beginner, 4-6=intermediate, 7-8=advanced, 9-10=expert>,
      "evidence": "What the candidate built or achieved with this skill and how capable they are: concrete outcomes, systems, or products (1-2 sentences). Used for semantic matchmaking with job requirements."
    }
  ],

  "experience": [
    {
      "company": "Company name",
      "role": "Job title",
      "start_date": "YYYY-MM format (e.g., '2023-03') or null",
      "end_date": "YYYY-MM format or null if current job",
      "duration_months": <calculated months between dates or estimated>,
      "description": "Full description of responsibilities and achievements",
      "technologies": ["Tech used in this role..."]
    }
  ],

  "education": {
    "highest_degree": "Highest degree (e.g., 'Bachelor's', 'Master's', 'PhD') or null",
    "field": "Field of study (e.g., 'Computer Science') or null",
    "institution": "Name of university/college or null"
  },

  "projects": [
    {
      "name": "Project name",
      "description": "What the project does",
      "technologies": ["Tech stack used..."],
      "url": "Project URL or null"
    }
  ],

  "hackathons": [
    {
      "name": "Hackathon name",
      "description": "Placement/achievement (e.g., 'Top 6 teams out of 500', 'Finalist among 4000+ projects') or null",
      "prize": "Prize won (e.g., '1st Place', 'Best DeFi') or null",
      "prize_amount_usd": <prize in USD as number or null>,
      "is_solana": <true if Solana/Solana Foundation hackathon>
    }
  ],

  "achievements": ["Other awards, certifications, notable accomplishments..."],

  "social_handles": {
    "github": "GitHub username only (not full URL) or null",
    "linkedin": "LinkedIn handle only (not full URL) or null",
    "twitter": "Twitter/X handle only (not full URL) or null"
  },

  "verified_communities": ["Any crypto/tech communities they are verified members of..."],

  "soft_attributes": {
    "leadership": {
      "score": <1-5>,
      "reasoning": "1-2 sentence explanation citing specific CV evidence"
    },
    "autonomy": {
      "score": <1-5>,
      "reasoning": "1-2 sentence explanation citing specific CV evidence"
    },
    "technical_depth": {
      "score": <1-5>,
      "reasoning": "1-2 sentence explanation citing specific CV evidence"
    },
    "communication": {
      "score": <1-5>,
      "reasoning": "1-2 sentence explanation citing specific CV evidence"
    },
    "growth_trajectory": {
      "score": <1-5>,
      "reasoning": "1-2 sentence explanation citing specific CV evidence"
    }
  },

  "narratives": {
    "experience": "Pure prose paragraph describing their career journey, key roles, progression, and what they've accomplished. Write as natural flowing text, no labels or bullet points.",
    "domain": "Pure prose paragraph about the industries, markets, and problem spaces they know deeply. Include specific protocols, ecosystems, or verticals.",
    "personality": "Pure prose paragraph describing work style, values, collaboration approach, and culture signals. What kind of environment do they thrive in?",
    "impact": "Pure prose paragraph about scope of ownership, scale of systems built, team sizes led, and measurable outcomes achieved.",
    "technical": "Pure prose paragraph describing their technical depth - systems thinking, architecture decisions, infrastructure experience, and areas of deep expertise."
  }
}

IMPORTANT:
- Extract email and phone if present in the CV
- When "country" is present in location, always set "region". If the CV does not state the region, infer it from the country (e.g. China -> Asia, Germany -> Europe, United States -> North America). Use consistent region names: Europe, North America, South America, Asia, APAC, EMEA, Middle East, Southeast Asia, Africa, Australia.
- skills is an array of objects with "name", "years", "proficiency" (1-10), and "evidence"
- For experience dates, use YYYY-MM format (e.g., "2023-03"). Use null for end_date if current job
- For social handles, extract just the username (e.g., "darshan9solanki" not "https://x.com/darshan9solanki")
- Separate hackathons from general projects - hackathons have prizes/competitions
- seniority_level must be uppercase: JUNIOR, MID, SENIOR, STAFF (IC track), LEAD (management track), PRINCIPAL, or EXECUTIVE
- Be factual. If information is missing, use null. Do not make up information.

SKILL PROFICIENCY RATING GUIDELINES (use this exact scale: {proficiency_scale}):
- 1 (Novice): Mentioned skill, no evidence of use
- 2 (Beginner): Coursework, tutorials, or minimal exposure
- 3 (Elementary): Basic exposure, used in a minor context
- 4 (Developing): Used in projects, ~1-2 years experience
- 5 (Competent): Solid working knowledge, ~2-3 years
- 6 (Proficient): Primary tech in job roles, 3+ years, meaningful projects
- 7 (Advanced): Strong practitioner, significant production experience
- 8 (Expert): Tech lead/architect level, core contributor, 5+ years deep experience
- 9 (Master): Industry-recognized depth, core infrastructure/protocol work
- 10 (World-class): Published author, major OSS maintainer, recognized expert
- Base ratings on EVIDENCE in the CV, not assumptions. Be conservative if evidence is limited.

SKILL EVIDENCE / ACHIEVEMENTS (for matchmaking):
For each skill, the "evidence" field is used for semantic matching with job requirements. Describe in 1-2 sentences what the candidate built or delivered using this skill (e.g. "Built production trading engine in Rust; led design of multi-token staking rewards"). Avoid generic phrases; cite specific projects, scale, or impact so we can compare to what jobs expect.

SOFT ATTRIBUTES RATING GUIDELINES (1-5 scale):

Leadership (has led teams/projects, made key decisions):
- 1: IC only, no leadership evidence
- 2: Informal mentoring or small project leadership
- 3: Led small projects or feature teams
- 4: Led significant teams/initiatives, hiring involvement
- 5: Led organizations/departments, C-level or founder experience

Autonomy (works independently, self-directed):
- 1: Worked only in structured team settings
- 2: Some independent task completion
- 3: Independent on projects, minimal supervision needed
- 4: Self-directed, drove initiatives without guidance
- 5: Founder-type self-starter, built things from scratch solo

Technical Depth (systems thinking, architecture skills):
- 1: Surface-level implementation
- 2: Understands components, some debugging skills
- 3: Solid practitioner, designs features independently
- 4: Architect-level, designs complex systems
- 5: Industry expert, deep infrastructure/protocol work

Communication (collaboration, documentation, public presence):
- 1: Minimal evidence of collaboration
- 2: Basic teamwork mentioned
- 3: Collaborates well, writes documentation
- 4: Technical writing, presentations, open source maintainer
- 5: DevRel, conference speaker, published author

Growth Trajectory (career progression, learning velocity):
- 1: Static career, same level roles
- 2: Slow, steady progression
- 3: Normal career growth
- 4: Fast progression, expanding scope
- 5: Exceptional acceleration, rapid title/responsibility growth

IMPORTANT: The "reasoning" field for each soft attribute should be a descriptive sentence citing specific evidence from the CV. This text will be used for semantic matching, so be specific and qualitative.

NARRATIVE WRITING GUIDELINES:

The "narratives" section contains pure prose paragraphs optimized for semantic search. These will be embedded as vectors and compared via cosine similarity against job description narratives. Write them as natural, flowing paragraphs - NO labels, NO bullet points, NO structured text.

**Vocabulary alignment:** Use the same vocabulary and concepts that job descriptions use. For example, describe career progression ("progressed from X to Y"), domain expertise ("deep expertise in X, particularly Y"), work style ("async-first", "ownership mentality", "builder"), impact scope ("led a team of N", "platform serving N users"), and technical depth ("distributed systems", "architecture decisions", "infrastructure"). This alignment maximizes match quality between candidate and job embeddings.

Experience narrative example:
"Progressed from backend engineer to tech lead over five years, primarily in fintech and crypto. Built trading systems handling millions in daily volume at Coinbase, then led a team of eight engineers at a DeFi startup where they architected the core smart contract infrastructure. Most recent role involved full-stack ownership of a cross-chain bridge serving institutional clients."

Domain narrative example:
"Deep expertise in decentralized finance, particularly lending protocols and automated market makers. Extensive experience with EVM chains, Solana, and cross-chain infrastructure. Understands both the technical implementation and the financial mechanisms that drive DeFi products. Has worked extensively with institutional trading workflows and compliance requirements."

Personality narrative example:
"Builder mentality who thrives in early-stage chaos and ambiguity. Prefers async-first communication and deep work blocks over constant meetings. Takes ownership of problems end-to-end rather than waiting for direction. Values shipping quickly and iterating based on user feedback over perfect planning."

Impact narrative example:
"Led architecture decisions for a platform serving over ten million users with 99.99% uptime. Owned the end-to-end development of a trading engine processing fifty million dollars in daily volume. Grew engineering team from three to twelve while maintaining velocity. Reduced infrastructure costs by sixty percent through optimization work."

Technical narrative example:
"Systems thinker who designs for scale and maintainability. Strong in distributed systems, particularly consensus mechanisms and eventual consistency patterns. Deep experience with Rust for performance-critical components and TypeScript for rapid iteration. Has built and operated production infrastructure handling millions of concurrent connections."

Be specific and cite evidence from the CV. Each narrative should be 3-5 sentences of pure prose."""

SYSTEM_PROMPT = _SYSTEM_PROMPT_TEMPLATE.replace(
    "{proficiency_scale}", proficiency_scale_for_prompt()
)


def _build_system_prompt(
    base_prompt: str,
    desired_job_categories: list[str] | None,
) -> str:
    """Build system prompt: base + optional concatenated category prompts.

    When desired_job_categories is non-empty, appends a section listing
    important skills/evidence to consider for each category (from DB or default).
    """
    if not desired_job_categories:
        return base_prompt
    parts = [
        "For this candidate we match to the following role categories. For each category, consider these important skills and evidence:",
        "",
    ]
    for cat in desired_job_categories:
        if not (cat and str(cat).strip()):
            continue
        prompt_text = get_cv_extraction_prompt(str(cat).strip())
        parts.append(f"**{cat.strip()}:**")
        parts.append(prompt_text)
        parts.append("")
    if len(parts) <= 2:
        return base_prompt
    return base_prompt + "\n\n" + "\n".join(parts)


class NormalizeCVResult:
    """Result of CV normalization with usage stats for Dagster metadata."""

    def __init__(
        self, data: dict[str, Any], usage: dict[str, Any], model: str, prompt_version: str
    ):
        self.data = data
        self.usage = usage
        self.model = model
        self.prompt_version = prompt_version

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


def _build_cv_content(
    cv_text_airtable: str | None,
    cv_text_pdf: str | None,
) -> str:
    """Build the CV content from available sources.

    When both sources are available, combines them with instructions
    for the LLM to merge and reconcile information from both.
    """
    has_airtable = cv_text_airtable and len(cv_text_airtable.strip()) > 50
    has_pdf = cv_text_pdf and len(cv_text_pdf.strip()) > 50

    if has_airtable and has_pdf:
        # Both sources available - combine with merge instructions
        return f"""You have TWO sources of information for this candidate. Merge them to extract ALL available data.

=== SOURCE 1: AIRTABLE TEXT ===
This may contain manually entered or curated information:

{cv_text_airtable}

=== SOURCE 2: PDF EXTRACTION ===
This is extracted directly from the candidate's CV/resume PDF:

{cv_text_pdf}

=== INSTRUCTIONS ===
- Combine information from BOTH sources
- If there are conflicts, prefer more detailed/recent information
- Extract ALL skills, experience, and projects mentioned in EITHER source
- Don't miss any information - the final profile should be comprehensive"""

    elif has_pdf:
        return f"Parse this CV:\n\n{cv_text_pdf}"
    elif has_airtable:
        return f"Parse this CV:\n\n{cv_text_airtable}"
    else:
        return "No CV content available."


async def normalize_cv(
    openrouter: "OpenRouterResource",
    raw_cv_text: str | None = None,
    cv_text_pdf: str | None = None,
    desired_job_categories: list[str] | None = None,
) -> NormalizeCVResult:
    """Normalize a CV into structured format using LLM.

    Supports dual-source CV extraction where both Airtable text and
    PDF-extracted text are available. The LLM merges both sources
    to create a comprehensive profile.

    When desired_job_categories is provided, the system prompt includes
    concatenated category-specific extraction guidance (from DB or default)
    so the single extraction run captures evidence relevant to all target roles.

    Args:
        openrouter: OpenRouterResource instance for API calls
        raw_cv_text: Raw text from Airtable (original cv_text field)
        cv_text_pdf: Text extracted from PDF attachment
        desired_job_categories: Optional list of job categories; when set, appends
            per-category prompt fragments to the base prompt (DB or in-code default).

    Returns:
        NormalizeCVResult with data and usage stats for metadata
    """
    model = DEFAULT_MODEL

    system_prompt = _build_system_prompt(SYSTEM_PROMPT, desired_job_categories)
    user_content = _build_cv_content(raw_cv_text, cv_text_pdf)

    response = await openrouter.complete(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        model=model,
        operation="normalize_cv",
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    content = response["choices"][0]["message"]["content"]
    usage = response.get("usage", {})

    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"LLM returned {type(parsed).__name__} instead of dict. "
            f"First 200 chars: {content[:200]}"
        )
    if "name" not in parsed:
        raise ValueError(
            f"LLM response missing required 'name' field. " f"Keys returned: {list(parsed.keys())}"
        )

    return NormalizeCVResult(data=parsed, usage=usage, model=model, prompt_version=PROMPT_VERSION)
