"""Select final shortlist of at most 15 from 30 scored candidates.

Only includes candidates who fulfill ALL must-have requirements.
Used by llm_refined_shortlist after scoring each candidate.

Bump PROMPT_VERSION when changing the prompt to trigger asset staleness.
"""

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from talent_matching.resources.openrouter import OpenRouterResource

# Bump this version when the prompt changes
PROMPT_VERSION = "1.0.0"

# Use a more capable model for selection/ranking
DEFAULT_MODEL = "openai/gpt-4o"

SYSTEM_PROMPT = """You are an expert technical recruiter selecting the final shortlist for a job.

You receive a list of 30 candidates, each with a fit score (1-10), pros, cons, and a flag indicating whether they fulfill ALL must-have requirements.

HARD CONSTRAINT: Only include candidates who fulfill ALL must-have requirements (fulfills_all_must_haves: true). Do NOT include any candidate who is missing any must-have. Exclude them entirely.

From those who qualify, rank and select up to 15 best-fit candidates. Order by fit quality (best first).

Return a single JSON object:
{
  "selected_candidate_ids": ["uuid1", "uuid2", ...],
  "reasoning": "2-4 sentences explaining the selection rationale"
}

If no candidates fulfill all must-haves, return:
{
  "selected_candidate_ids": [],
  "reasoning": "No candidates met all must-have requirements."
}

Use the exact candidate_id UUIDs from the input. Do not invent or modify IDs."""


async def select_final_shortlist(
    openrouter: "OpenRouterResource",
    scored_candidates: list[dict[str, Any]],
    job_title: str,
) -> dict[str, Any]:
    """Select up to 15 candidates from the 30 scored, only those who fulfill all must-haves.

    Args:
        openrouter: OpenRouterResource instance for API calls
        scored_candidates: List of dicts with candidate_id, candidate_name, fit_score, pros, cons, fulfills_all_must_haves
        job_title: Job title for context

    Returns:
        Dict with selected_candidate_ids (list of UUID strings) and reasoning
    """
    user_content = f"""Job: {job_title}

Scored candidates (30 total):
{json.dumps(scored_candidates, indent=2, default=str)}

Select up to 15 candidates who fulfill ALL must-haves. Return JSON with selected_candidate_ids and reasoning."""

    response = await openrouter.complete(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        model=DEFAULT_MODEL,
        operation="select_final_shortlist",
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    content = response["choices"][0]["message"]["content"]
    data = json.loads(content)

    selected = data.get("selected_candidate_ids", [])
    if not isinstance(selected, list):
        selected = []
    data["selected_candidate_ids"] = [str(s) for s in selected]
    data["reasoning"] = str(data.get("reasoning", ""))

    return data
