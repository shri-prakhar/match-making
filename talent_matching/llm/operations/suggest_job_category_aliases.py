"""LLM operation: suggest match_category_aliases for a job category from allowed list."""

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from talent_matching.resources.openrouter import OpenRouterResource

PROMPT_VERSION = "1.0.0"

_SYSTEM = """You are a matchmaking taxonomy helper. Given a job category and a list of allowed job categories (from the Talent table), output which other allowed categories might reasonably describe candidates who would fit this kind of role. Return only category strings that appear in the allowed list. Output JSON only."""

_USER_TEMPLATE = """Job category: "{job_category}"

Allowed job categories (use only these exact strings): {allowed_list}

Return a JSON object with one key: "match_category_aliases" — an array of zero or more strings from the allowed list (excluding the job category itself) that candidates might have in their desired_job_categories for this role. Example: {{"match_category_aliases": ["Operations", "Legal"]}}"""


async def suggest_job_category_aliases(
    openrouter: "OpenRouterResource",
    job_category: str,
    allowed_job_categories: list[str],
    *,
    model: str = "openai/gpt-4o-mini",
) -> list[str]:
    """Ask LLM for suggested match_category_aliases; return only values in allowed list."""
    allowed_set = {c.strip() for c in allowed_job_categories if (c or "").strip()}
    job_cat_stripped = (job_category or "").strip()
    if not job_cat_stripped or not allowed_set:
        return []

    allowed_list_str = ", ".join(f'"{c}"' for c in sorted(allowed_set))
    user_content = _USER_TEMPLATE.format(
        job_category=job_cat_stripped,
        allowed_list=allowed_list_str,
    )

    response = await openrouter.complete(
        messages=[
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": user_content},
        ],
        model=model,
        operation="suggest_job_category_aliases",
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    content = response["choices"][0]["message"]["content"]
    data = json.loads(content)
    raw = data.get("match_category_aliases") or []
    if not isinstance(raw, list):
        return []
    return [str(c).strip() for c in raw if str(c).strip() in allowed_set]
