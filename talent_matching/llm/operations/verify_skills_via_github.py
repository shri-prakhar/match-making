"""LLM operation to verify candidate skills against GitHub profile data.

Single prompt: given claimed skills + evidence and full GitHub context
(manifests, README, commits), output per-skill verification status.
"""

import json
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from talent_matching.resources.openrouter import OpenRouterResource

PROMPT_VERSION = "1.0.0"
DEFAULT_MODEL = "openai/gpt-4o-mini"


def _build_verification_prompt(
    skills_with_evidence: list[dict[str, Any]],
    github_username: str,
    repo_metadata: list[dict[str, Any]],
    commit_messages: list[str],
) -> str:
    """Build the single LLM prompt for skill verification."""
    skills_block = "\n".join(
        f'- {s.get("name", "?")}: "{s.get("evidence", "")}"' for s in skills_with_evidence
    )

    repos_blocks = []
    for meta in repo_metadata:
        manifests_str = "N/A"
        if meta.get("manifests"):
            parts = []
            for k, v in meta["manifests"].items():
                vstr = str(v)
                parts.append(f"{k}:\n{vstr[:500]}..." if len(vstr) > 500 else f"{k}:\n{vstr}")
            manifests_str = "\n\n".join(parts)

        entry_str = "N/A"
        if meta.get("entry_points"):
            entry_str = "\n\n".join(f"{k}:\n{v}" for k, v in meta["entry_points"].items())

        block = f"""
--- REPO: {meta.get("full_name", "?")} ---
Description: {meta.get("description", "N/A")}
Languages: {meta.get("languages", {})}

README (excerpt):
{meta.get("readme", "N/A")}

Manifests:
{manifests_str}

Entry-point code:
{entry_str}
"""
        repos_blocks.append(block)

    commits_block = "\n".join(f'- "{m}"' for m in commit_messages[:100])

    return f"""You are verifying a candidate's self-reported skills against their GitHub profile.

CANDIDATE'S CLAIMED SKILLS (with CV evidence):
{skills_block}

GITHUB DATA (user: {github_username}):
{"".join(repos_blocks)}

Commit history (top by message length):
{commits_block}

For each claimed skill above, output a JSON array. Each object must have: "skill" (string), "verified" (boolean), "confidence" (float 0-1), "evidence" (string, one sentence citing specific repo/commit/content).
Return ONLY a valid JSON array, no markdown, no other text."""


async def verify_skills_via_github(
    skills_with_evidence: list[dict[str, Any]],
    github_data: dict[str, Any],
    openrouter: "OpenRouterResource",
) -> list[dict[str, Any]]:
    """Verify each skill against GitHub data via single LLM call.

    Args:
        skills_with_evidence: [{name, evidence}, ...]
        github_data: From fetch_repo_metadata + commit history
        openrouter: OpenRouterResource

    Returns:
        [{skill, verified, confidence, evidence}, ...]
    """
    repo_metadata = github_data.get("repo_metadata", [])
    commit_history = github_data.get("commit_history", {})
    username = github_data.get("username", "")

    # Sample commits
    all_commits: list[tuple[str, int]] = []
    for repo in commit_history.get("repos", []):
        for c in repo.get("commits", []):
            msg = c.get("message", "")
            all_commits.append((msg, len(msg)))
    all_commits.sort(key=lambda x: x[1], reverse=True)
    commit_messages = [msg for msg, _ in all_commits[:100]]

    prompt = _build_verification_prompt(
        skills_with_evidence=skills_with_evidence,
        github_username=username,
        repo_metadata=repo_metadata,
        commit_messages=commit_messages,
    )

    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=DEFAULT_MODEL,
        operation="verify_skills_via_github",
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    content = response["choices"][0]["message"]["content"].strip()

    # Extract JSON array (handle markdown code blocks)
    json_match = re.search(r"\[[\s\S]*\]", content)
    if json_match:
        content = json_match.group(0)

    parsed = json.loads(content)
    if isinstance(parsed, list):
        return parsed

    if isinstance(parsed, dict) and "results" in parsed:
        return parsed["results"]

    return []
