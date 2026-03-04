"""GitHub-based skill verification.

Fetches commit history via blobless git clone and repo metadata via API,
then uses a single LLM call to verify each skill against the evidence.
"""

import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from talent_matching.resources.github import GitHubAPIResource
    from talent_matching.resources.openrouter import OpenRouterResource

# Dependency files to fetch for manifests
DEPENDENCY_FILES = [
    "package.json",
    "requirements.txt",
    "pyproject.toml",
    "Cargo.toml",
    "go.mod",
    "Gemfile",
    "pom.xml",
]

# Entry-point files to sample (first 40 lines)
ENTRY_POINT_PATTERNS = [
    "main.py",
    "app.py",
    "index.ts",
    "index.js",
    "src/App.tsx",
    "src/main.tsx",
    "lib/main.rs",
    "cmd/main.go",
]


def clone_and_extract_commits(
    username: str,
    github: "GitHubAPIResource",
    max_repos: int = 15,
) -> dict[str, Any]:
    """Clone repos with blobless clone and extract full commit history.

    Args:
        username: GitHub username
        github: GitHubAPIResource for list_user_repos
        max_repos: Max repos to clone (limit API + clone time)

    Returns:
        {repos: [{name, full_name, url, commits: [{sha, message, author, date}]}]}
    """
    repos_data = github.list_user_repos(username, per_page=max_repos)
    result: dict[str, Any] = {"username": username, "repos": []}

    for repo in repos_data[:max_repos]:
        full_name = repo.get("full_name", "")
        if "/" not in full_name:
            continue
        owner, repo_name = full_name.split("/", 1)
        clone_url = repo.get("clone_url", f"https://github.com/{full_name}.git")

        with tempfile.TemporaryDirectory(prefix="github_clone_") as tmpdir:
            path = Path(tmpdir) / repo_name
            proc = subprocess.run(
                [
                    "git",
                    "clone",
                    "--filter=blob:none",
                    "--bare",
                    "--depth=500",
                    clone_url,
                    str(path),
                ],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=tmpdir,
            )
            if proc.returncode != 0:
                result["repos"].append(
                    {
                        "name": repo_name,
                        "full_name": full_name,
                        "url": clone_url,
                        "commits": [],
                        "error": proc.stderr[:200] if proc.stderr else "clone failed",
                    }
                )
                continue

            # Extract commit log: sha|subject|author|date
            log_proc = subprocess.run(
                [
                    "git",
                    "log",
                    "--all",
                    "--format=%H|%s|%an|%ad",
                    "--date=short",
                    "-n",
                    "500",
                ],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(path),
            )
            commits = []
            if log_proc.returncode == 0 and log_proc.stdout:
                for line in log_proc.stdout.strip().split("\n"):
                    if "|" in line:
                        parts = line.split("|", 3)
                        if len(parts) >= 4:
                            commits.append(
                                {
                                    "sha": parts[0][:12],
                                    "message": parts[1],
                                    "author": parts[2],
                                    "date": parts[3],
                                }
                            )

            result["repos"].append(
                {
                    "name": repo_name,
                    "full_name": full_name,
                    "url": clone_url,
                    "commits": commits,
                }
            )

    return result


def fetch_repo_metadata(
    owner: str,
    repo: str,
    github: "GitHubAPIResource",
) -> dict[str, Any]:
    """Fetch manifests, README, languages, topics, and entry-point snippets.

    Returns:
        Dict with languages, description, topics, readme, manifests, entry_points
    """
    result: dict[str, Any] = {
        "owner": owner,
        "repo": repo,
        "full_name": f"{owner}/{repo}",
        "languages": {},
        "description": None,
        "topics": [],
        "readme": None,
        "manifests": {},
        "entry_points": {},
    }

    result["languages"] = github.get_repo_languages(owner, repo)

    # Get repo info for description (from list_user_repos we might have it, but fetch tree)
    tree = github.get_repo_tree(owner, repo, ref="HEAD", recursive=True)
    paths = [t.get("path") for t in tree if t.get("type") == "blob" and t.get("path")]

    # Fetch dependency files
    for dep_file in DEPENDENCY_FILES:
        if dep_file in paths or any(p.endswith(f"/{dep_file}") for p in paths):
            match = (
                dep_file
                if dep_file in paths
                else next((p for p in paths if p.endswith(f"/{dep_file}")), None)
            )
            if match:
                content = github.get_file_contents(owner, repo, match)
                if content:
                    result["manifests"][dep_file] = content[:2000]

    # README
    for readme_name in ["README.md", "README.rst", "README"]:
        if readme_name in paths or any(p.endswith(f"/{readme_name}") for p in paths):
            match = (
                readme_name
                if readme_name in paths
                else next((p for p in paths if p.endswith(f"/{readme_name}")), None)
            )
            if match:
                content = github.get_file_contents(owner, repo, match)
                if content:
                    result["readme"] = content[:800]
                break

    # Entry-point snippets
    for ep in ENTRY_POINT_PATTERNS:
        if ep in paths or any(p.endswith(f"/{ep}") for p in paths):
            match = ep if ep in paths else next((p for p in paths if p.endswith(f"/{ep}")), None)
            if match:
                content = github.get_file_contents(owner, repo, match)
                if content:
                    lines = content.split("\n")[:40]
                    result["entry_points"][ep] = "\n".join(lines)

    return result


def sample_commits_for_prompt(commit_history: dict[str, Any], top_n: int = 100) -> list[str]:
    """Sample top N commits by message length for the LLM prompt."""
    all_commits: list[tuple[str, int]] = []
    for repo in commit_history.get("repos", []):
        for c in repo.get("commits", []):
            msg = c.get("message", "")
            all_commits.append((msg, len(msg)))

    all_commits.sort(key=lambda x: x[1], reverse=True)
    return [msg for msg, _ in all_commits[:top_n]]


async def verify_skills_llm(
    skills_with_evidence: list[dict[str, Any]],
    github_data: dict[str, Any],
    openrouter: "OpenRouterResource",
) -> list[dict[str, Any]]:
    """Single LLM call to verify each skill. Returns list of VerificationResult dicts."""
    from talent_matching.llm.operations.verify_skills_via_github import (
        verify_skills_via_github,
    )

    return await verify_skills_via_github(
        skills_with_evidence=skills_with_evidence,
        github_data=github_data,
        openrouter=openrouter,
    )
