"""GitHub API resource for fetching developer activity metrics.

This module provides both a mock implementation for testing and a stub
for the future production implementation.
"""

import random
from datetime import datetime, timedelta
from typing import Any

import httpx
from dagster import ConfigurableResource
from pydantic import Field

GITHUB_API_BASE = "https://api.github.com"


class GitHubAPIResource(ConfigurableResource):
    """GitHub API resource for fetching developer statistics.

    In production, this would use the GitHub API with proper authentication.
    Currently implemented as a mock that returns plausible test data.
    """

    api_token: str = Field(
        default="",
        description="GitHub API token (leave empty for mock mode)",
    )
    mock_mode: bool = Field(
        default=True,
        description="If True, return mock data instead of calling GitHub API",
    )

    def get_user_stats(self, username: str) -> dict[str, Any]:
        """Fetch GitHub statistics for a user.

        Args:
            username: GitHub username

        Returns:
            Dictionary containing GitHub activity metrics
        """
        if self.mock_mode or not self.api_token:
            return self._mock_user_stats(username)

        # Production implementation would go here
        # For now, always use mock
        return self._mock_user_stats(username)

    def _mock_user_stats(self, username: str) -> dict[str, Any]:
        """Generate mock GitHub statistics.

        Uses username hash for deterministic results.
        """
        # Use username for deterministic mock data
        seed = hash(username) % (2**32)
        rng = random.Random(seed)

        # Calculate account age
        years_active = rng.randint(1, 10)
        account_created = datetime.now() - timedelta(days=years_active * 365)

        # Generate language distribution
        all_languages = [
            "Python",
            "JavaScript",
            "TypeScript",
            "Rust",
            "Go",
            "Solidity",
            "Java",
            "C++",
        ]
        num_languages = rng.randint(2, 6)
        languages = rng.sample(all_languages, num_languages)

        # Generate contribution pattern
        total_commits = rng.randint(100, 5000)
        total_repos = rng.randint(5, 100)
        total_stars = rng.randint(0, 500)

        # Consistency score based on activity patterns
        # 1-5 scale: 1=sporadic, 5=very consistent
        consistency_score = rng.randint(1, 5)

        return {
            "username": username,
            "github_commits": total_commits,
            "github_repos": total_repos,
            "github_stars": total_stars,
            "github_years_active": years_active,
            "github_languages": languages,
            "github_consistency_score": consistency_score,
            "account_created": account_created.isoformat(),
            "followers": rng.randint(0, 1000),
            "following": rng.randint(0, 500),
            "public_gists": rng.randint(0, 50),
            "contributions_last_year": rng.randint(50, 1000),
            "_meta": {
                "fetched_at": datetime.now().isoformat(),
                "mock": True,
            },
        }

    def get_repo_stats(self, owner: str, repo: str) -> dict[str, Any]:
        """Fetch statistics for a specific repository.

        Args:
            owner: Repository owner username
            repo: Repository name

        Returns:
            Dictionary containing repository metrics
        """
        if self.mock_mode or not self.api_token:
            return self._mock_repo_stats(owner, repo)

        return self._mock_repo_stats(owner, repo)

    def _mock_repo_stats(self, owner: str, repo: str) -> dict[str, Any]:
        """Generate mock repository statistics."""
        seed = hash(f"{owner}/{repo}") % (2**32)
        rng = random.Random(seed)

        languages = ["Python", "JavaScript", "TypeScript", "Rust"]

        return {
            "owner": owner,
            "name": repo,
            "full_name": f"{owner}/{repo}",
            "stars": rng.randint(0, 1000),
            "forks": rng.randint(0, 200),
            "watchers": rng.randint(0, 100),
            "open_issues": rng.randint(0, 50),
            "primary_language": rng.choice(languages),
            "languages": rng.sample(languages, k=rng.randint(1, 3)),
            "created_at": (datetime.now() - timedelta(days=rng.randint(30, 1000))).isoformat(),
            "updated_at": (datetime.now() - timedelta(days=rng.randint(0, 30))).isoformat(),
            "description": f"A mock repository for {repo}",
            "_meta": {
                "fetched_at": datetime.now().isoformat(),
                "mock": True,
            },
        }

    def _headers(self) -> dict[str, str]:
        """Build request headers with optional auth."""
        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def list_user_repos(
        self,
        username: str,
        per_page: int = 100,
        sort: str = "updated",
        type_: str = "owner",
    ) -> list[dict[str, Any]]:
        """List public repositories for a user.

        Args:
            username: GitHub username
            per_page: Max repos per page (max 100)
            sort: Sort by pushed, created, updated, full_name
            type_: owner, all, member

        Returns:
            List of repo dicts with name, full_name, clone_url, description, etc.
        """
        if self.mock_mode or not self.api_token:
            return self._mock_list_user_repos(username)

        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{GITHUB_API_BASE}/users/{username}/repos",
                headers=self._headers(),
                params={"per_page": per_page, "sort": sort, "type": type_},
            )
            resp.raise_for_status()
            return resp.json()

    def _mock_list_user_repos(self, username: str) -> list[dict[str, Any]]:
        """Mock list of user repos."""
        seed = hash(username) % (2**32)
        rng = random.Random(seed)
        n = rng.randint(3, 8)
        return [
            {
                "name": f"repo-{i}",
                "full_name": f"{username}/repo-{i}",
                "clone_url": f"https://github.com/{username}/repo-{i}.git",
                "description": f"Mock repo {i}",
                "default_branch": "main",
            }
            for i in range(n)
        ]

    def get_repo_languages(self, owner: str, repo: str) -> dict[str, int]:
        """Get language bytes per language for a repository.

        Returns:
            Dict mapping language name to byte count, e.g. {"Python": 125000, "JavaScript": 45000}
        """
        if self.mock_mode or not self.api_token:
            return self._mock_repo_languages(owner, repo)

        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{GITHUB_API_BASE}/repos/{owner}/{repo}/languages",
                headers=self._headers(),
            )
            resp.raise_for_status()
            return resp.json()

    def _mock_repo_languages(self, owner: str, repo: str) -> dict[str, int]:
        """Mock repo languages."""
        seed = hash(f"{owner}/{repo}") % (2**32)
        rng = random.Random(seed)
        langs = ["Python", "JavaScript", "TypeScript", "Rust", "Go"]
        chosen = rng.sample(langs, k=rng.randint(1, 3))
        return {lang: rng.randint(10000, 200000) for lang in chosen}

    def get_repo_tree(
        self,
        owner: str,
        repo: str,
        ref: str = "HEAD",
        recursive: bool = True,
    ) -> list[dict[str, Any]]:
        """Get git tree (file listing) for a repository.

        Args:
            owner: Repo owner
            repo: Repo name
            ref: Branch, tag, or SHA (default HEAD)
            recursive: If True, return full tree recursively

        Returns:
            List of tree entries: {path, type, sha, ...}
        """
        if self.mock_mode or not self.api_token:
            return self._mock_repo_tree(owner, repo)

        with httpx.Client(timeout=30) as client:
            # Resolve HEAD to default branch
            tree_ref = ref
            if ref in ("HEAD", "head"):
                repo_resp = client.get(
                    f"{GITHUB_API_BASE}/repos/{owner}/{repo}",
                    headers=self._headers(),
                )
                repo_resp.raise_for_status()
                tree_ref = repo_resp.json().get("default_branch", "main")

            tree_resp = client.get(
                f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/trees/{tree_ref}",
                headers=self._headers(),
                params={"recursive": "1" if recursive else "0"},
            )
            tree_resp.raise_for_status()
            data = tree_resp.json()
            return data.get("tree", [])

    def _mock_repo_tree(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """Mock repo tree with common files."""
        return [
            {"path": "README.md", "type": "blob", "sha": "abc"},
            {"path": "package.json", "type": "blob", "sha": "def"},
            {"path": "requirements.txt", "type": "blob", "sha": "ghi"},
            {"path": "main.py", "type": "blob", "sha": "jkl"},
        ]

    def get_file_contents(
        self,
        owner: str,
        repo: str,
        path: str,
        ref: str | None = None,
    ) -> str:
        """Get raw file contents from a repository.

        Args:
            owner: Repo owner
            repo: Repo name
            path: File path (e.g. package.json, README.md)
            ref: Optional branch/tag/SHA (default: default branch)

        Returns:
            Decoded file content as string
        """
        if self.mock_mode or not self.api_token:
            return self._mock_file_contents(owner, repo, path)

        params = {}
        if ref:
            params["ref"] = ref

        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{path}",
                headers={**self._headers(), "Accept": "application/vnd.github.raw"},
                params=params or None,
            )
            resp.raise_for_status()
            return resp.text

    def _mock_file_contents(self, owner: str, repo: str, path: str) -> str:
        """Mock file contents."""
        if "package.json" in path:
            return '{"dependencies": {"react": "^18.0.0", "typescript": "^5.0.0"}}'
        if "requirements.txt" in path:
            return "django==4.2\nredis\npsycopg2-binary"
        if "README" in path:
            return "Mock README for skill verification."
        return ""

    def check_rate_limit(self) -> dict[str, Any]:
        """Check current GitHub API rate limit status.

        Returns:
            Dictionary with rate limit information
        """
        if self.mock_mode or not self.api_token:
            return {
                "limit": 5000,
                "remaining": 4999,
                "reset_at": (datetime.now() + timedelta(hours=1)).isoformat(),
                "mock": True,
            }

        with httpx.Client(timeout=10) as client:
            resp = client.get(
                f"{GITHUB_API_BASE}/rate_limit",
                headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            core = data.get("resources", {}).get("core", {})
            return {
                "limit": core.get("limit", 5000),
                "remaining": core.get("remaining", 4999),
                "reset_at": datetime.fromtimestamp(core.get("reset", 0)).isoformat(),
                "mock": False,
            }
