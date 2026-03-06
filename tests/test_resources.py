"""Tests for Dagster resources."""

from talent_matching.resources import GitHubAPIResource


class TestGitHubAPIResource:
    """Tests for the GitHubAPIResource."""

    def test_fallback_returns_data_when_no_token(self):
        """Test that without token, fallback returns plausible data."""
        resource = GitHubAPIResource(api_token="")
        result = resource.get_user_stats("testuser")

        assert result["username"] == "testuser"
        assert "github_commits" in result
        assert "github_repos" in result
        assert "github_languages" in result
        assert result["_meta"]["mock"] is True

    def test_deterministic_fallback_data(self):
        """Test that same username gives same fallback data."""
        resource = GitHubAPIResource(api_token="")

        result1 = resource.get_user_stats("consistent_user")
        result2 = resource.get_user_stats("consistent_user")

        assert result1["github_commits"] == result2["github_commits"]
        assert result1["github_repos"] == result2["github_repos"]

    def test_rate_limit_check(self):
        """Test rate limit checking."""
        resource = GitHubAPIResource(api_token="")
        result = resource.check_rate_limit()

        assert "limit" in result
        assert "remaining" in result
