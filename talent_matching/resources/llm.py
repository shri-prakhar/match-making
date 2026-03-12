"""Mock LLM resource for development and testing.

This resource simulates OpenAI-style completion APIs without making actual API calls.
It returns plausible mock data structures matching the expected normalization schemas.
"""

import random
import uuid
from typing import Any

from dagster import ConfigurableResource
from pydantic import Field


class MockLLMResource(ConfigurableResource):
    """Mock LLM resource that simulates OpenAI-style completions.

    In production, this would be replaced with an actual OpenAI or Anthropic resource
    that makes real API calls.
    """

    model_version: str = Field(
        default="mock-v1",
        description="Version identifier for the mock model",
    )
    prompt_version: str = Field(
        default="v1.0.0",
        description="Version identifier for the prompts",
    )

    def normalize_cv(self, raw_cv_text: str) -> dict[str, Any]:
        """Normalize a CV into structured format.

        In production, this would send the CV text to an LLM with a normalization prompt.

        Args:
            raw_cv_text: Raw text extracted from a CV/resume

        Returns:
            Normalized candidate profile as a dictionary
        """
        # Generate mock normalized data
        mock_skills = ["Python", "JavaScript", "Rust", "Solana", "React", "PostgreSQL"]
        selected_skills = random.sample(mock_skills, k=random.randint(2, 5))

        return {
            "name": f"Candidate {uuid.uuid4().hex[:8]}",
            "years_of_experience": random.randint(1, 15),
            "current_role": random.choice(
                [
                    "Senior Software Engineer",
                    "Full Stack Developer",
                    "Backend Engineer",
                    "Blockchain Developer",
                    "DevOps Engineer",
                ]
            ),
            "summary": "Experienced developer with a passion for building scalable systems.",
            "skills": {
                "languages": [s for s in selected_skills if s in ["Python", "JavaScript", "Rust"]],
                "frameworks": [s for s in selected_skills if s in ["React", "Solana"]],
                "tools": [s for s in selected_skills if s in ["PostgreSQL"]],
                "domains": random.sample(["DeFi", "NFT", "Infrastructure", "Trading"], k=2),
            },
            "experience": [
                {
                    "company": random.choice(
                        ["Acme Corp", "TechStart", "BlockChain Labs", "DeFi Protocol"]
                    ),
                    "role": "Software Engineer",
                    "duration_months": random.randint(6, 36),
                    "description": "Built and maintained backend services.",
                    "technologies": selected_skills[:2],
                }
            ],
            "education": [
                {
                    "institution": "University of Technology",
                    "degree": "BS Computer Science",
                    "year": random.randint(2010, 2022),
                }
            ],
            "notable_achievements": [
                "Hackathon winner",
                "Open source contributor",
            ],
            "_meta": {
                "model_version": self.model_version,
                "prompt_version": self.prompt_version,
            },
        }

    def normalize_job(self, raw_job_text: str) -> dict[str, Any]:
        """Normalize a job description into structured format.

        Args:
            raw_job_text: Raw job description text

        Returns:
            Normalized job requirements as a dictionary
        """
        mock_skills = ["Python", "JavaScript", "Rust", "Solana", "React", "PostgreSQL", "AWS"]
        must_have = random.sample(mock_skills, k=random.randint(2, 4))
        nice_to_have = random.sample([s for s in mock_skills if s not in must_have], k=2)

        return {
            "title": random.choice(
                [
                    "Senior Backend Engineer",
                    "Full Stack Developer",
                    "Blockchain Engineer",
                    "Platform Engineer",
                ]
            ),
            "seniority_level": random.choice(["junior", "mid", "senior", "lead"]),
            "employment_type": "full-time",
            "requirements": {
                "must_have_skills": must_have,
                "nice_to_have_skills": nice_to_have,
                "years_of_experience_min": random.randint(2, 5),
                "years_of_experience_max": random.randint(6, 10),
                "education_required": None,
                "domain_experience": random.sample(
                    ["DeFi", "NFT", "Trading", "Infrastructure"], k=2
                ),
            },
            "role_description": "Join our team to build the next generation of blockchain applications.",
            "team_context": "Small, fast-moving team of 5-10 engineers.",
            "tech_stack": must_have + nice_to_have,
            "compensation": {
                "salary_min": random.randint(100, 150) * 1000,
                "salary_max": random.randint(150, 250) * 1000,
                "currency": "USD",
                "equity": random.choice([True, False]),
            },
            "location": {
                "type": random.choice(["remote", "hybrid", "onsite"]),
                "locations": ["San Francisco", "Remote"],
            },
            "_meta": {
                "model_version": self.model_version,
                "prompt_version": self.prompt_version,
            },
        }

    def score_candidate(self, normalized_profile: dict[str, Any]) -> dict[str, Any]:
        """Generate scores for a candidate profile.

        Args:
            normalized_profile: Normalized candidate profile

        Returns:
            Scoring breakdown with individual metric scores
        """
        return {
            "overall_score": round(random.uniform(0.5, 0.95), 3),
            "experience_score": round(random.uniform(0.4, 1.0), 3),
            "skills_score": round(random.uniform(0.5, 1.0), 3),
            "domain_score": round(random.uniform(0.3, 1.0), 3),
            "reasoning": "Mock scoring - candidate shows strong potential based on experience.",
            "_meta": {
                "model_version": self.model_version,
                "prompt_version": self.prompt_version,
            },
        }
