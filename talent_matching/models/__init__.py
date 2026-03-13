"""SQLAlchemy models for the Talent Matching database."""

from talent_matching.models.base import Base
from talent_matching.models.candidates import (
    CandidateAttribute,
    CandidateExperience,
    CandidateGithubCommitHistory,
    CandidateGithubMetrics,
    CandidateLinkedinMetrics,
    CandidateProject,
    CandidateRoleFitness,
    CandidateSkill,
    CandidateTwitterMetrics,
    NormalizedCandidate,
)
from talent_matching.models.enums import (
    PROFICIENCY_LEVELS,
    CompanyStageEnum,
    CVExtractionMethodEnum,
    EmploymentTypeEnum,
    JobStatusEnum,
    LocationTypeEnum,
    MatchStatusEnum,
    ProcessingStatusEnum,
    RequirementTypeEnum,
    ReviewStatusEnum,
    SeniorityEnum,
    SkillVerificationStatusEnum,
    VerificationStatusEnum,
    proficiency_scale_for_prompt,
)
from talent_matching.models.ground_truth import GroundTruthOutcome
from talent_matching.models.job_category_prompts import JobCategoryPromptsRecord
from talent_matching.models.jobs import JobRequiredSkill, NormalizedJob
from talent_matching.models.llm_costs import LLMCost
from talent_matching.models.location import (
    LocationCityAlias,
    LocationCountryAlias,
    LocationRegionAlias,
    LocationRegionCountry,
)
from talent_matching.models.location_timezones import LocationTimezone
from talent_matching.models.matches import Match
from talent_matching.models.raw import RawCandidate, RawJob
from talent_matching.models.scoring_weights import ScoringWeightsRecord
from talent_matching.models.skills import Skill, SkillAlias
from talent_matching.models.vectors import CandidateVector, JobVector

__all__ = [
    # Base
    "Base",
    # Constants
    "PROFICIENCY_LEVELS",
    "proficiency_scale_for_prompt",
    # Enums
    "ProcessingStatusEnum",
    "CVExtractionMethodEnum",
    "SeniorityEnum",
    "VerificationStatusEnum",
    "SkillVerificationStatusEnum",
    "ReviewStatusEnum",
    "CompanyStageEnum",
    "EmploymentTypeEnum",
    "LocationTypeEnum",
    "RequirementTypeEnum",
    "JobStatusEnum",
    "MatchStatusEnum",
    # Raw tables
    "RawCandidate",
    "RawJob",
    # Scoring weights
    "ScoringWeightsRecord",
    # Job category prompts
    "JobCategoryPromptsRecord",
    # Skills
    "Skill",
    "SkillAlias",
    # Candidates
    "NormalizedCandidate",
    "CandidateSkill",
    "CandidateExperience",
    "CandidateProject",
    "CandidateAttribute",
    "CandidateRoleFitness",
    "CandidateGithubMetrics",
    "CandidateGithubCommitHistory",
    "CandidateTwitterMetrics",
    "CandidateLinkedinMetrics",
    # Jobs
    "NormalizedJob",
    "JobRequiredSkill",
    # Matches
    "Match",
    # Ground Truth
    "GroundTruthOutcome",
    # Vectors
    "CandidateVector",
    "JobVector",
    # Location aliases and region-countries (DB-backed location matching)
    "LocationCityAlias",
    "LocationCountryAlias",
    "LocationRegionAlias",
    "LocationRegionCountry",
    # Location Timezones
    "LocationTimezone",
    # LLM Costs
    "LLMCost",
]
