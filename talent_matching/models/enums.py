"""Database enums for the Talent Matching schema."""

import enum


class ProcessingStatusEnum(str, enum.Enum):
    """Status of raw data processing."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class CVExtractionMethodEnum(str, enum.Enum):
    """How CV text was obtained."""

    AIRTABLE = "airtable"  # Text was already in Airtable
    PDF_TEXT = "pdf_text"  # Extracted via OpenRouter pdf-text engine (free)
    MISTRAL_OCR = "mistral_ocr"  # Extracted via OpenRouter mistral-ocr ($2/1000 pages)
    NATIVE = "native"  # Model's native PDF processing
    FAILED = "failed"  # Extraction attempted but failed


class SeniorityEnum(str, enum.Enum):
    """Career seniority levels."""

    JUNIOR = "junior"
    MID = "mid"
    SENIOR = "senior"
    STAFF = "staff"
    LEAD = "lead"
    PRINCIPAL = "principal"
    EXECUTIVE = "executive"


class VerificationStatusEnum(str, enum.Enum):
    """Candidate verification status."""

    UNVERIFIED = "unverified"
    VERIFIED = "verified"
    SUSPICIOUS = "suspicious"
    FRAUDULENT = "fraudulent"


class SkillVerificationStatusEnum(str, enum.Enum):
    """Per-skill GitHub verification status."""

    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    NO_EVIDENCE = "no_evidence"
    SKIPPED = "skipped"


class ReviewStatusEnum(str, enum.Enum):
    """Review status for skills and other entities."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class CompanyStageEnum(str, enum.Enum):
    """Company growth stage."""

    STARTUP = "startup"
    SCALEUP = "scaleup"
    ENTERPRISE = "enterprise"
    DAO = "dao"


class EmploymentTypeEnum(str, enum.Enum):
    """Type of employment."""

    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CONTRACT = "contract"


class LocationTypeEnum(str, enum.Enum):
    """Work location type."""

    REMOTE = "remote"
    HYBRID = "hybrid"
    ONSITE = "onsite"


class RequirementTypeEnum(str, enum.Enum):
    """Skill requirement type for jobs."""

    MUST_HAVE = "must_have"
    NICE_TO_HAVE = "nice_to_have"


class JobStatusEnum(str, enum.Enum):
    """Job posting status."""

    ACTIVE = "active"
    PAUSED = "paused"
    FILLED = "filled"
    CLOSED = "closed"


class MatchStatusEnum(str, enum.Enum):
    """Match review status."""

    MATCHED = "matched"
    REVIEWED = "reviewed"
    CONTACTED = "contacted"
    REJECTED = "rejected"
    HIRED = "hired"


# Canonical 1-10 proficiency scale shared across LLM prompts and scoring.
# Candidate skill ratings and job min_level both use this scale.
PROFICIENCY_LEVELS: dict[int, str] = {
    1: "Novice",
    2: "Beginner",
    3: "Elementary",
    4: "Developing",
    5: "Competent",
    6: "Proficient",
    7: "Advanced",
    8: "Expert",
    9: "Master",
    10: "World-class",
}


def proficiency_scale_for_prompt() -> str:
    """Format the proficiency scale as a string suitable for inclusion in LLM prompts."""
    return ", ".join(f"{k}={v}" for k, v in PROFICIENCY_LEVELS.items())
