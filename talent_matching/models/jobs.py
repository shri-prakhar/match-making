"""Normalized job models."""

import uuid
from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from talent_matching.models.base import Base
from talent_matching.models.enums import (
    CompanyStageEnum,
    EmploymentTypeEnum,
    JobStatusEnum,
    LocationTypeEnum,
    RequirementTypeEnum,
    SeniorityEnum,
)


class NormalizedJob(Base):
    """LLM-normalized job posting."""

    __tablename__ = "normalized_jobs"
    __table_args__ = (
        CheckConstraint(
            "min_leadership_score IS NULL OR (min_leadership_score >= 1 AND min_leadership_score <= 5)",
            name="ck_job_leadership_range",
        ),
        CheckConstraint(
            "min_autonomy_score IS NULL OR (min_autonomy_score >= 1 AND min_autonomy_score <= 5)",
            name="ck_job_autonomy_range",
        ),
        CheckConstraint(
            "min_technical_depth_score IS NULL OR (min_technical_depth_score >= 1 AND min_technical_depth_score <= 5)",
            name="ck_job_technical_depth_range",
        ),
        CheckConstraint(
            "min_communication_score IS NULL OR (min_communication_score >= 1 AND min_communication_score <= 5)",
            name="ck_job_communication_range",
        ),
        CheckConstraint(
            "min_growth_trajectory_score IS NULL OR (min_growth_trajectory_score >= 1 AND min_growth_trajectory_score <= 5)",
            name="ck_job_growth_trajectory_range",
        ),
        CheckConstraint(
            "priority >= 1 AND priority <= 4",
            name="ck_job_priority_range",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    airtable_record_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)
    raw_job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("raw_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )

    # ═══════════════════════════════════════════════════════════════════
    # JOB IDENTITY
    # ═══════════════════════════════════════════════════════════════════
    job_title: Mapped[str] = mapped_column(Text, nullable=False)
    job_category: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # Engineering, Design, etc.
    role_type: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # engineering, devrel, growth, product, design

    # ═══════════════════════════════════════════════════════════════════
    # COMPANY INFO
    # ═══════════════════════════════════════════════════════════════════
    company_name: Mapped[str] = mapped_column(Text, nullable=False)
    company_stage: Mapped[CompanyStageEnum | None] = mapped_column(
        Enum(CompanyStageEnum, name="company_stage_enum"), nullable=True
    )
    company_size: Mapped[str | None] = mapped_column(Text, nullable=True)
    company_website: Mapped[str | None] = mapped_column(Text, nullable=True)
    company_x_url: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # Company X/Twitter profile

    # ═══════════════════════════════════════════════════════════════════
    # JOB DETAILS
    # ═══════════════════════════════════════════════════════════════════
    job_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    role_summary: Mapped[str | None] = mapped_column(Text, nullable=True)  # 2-3 sentence summary
    responsibilities: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    nice_to_haves: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    benefits: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    team_context: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # Team size, reporting structure

    # ═══════════════════════════════════════════════════════════════════
    # REQUIREMENTS
    # ═══════════════════════════════════════════════════════════════════
    seniority_level: Mapped[SeniorityEnum | None] = mapped_column(
        Enum(SeniorityEnum, name="seniority_enum"), nullable=True
    )
    education_required: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # e.g., "Bachelor's in CS or equivalent"
    domain_experience: Mapped[list[str] | None] = mapped_column(
        ARRAY(Text), nullable=True
    )  # e.g., ['DeFi', 'Trading', 'Infrastructure']
    tech_stack: Mapped[list[str] | None] = mapped_column(
        ARRAY(Text), nullable=True
    )  # Specific technologies

    # ═══════════════════════════════════════════════════════════════════
    # LOCATION & WORK TYPE
    # ═══════════════════════════════════════════════════════════════════
    location_type: Mapped[LocationTypeEnum | None] = mapped_column(
        Enum(LocationTypeEnum, name="location_type_enum"), nullable=True
    )
    locations: Mapped[list[str] | None] = mapped_column(
        ARRAY(Text), nullable=True
    )  # For hybrid/onsite or allowed countries
    timezone_requirements: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # e.g., "UTC-5 to UTC+3"
    employment_type: Mapped[list[EmploymentTypeEnum] | None] = mapped_column(
        ARRAY(Enum(EmploymentTypeEnum, name="employment_type_enum", create_constraint=False)),
        nullable=True,
    )

    # ═══════════════════════════════════════════════════════════════════
    # EXPERIENCE REQUIREMENTS
    # ═══════════════════════════════════════════════════════════════════
    min_years_experience: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_years_experience: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # COMPENSATION
    # ═══════════════════════════════════════════════════════════════════
    salary_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    salary_max: Mapped[int | None] = mapped_column(Integer, nullable=True)
    salary_currency: Mapped[str] = mapped_column(String(10), default="USD")
    has_equity: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    equity_details: Mapped[str | None] = mapped_column(Text, nullable=True)
    has_token_compensation: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # SOFT ATTRIBUTE REQUIREMENTS (minimum scores, NULL = not required)
    # LLM-inferred from job description signals
    # ═══════════════════════════════════════════════════════════════════
    min_leadership_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_autonomy_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_technical_depth_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_communication_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_growth_trajectory_score: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # STATUS & PRIORITY
    # ═══════════════════════════════════════════════════════════════════
    status: Mapped[JobStatusEnum] = mapped_column(
        Enum(JobStatusEnum, name="job_status_enum"), default=JobStatusEnum.ACTIVE
    )
    priority: Mapped[int] = mapped_column(Integer, default=3)  # 1=urgent, 2=high, 3=normal, 4=low
    posted_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    deadline_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    is_urgent: Mapped[bool] = mapped_column(Boolean, default=False)

    # ═══════════════════════════════════════════════════════════════════
    # CONTACT
    # ═══════════════════════════════════════════════════════════════════
    hiring_manager_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    hiring_manager_email: Mapped[str | None] = mapped_column(Text, nullable=True)
    application_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # PROCESSING METADATA
    # ═══════════════════════════════════════════════════════════════════
    prompt_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    model_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    normalized_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    normalized_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # NARRATIVE PROSE (for vector matching with candidate vectors)
    # ═══════════════════════════════════════════════════════════════════
    narrative_experience: Mapped[str | None] = mapped_column(Text, nullable=True)
    narrative_domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    narrative_personality: Mapped[str | None] = mapped_column(Text, nullable=True)
    narrative_impact: Mapped[str | None] = mapped_column(Text, nullable=True)
    narrative_technical: Mapped[str | None] = mapped_column(Text, nullable=True)
    narrative_role: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    required_skills: Mapped[list["JobRequiredSkill"]] = relationship(
        "JobRequiredSkill", back_populates="job", cascade="all, delete-orphan"
    )


class JobRequiredSkill(Base):
    """Skill requirement for a job.

    Maps to Linked Records in Airtable.
    """

    __tablename__ = "job_required_skills"
    __table_args__ = (
        UniqueConstraint("job_id", "skill_id", name="uq_job_skill"),
        CheckConstraint("min_years IS NULL OR min_years >= 0", name="ck_min_years_positive"),
        CheckConstraint(
            "min_level IS NULL OR (min_level >= 1 AND min_level <= 10)",
            name="ck_min_level_range",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    airtable_record_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)

    # Foreign keys
    job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("normalized_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    skill_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("skills.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Requirement type
    requirement_type: Mapped[RequirementTypeEnum] = mapped_column(
        Enum(RequirementTypeEnum, name="requirement_type_enum"),
        default=RequirementTypeEnum.MUST_HAVE,
    )
    min_years: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    expected_capability: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    job: Mapped["NormalizedJob"] = relationship("NormalizedJob", back_populates="required_skills")
    skill: Mapped["Skill"] = relationship("Skill")


# Import Skill for type hints
from talent_matching.models.skills import Skill  # noqa: E402
