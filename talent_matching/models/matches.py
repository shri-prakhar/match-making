"""Matching results models."""

import uuid
from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    CheckConstraint,
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
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from talent_matching.models.base import Base
from talent_matching.models.enums import MatchStatusEnum


class Match(Base):
    """Candidate-job match with scoring.

    Maps to Linked Records in Airtable.
    """

    __tablename__ = "matches"
    __table_args__ = (
        UniqueConstraint("candidate_id", "job_id", name="uq_candidate_job"),
        CheckConstraint("match_score >= 0 AND match_score <= 1", name="ck_match_score_range"),
    )

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    airtable_record_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)

    # Foreign keys
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("normalized_candidates.id", ondelete="CASCADE"),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("normalized_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )

    # ═══════════════════════════════════════════════════════════════════
    # MATCH SCORING
    # ═══════════════════════════════════════════════════════════════════
    match_score: Mapped[float] = mapped_column(Float, nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Rank among job's matches

    # ═══════════════════════════════════════════════════════════════════
    # SCORE BREAKDOWN
    # ═══════════════════════════════════════════════════════════════════
    skills_match_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    experience_match_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    compensation_match_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    location_match_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Vector similarity components (0–1 each); Notion formula: role 40%, domain 35%, culture 25%
    role_similarity_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    domain_similarity_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    culture_similarity_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # ANALYSIS
    # ═══════════════════════════════════════════════════════════════════
    matching_skills: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    missing_skills: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    match_reasoning: Mapped[str | None] = mapped_column(Text, nullable=True)
    red_flags: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    strengths: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    llm_fit_score: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )  # 1-10 from LLM refinement stage

    # ═══════════════════════════════════════════════════════════════════
    # STATUS & WORKFLOW
    # ═══════════════════════════════════════════════════════════════════
    status: Mapped[MatchStatusEnum] = mapped_column(
        Enum(MatchStatusEnum, name="match_status_enum"), default=MatchStatusEnum.MATCHED
    )
    reviewer_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_by: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # PROCESSING METADATA
    # ═══════════════════════════════════════════════════════════════════
    matched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    algorithm_version: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Relationships
    candidate: Mapped["NormalizedCandidate"] = relationship("NormalizedCandidate")
    job: Mapped["NormalizedJob"] = relationship("NormalizedJob")


# Import for type hints
from talent_matching.models.candidates import NormalizedCandidate  # noqa: E402
from talent_matching.models.jobs import NormalizedJob  # noqa: E402
