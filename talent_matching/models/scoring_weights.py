"""Scoring weights per job category for matchmaking.

Stored in DB so we can create a record with default weights when a new job category
is first seen in the pipeline, and later support ML tuning per category.
"""

from datetime import datetime

from sqlalchemy import DateTime, Float, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from talent_matching.models.base import Base


class ScoringWeightsRecord(Base):
    """One row per job category: tunable matchmaking weights.

    When a new job category is encountered in the matchmaking pipeline, we insert
    a row with default weights. ML or manual tuning can update these values later.
    match_category_aliases: when set, jobs with this category also match candidates
    who have any of these categories in desired_job_categories (e.g. Compliance -> [Operations, Legal]).
    """

    __tablename__ = "scoring_weights"

    job_category: Mapped[str] = mapped_column(String(255), primary_key=True)
    match_category_aliases: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)

    # Vector similarity sub-weights (role, domain, culture, impact, technical)
    role_weight: Mapped[float] = mapped_column(Float, nullable=False)
    domain_weight: Mapped[float] = mapped_column(Float, nullable=False)
    culture_weight: Mapped[float] = mapped_column(Float, nullable=False)
    impact_weight: Mapped[float] = mapped_column(Float, nullable=False)
    technical_weight: Mapped[float] = mapped_column(Float, nullable=False)
    # Combined score blend
    vector_weight: Mapped[float] = mapped_column(Float, nullable=False)
    skill_fit_weight: Mapped[float] = mapped_column(Float, nullable=False)
    compensation_weight: Mapped[float] = mapped_column(Float, nullable=False)
    location_weight: Mapped[float] = mapped_column(Float, nullable=False)
    seniority_scale_weight: Mapped[float] = mapped_column(Float, nullable=False)
    # Skill fit sub-weights
    skill_rating_weight: Mapped[float] = mapped_column(Float, nullable=False)
    skill_semantic_weight: Mapped[float] = mapped_column(Float, nullable=False)
    seniority_max_deduction: Mapped[float] = mapped_column(Float, nullable=False)
    seniority_level_max_deduction: Mapped[float] = mapped_column(Float, nullable=False)
    tenure_instability_max_deduction: Mapped[float] = mapped_column(Float, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
