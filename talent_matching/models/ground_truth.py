"""Ground-truth outcomes from Airtable ATS (introduced/hired candidates)."""

import uuid
from datetime import datetime
from uuid import uuid4

from sqlalchemy import DateTime, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from talent_matching.models.base import Base


class GroundTruthOutcome(Base):
    """Job-candidate outcome from Airtable ATS columns.

    North-star dataset: (job, candidate) pairs with stage timestamps.
    Synced by ground_truth_sync_sensor from Potential Talent Fit,
    CLIENT INTRODUCTION, and Hired columns.
    """

    __tablename__ = "ground_truth_outcomes"
    __table_args__ = (
        UniqueConstraint(
            "job_airtable_record_id",
            "candidate_airtable_record_id",
            name="uq_ground_truth_job_candidate",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    job_airtable_record_id: Mapped[str] = mapped_column(String(255), nullable=False)
    candidate_airtable_record_id: Mapped[str] = mapped_column(String(255), nullable=False)

    potential_talent_fit_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    client_introduction_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    hired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    source_columns: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
