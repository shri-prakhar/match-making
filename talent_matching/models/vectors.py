"""Vector embedding models for semantic search."""

import uuid
from datetime import datetime
from uuid import uuid4

from pgvector.sqlalchemy import Vector
from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from talent_matching.models.base import Base


class CandidateVector(Base):
    """Semantic embeddings for candidate profiles.

    Multiple vectors per candidate for different semantic aspects:
    - position: Individual role/project descriptions
    - experience: Concatenated experience across all roles
    - domain_context: Industries and problem spaces
    - personality: Work style and personality signals
    """

    __tablename__ = "candidate_vectors"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Foreign key to raw candidate
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("raw_candidates.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Vector type and data
    vector_type: Mapped[str] = mapped_column(
        String(150), nullable=False
    )  # 'experience', 'domain', 'skill_python', 'position_0', etc.
    vector: Mapped[list[float]] = mapped_column(
        Vector(1536), nullable=False
    )  # OpenAI text-embedding-3-small dimensions

    # Metadata
    model_version: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class JobVector(Base):
    """Semantic embeddings for job descriptions.

    Multiple vectors per job for different semantic aspects:
    - role_description: Full job responsibilities and day-to-day work
    - domain_context: Product/industry context (DeFi, gaming, enterprise, etc.)
    - culture: Company culture and team working style
    """

    __tablename__ = "job_vectors"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Foreign key to raw job
    job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("raw_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Vector type and data
    vector_type: Mapped[str] = mapped_column(
        String(150), nullable=False
    )  # 'role_description', 'domain', 'skill_react', etc.
    vector: Mapped[list[float]] = mapped_column(
        Vector(1536), nullable=False
    )  # OpenAI text-embedding-3-small dimensions

    # Metadata
    model_version: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
