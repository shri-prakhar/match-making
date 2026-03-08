"""Skills taxonomy models."""

from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from talent_matching.models.base import Base
from talent_matching.models.enums import ReviewStatusEnum


class Skill(Base):
    """Canonical skill in the taxonomy.

    Skills are normalized names that all variants map to.
    Example: "TypeScript" is canonical, "TS", "Typescript" are aliases.
    """

    __tablename__ = "skills"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    airtable_record_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)

    # Skill identity
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)  # "TypeScript"
    slug: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)  # "typescript"
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_by: Mapped[str] = mapped_column(String(50), default="seed")  # seed, llm, manual
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    review_status: Mapped[ReviewStatusEnum] = mapped_column(
        Enum(ReviewStatusEnum, name="review_status_enum"),
        default=ReviewStatusEnum.APPROVED,
    )
    is_requirement: Mapped[bool] = mapped_column(Boolean, default=False)
    """True when this skill was added from a job requirement; false for candidate-facing skills."""

    # Relationships
    aliases: Mapped[list["SkillAlias"]] = relationship(
        "SkillAlias", back_populates="skill", cascade="all, delete-orphan"
    )


class SkillAlias(Base):
    """Alias mapping to a canonical skill.

    Example: "TS" -> TypeScript, "ReactJS" -> React
    """

    __tablename__ = "skill_aliases"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    alias: Mapped[str] = mapped_column(Text, nullable=False)

    # Foreign key to canonical skill
    skill_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("skills.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Metadata
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    added_by: Mapped[str] = mapped_column(String(50), default="manual")  # seed, llm, manual

    # Relationships
    skill: Mapped["Skill"] = relationship("Skill", back_populates="aliases")
