"""Per-job-category prompts for CV extraction and LLM refinement.

Stored in DB so prompts can be edited without code deploys and support
future AI self-improvement loops. When no row exists, callers use
in-code default (same pattern as get_weights_for_job_category).
"""

from datetime import datetime

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from talent_matching.models.base import Base


class JobCategoryPromptsRecord(Base):
    """One row per job category: cv_extraction_prompt and refinement_prompt.

    Missing or null column means use in-code default for that prompt type.
    """

    __tablename__ = "job_category_prompts"

    job_category: Mapped[str] = mapped_column(String(255), primary_key=True)
    cv_extraction_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    refinement_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
