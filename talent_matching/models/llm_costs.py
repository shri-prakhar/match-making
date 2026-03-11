"""LLM cost tracking models."""

import uuid
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import DateTime, Integer, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from talent_matching.models.base import Base


class LLMCost(Base):
    """Track LLM API costs per asset, run, and operation.

    Enables cost analysis by:
    - Asset (which Dagster asset triggered the call)
    - Operation type (normalize_cv, normalize_job, score, embed)
    - Code version (for A/B testing prompts)
    - Time period (daily/weekly trends)
    """

    __tablename__ = "llm_costs"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Dagster context
    run_id: Mapped[str] = mapped_column(String(100), nullable=False)
    asset_key: Mapped[str] = mapped_column(String(100), nullable=False)
    partition_key: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Operation type
    operation: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # 'normalize_cv', 'normalize_job', 'score', 'embed'

    # Model and tokens
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    # Note: total_tokens would be a generated column in PostgreSQL
    # but SQLAlchemy doesn't support GENERATED ALWAYS AS well
    # We'll compute it in application code or add it in migration

    # Cost
    cost_usd: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)

    # Versioning
    code_version: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Timestamp
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    @property
    def total_tokens(self) -> int:
        """Total tokens (input + output)."""
        return self.input_tokens + self.output_tokens
