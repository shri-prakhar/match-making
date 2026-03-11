"""Location timezone lookup table.

Caches resolved (city, country) -> IANA timezone mappings to avoid
redundant LLM calls. Many candidates share the same city/country.
"""

import uuid
from datetime import datetime
from uuid import uuid4

from sqlalchemy import DateTime, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from talent_matching.models.base import Base


class LocationTimezone(Base):
    __tablename__ = "location_timezones"
    __table_args__ = (
        UniqueConstraint("city", "country", name="uq_location_timezones_city_country"),
    )

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    country: Mapped[str] = mapped_column(Text, nullable=False)
    region: Mapped[str | None] = mapped_column(Text, nullable=True)
    timezone: Mapped[str] = mapped_column(String(50), nullable=False)
    utc_offset: Mapped[str | None] = mapped_column(String(10), nullable=True)
    confidence: Mapped[str] = mapped_column(String(10), nullable=False, default="high")
    resolved_by: Mapped[str] = mapped_column(String(20), nullable=False, default="llm")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
