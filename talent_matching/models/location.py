"""Location alias and region-country models for DB-backed location matching."""

import uuid
from datetime import datetime
from uuid import uuid4

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from talent_matching.models.base import Base


class LocationCountryAlias(Base):
    """Alias mapping to a canonical country name.

    Example: "ny" -> "united states", "new york" -> "united states".
    Used for location pre-filter and candidate/job location resolution.
    """

    __tablename__ = "location_country_aliases"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    alias: Mapped[str] = mapped_column(Text, nullable=False)
    country_canonical: Mapped[str] = mapped_column(Text, nullable=False)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    added_by: Mapped[str] = mapped_column(String(50), default="seed", nullable=False)


class LocationRegionCountry(Base):
    """Region-to-country mapping (one row per region, country pair).

    Example: ("europe", "germany"), ("europe", "france").
    Used to expand job "Europe" to allowed countries and to derive
    country -> region for get_region_for_country.
    """

    __tablename__ = "location_region_countries"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    region: Mapped[str] = mapped_column(Text, nullable=False)
    country: Mapped[str] = mapped_column(Text, nullable=False)
