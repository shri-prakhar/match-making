"""Add location_country_aliases and location_region_countries for DB-backed location matching.

Revision ID: 20260314_location_aliases
Revises: 20260313_match_cat_aliases
Create Date: 2026-03-14

Replaces hardcoded COUNTRY_ALIASES and REGION_COUNTRIES in location_filter;
tables are seeded via LLM-driven seed script/job (empty at migration time).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260314_location_aliases"
down_revision: str | None = "20260313_match_cat_aliases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "location_country_aliases",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.Column("country_canonical", sa.Text(), nullable=False),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("added_by", sa.String(length=50), nullable=False, server_default="seed"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_location_country_aliases")),
    )
    op.create_index(
        "ix_location_country_aliases_alias_lower",
        "location_country_aliases",
        [sa.text("lower(alias)")],
        unique=True,
    )

    op.create_table(
        "location_region_countries",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("region", sa.Text(), nullable=False),
        sa.Column("country", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_location_region_countries")),
        sa.UniqueConstraint(
            "region",
            "country",
            name=op.f("uq_location_region_countries_region_country"),
        ),
    )


def downgrade() -> None:
    op.drop_table("location_region_countries")
    op.drop_index(
        "ix_location_country_aliases_alias_lower",
        table_name="location_country_aliases",
    )
    op.drop_table("location_country_aliases")
