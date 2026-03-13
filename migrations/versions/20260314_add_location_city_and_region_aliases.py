"""Add location_city_aliases and location_region_aliases for city/region slug matching.

Revision ID: 20260314_city_region_aliases
Revises: 20260314_location_aliases
Create Date: 2026-03-14

City: New York, NYC, NY -> one canonical city slug. Region: Europe, EU -> one canonical region.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260314_city_region_aliases"
down_revision: str | None = "20260314_location_aliases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "location_city_aliases",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.Column("city_canonical", sa.Text(), nullable=False),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("added_by", sa.String(length=50), nullable=False, server_default="llm"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_location_city_aliases")),
    )
    op.create_index(
        "ix_location_city_aliases_alias_lower",
        "location_city_aliases",
        [sa.text("lower(alias)")],
        unique=True,
    )

    op.create_table(
        "location_region_aliases",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.Column("region_canonical", sa.Text(), nullable=False),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("added_by", sa.String(length=50), nullable=False, server_default="llm"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_location_region_aliases")),
    )
    op.create_index(
        "ix_location_region_aliases_alias_lower",
        "location_region_aliases",
        [sa.text("lower(alias)")],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_location_region_aliases_alias_lower",
        table_name="location_region_aliases",
    )
    op.drop_table("location_region_aliases")
    op.drop_index(
        "ix_location_city_aliases_alias_lower",
        table_name="location_city_aliases",
    )
    op.drop_table("location_city_aliases")
