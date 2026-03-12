"""Add normalization_input_hash to raw_candidates for candidate sensor skip logic.

Revision ID: 20260312_norm_input_hash
Revises: 20260311_seniority_impact
Create Date: 2026-03-12

Used by the candidate pipeline sensor to avoid retriggering when the only
Airtable change was (N) write-back columns (hash of normalization-input fields only).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260312_norm_input_hash"
down_revision: str | None = "20260311_seniority_impact"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "raw_candidates",
        sa.Column("normalization_input_hash", sa.String(32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("raw_candidates", "normalization_input_hash")
