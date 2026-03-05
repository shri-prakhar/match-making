"""Add talent Job Status for Fraud filtering.

Revision ID: 20260305_job_status
Revises: 20260304_skill_verif
Create Date: 2026-03-05

- raw_candidates: job_status_raw (from Airtable Job Status)
- normalized_candidates: job_status (pass-through for matchmaking filter)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260305_job_status"
down_revision: str | None = "20260304_skill_verif"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "raw_candidates",
        sa.Column("job_status_raw", sa.Text(), nullable=True),
    )
    op.add_column(
        "normalized_candidates",
        sa.Column("job_status", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("normalized_candidates", "job_status")
    op.drop_column("raw_candidates", "job_status_raw")
