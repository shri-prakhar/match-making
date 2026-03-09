"""Add ground_truth_outcomes table for north-star dataset from Airtable ATS.

Revision ID: 20260309_ground_truth
Revises: 20260305_job_status
Create Date: 2026-03-09

- ground_truth_outcomes: (job, candidate) pairs with stage timestamps
  (Potential Talent Fit, CLIENT INTRODUCTION, Hired)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260309_ground_truth"
down_revision: str | None = "20260305_job_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ground_truth_outcomes",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_airtable_record_id", sa.String(length=255), nullable=False),
        sa.Column("candidate_airtable_record_id", sa.String(length=255), nullable=False),
        sa.Column("potential_talent_fit_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("client_introduction_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("hired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("source_columns", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_ground_truth_outcomes")),
        sa.UniqueConstraint(
            "job_airtable_record_id",
            "candidate_airtable_record_id",
            name="uq_ground_truth_job_candidate",
        ),
    )


def downgrade() -> None:
    op.drop_table("ground_truth_outcomes")
