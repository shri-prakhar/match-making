"""Add scoring_weights table for per-job-category matchmaking weights.

Revision ID: 20260310_scoring_weights
Revises: 20260309_ground_truth
Create Date: 2026-03-10

When a new job category is seen in the matchmaking pipeline, we insert a row
with default weights. No seed rows: categories are created on first use.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260310_scoring_weights"
down_revision: str | None = "20260309_ground_truth"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "scoring_weights",
        sa.Column("job_category", sa.String(length=255), nullable=False),
        sa.Column("role_weight", sa.Float(), nullable=False),
        sa.Column("domain_weight", sa.Float(), nullable=False),
        sa.Column("culture_weight", sa.Float(), nullable=False),
        sa.Column("vector_weight", sa.Float(), nullable=False),
        sa.Column("skill_fit_weight", sa.Float(), nullable=False),
        sa.Column("compensation_weight", sa.Float(), nullable=False),
        sa.Column("location_weight", sa.Float(), nullable=False),
        sa.Column("skill_rating_weight", sa.Float(), nullable=False),
        sa.Column("skill_semantic_weight", sa.Float(), nullable=False),
        sa.Column("seniority_max_deduction", sa.Float(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("job_category", name=op.f("pk_scoring_weights")),
    )


def downgrade() -> None:
    op.drop_table("scoring_weights")
