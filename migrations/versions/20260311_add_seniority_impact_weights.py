"""Add impact, seniority scale, level and tenure weights to scoring_weights.

Revision ID: 20260311_seniority_impact
Revises: 20260311_job_cat_prompts
Create Date: 2026-03-11

Adds: impact_weight, technical_weight, seniority_scale_weight,
seniority_level_max_deduction, tenure_instability_max_deduction.
Existing rows get defaults so matchmaking continues to work.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260311_seniority_impact"
down_revision: str | None = "20260311_job_cat_prompts"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "scoring_weights",
        sa.Column("impact_weight", sa.Float(), nullable=True),
    )
    op.add_column(
        "scoring_weights",
        sa.Column("technical_weight", sa.Float(), nullable=True),
    )
    op.add_column(
        "scoring_weights",
        sa.Column("seniority_scale_weight", sa.Float(), nullable=True),
    )
    op.add_column(
        "scoring_weights",
        sa.Column("seniority_level_max_deduction", sa.Float(), nullable=True),
    )
    op.add_column(
        "scoring_weights",
        sa.Column("tenure_instability_max_deduction", sa.Float(), nullable=True),
    )
    # Backfill defaults for existing rows (match config default_weights_dict)
    op.execute(
        """
        UPDATE scoring_weights
        SET impact_weight = 0.15,
            technical_weight = 0.0,
            seniority_scale_weight = 0.05,
            seniority_level_max_deduction = 0.1,
            tenure_instability_max_deduction = 0.1
        WHERE impact_weight IS NULL
        """
    )
    op.alter_column(
        "scoring_weights",
        "impact_weight",
        existing_type=sa.Float(),
        nullable=False,
    )
    op.alter_column(
        "scoring_weights",
        "technical_weight",
        existing_type=sa.Float(),
        nullable=False,
    )
    op.alter_column(
        "scoring_weights",
        "seniority_scale_weight",
        existing_type=sa.Float(),
        nullable=False,
    )
    op.alter_column(
        "scoring_weights",
        "seniority_level_max_deduction",
        existing_type=sa.Float(),
        nullable=False,
    )
    op.alter_column(
        "scoring_weights",
        "tenure_instability_max_deduction",
        existing_type=sa.Float(),
        nullable=False,
    )


def downgrade() -> None:
    op.drop_column("scoring_weights", "tenure_instability_max_deduction")
    op.drop_column("scoring_weights", "seniority_level_max_deduction")
    op.drop_column("scoring_weights", "seniority_scale_weight")
    op.drop_column("scoring_weights", "technical_weight")
    op.drop_column("scoring_weights", "impact_weight")
