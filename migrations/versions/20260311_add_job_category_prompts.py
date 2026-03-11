"""Add job_category_prompts table for storable, dynamic per-category prompts.

Revision ID: 20260311_job_cat_prompts
Revises: 20260310_scoring_weights
Create Date: 2026-03-11

Stores cv_extraction_prompt and refinement_prompt per job category.
When no row exists, callers use in-code default (same pattern as scoring_weights).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260311_job_cat_prompts"
down_revision: str | None = "20260310_scoring_weights"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "job_category_prompts",
        sa.Column("job_category", sa.String(length=255), nullable=False),
        sa.Column("cv_extraction_prompt", sa.Text(), nullable=True),
        sa.Column("refinement_prompt", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("job_category", name=op.f("pk_job_category_prompts")),
    )


def downgrade() -> None:
    op.drop_table("job_category_prompts")
