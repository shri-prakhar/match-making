"""Add match_category_aliases to scoring_weights for job category match expansion.

Revision ID: 20260313_match_cat_aliases
Revises: 20260312_norm_input_hash
Create Date: 2026-03-13

When set, a job with job_category X also matches candidates who have any of
match_category_aliases in their desired_job_categories (e.g. Compliance -> [Operations, Legal]).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260313_match_cat_aliases"
down_revision: str | None = "20260312_norm_input_hash"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "scoring_weights",
        sa.Column("match_category_aliases", postgresql.ARRAY(sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("scoring_weights", "match_category_aliases")
