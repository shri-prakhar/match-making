"""Add llm_fit_score to matches (1-10 from LLM refinement stage).

Revision ID: 20260303_llm_fit
Revises: 20260226_proj_sal
Create Date: 2026-03-03

- matches.llm_fit_score (Integer, nullable, 1-10)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260303_llm_fit"
down_revision: str | None = "20260226_proj_sal"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "matches",
        sa.Column("llm_fit_score", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("matches", "llm_fit_score")
