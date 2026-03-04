"""Add skill verification columns and candidate_github_commit_history table.

Revision ID: 20260304_skill_verif
Revises: 20260303_llm_fit
Create Date: 2026-03-04

- candidate_skills: verification_status, verification_evidence, verified_at
- normalized_candidates: skill_verification_score
- candidate_github_commit_history: new table for blobless clone commit data
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260304_skill_verif"
down_revision: str | None = "20260303_llm_fit"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Create skill_verification_status enum type first
    op.execute(
        "CREATE TYPE skill_verification_status_enum AS ENUM ('verified', 'unverified', 'no_evidence', 'skipped')"
    )

    # Add columns to candidate_skills
    op.add_column(
        "candidate_skills",
        sa.Column(
            "verification_status",
            sa.Enum(
                "verified",
                "unverified",
                "no_evidence",
                "skipped",
                name="skill_verification_status_enum",
            ),
            nullable=True,
        ),
    )
    op.add_column(
        "candidate_skills",
        sa.Column("verification_evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "candidate_skills",
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Add skill_verification_score to normalized_candidates
    op.add_column(
        "normalized_candidates",
        sa.Column("skill_verification_score", sa.Float(), nullable=True),
    )

    # Create candidate_github_commit_history table
    op.create_table(
        "candidate_github_commit_history",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("airtable_record_id", sa.String(255), nullable=True),
        sa.Column("candidate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("github_username", sa.Text(), nullable=False),
        sa.Column("commit_history", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["normalized_candidates.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "airtable_record_id", name="uq_candidate_github_commit_history_airtable_record_id"
        ),
        sa.UniqueConstraint("candidate_id", name="uq_candidate_github_commit_history"),
    )


def downgrade() -> None:
    op.drop_table("candidate_github_commit_history")
    op.drop_column("normalized_candidates", "skill_verification_score")
    op.drop_column("candidate_skills", "verified_at")
    op.drop_column("candidate_skills", "verification_evidence")
    op.drop_column("candidate_skills", "verification_status")
    sa.Enum(
        "verified", "unverified", "no_evidence", "skipped", name="skill_verification_status_enum"
    ).drop(op.get_bind(), checkfirst=True)
