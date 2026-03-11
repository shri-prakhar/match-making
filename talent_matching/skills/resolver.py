"""Centralized skill resolution: alias lookup, get-or-create, and vector key generation.

All skill-related lookups and mutations should go through this module to ensure
consistent alias resolution, slug generation, and vector key naming.
"""

import uuid
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from talent_matching.models.enums import ReviewStatusEnum
from talent_matching.models.skills import Skill, SkillAlias


def load_alias_map(session: Session) -> dict[str, str]:
    """Load mapping of alias name -> canonical skill name from skill_aliases."""
    rows = session.execute(
        select(SkillAlias.alias, Skill.name).join(Skill, SkillAlias.skill_id == Skill.id)
    ).all()
    return {alias: canonical for alias, canonical in rows}


def resolve_skill_name(name: str, alias_map: dict[str, str]) -> str:
    """Resolve a skill name to its canonical form, falling back to the original."""
    return alias_map.get(name, name)


def skill_vector_key(skill_name: str) -> str:
    """Generate a consistent vector key for a skill name.

    Used by both candidate_vectors and job_vectors to ensure keys align
    for cosine similarity lookups during matchmaking.
    """
    return f"skill_{skill_name.lower().replace(' ', '_').replace('.', '')}"[:150]


def _make_slug(name: str) -> str:
    return name.lower().strip().replace(" ", "-").replace(".", "")[:100]


def get_or_create_skill(
    session: Session,
    skill_name: str,
    *,
    created_by: str = "llm",
    is_requirement: bool = False,
) -> UUID | None:
    """Get existing skill ID or create a new one, resolving aliases first.

    Resolution order:
    1. Check if the name is a known alias -> return the canonical skill's ID.
    2. Look up by slug or name (case-insensitive) -> return existing ID.
    3. Create a new Skill row via upsert and return its ID.
    """
    name = skill_name.strip()
    if not name:
        return None
    slug = _make_slug(name)

    alias_skill_id = (
        session.execute(
            select(SkillAlias.skill_id).where(func.lower(SkillAlias.alias) == name.lower())
        )
        .scalars()
        .first()
    )
    if alias_skill_id is not None:
        return cast(uuid.UUID, alias_skill_id)

    existing = (
        session.execute(select(Skill).where((Skill.slug == slug) | (Skill.name.ilike(name))))
        .scalars()
        .first()
    )
    if existing:
        return cast(uuid.UUID, existing.id)

    stmt = insert(Skill).values(
        id=uuid4(),
        name=name,
        slug=slug,
        created_by=created_by,
        is_active=True,
        review_status=ReviewStatusEnum.PENDING,
        is_requirement=is_requirement,
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["slug"])
    session.execute(stmt)
    session.flush()

    created = session.execute(select(Skill.id).where(Skill.slug == slug)).scalar_one_or_none()
    return cast(uuid.UUID | None, created)
