"""Skill normalization job: assign unprocessed skills to alias bags via LLM."""

import asyncio
from uuid import UUID

from dagster import OpExecutionContext, ScheduleDefinition, job, op
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from talent_matching.db import get_session
from talent_matching.jobs.asset_jobs import openrouter_retry_policy
from talent_matching.llm.operations.normalize_skills import (
    assign_skills_to_bags,
    cluster_new_skills,
)
from talent_matching.models.skills import Skill, SkillAlias


@op(description="Load existing alias bags and unprocessed skill names from DB")
def get_existing_bags_and_unprocessed_skills(context: OpExecutionContext) -> dict:
    """Return existing bags (canonical + skill_id + aliases) and unprocessed skill names."""
    session = get_session()
    canonicals = (
        session.execute(select(Skill).where(Skill.id.in_(select(SkillAlias.skill_id).distinct())))
        .scalars()
        .all()
    )
    existing_bags = []
    for skill in canonicals:
        aliases = [
            row[0]
            for row in session.execute(
                select(SkillAlias.alias).where(SkillAlias.skill_id == skill.id)
            ).all()
        ]
        existing_bags.append(
            {
                "canonical": skill.name,
                "skill_id": str(skill.id),
                "aliases": aliases,
            }
        )

    alias_names = {row[0] for row in session.execute(select(SkillAlias.alias)).all()}
    canonical_ids = {s.id for s in canonicals}
    unprocessed = (
        session.execute(select(Skill.name).where(Skill.id.not_in(canonical_ids))).scalars().all()
    )
    unprocessed_names = sorted({name for name in unprocessed if name not in alias_names})
    session.close()

    context.log.info(
        f"Found {len(existing_bags)} existing bags and {len(unprocessed_names)} unprocessed skill names"
    )
    return {
        "existing_bags": existing_bags,
        "unprocessed_names": unprocessed_names,
    }


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Assign unprocessed skill names to existing bags via LLM",
)
def assign_unprocessed_skills_to_bags(context: OpExecutionContext, data: dict) -> dict:
    """Call LLM to assign each unprocessed name to an existing canonical or new_groups."""
    existing_bags = data["existing_bags"]
    unprocessed_names = data["unprocessed_names"]
    if not unprocessed_names:
        context.log.info("No unprocessed names; skipping LLM")
        return {"assignments": [], "new_groups": [], "existing_bags": existing_bags}

    bags_for_prompt = [
        {"canonical": b["canonical"], "aliases": b["aliases"]} for b in existing_bags
    ]
    result = asyncio.run(
        assign_skills_to_bags(
            context.resources.openrouter,
            bags_for_prompt,
            unprocessed_names,
            dagster_log=context.log,
        )
    )
    result["existing_bags"] = existing_bags
    context.log.info(
        f"LLM returned {len(result['assignments'])} assignments and "
        f"{len(result['new_groups'])} new_groups"
    )
    return result


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Cluster unmatched skill names (new_groups) into new bags via LLM",
)
def cluster_unmatched_skills(context: OpExecutionContext, llm_result: dict) -> dict:
    """Group new_groups into clusters so new alias bags can be created."""
    new_groups = llm_result.get("new_groups") or []
    if not new_groups:
        context.log.info("No new_groups to cluster; skipping")
        llm_result["clusters"] = []
        return llm_result

    clusters = asyncio.run(
        cluster_new_skills(
            context.resources.openrouter,
            new_groups,
            dagster_log=context.log,
        )
    )
    llm_result["clusters"] = clusters
    multi_alias = [c for c in clusters if c.get("aliases")]
    context.log.info(
        f"Clustered {len(new_groups)} unmatched names into {len(clusters)} groups "
        f"({len(multi_alias)} with aliases, {len(clusters) - len(multi_alias)} singletons)"
    )
    return llm_result


@op(description="Apply assignments and clusters: insert skill_aliases rows, create new bags")
def apply_skill_assignments(context: OpExecutionContext, llm_result: dict) -> dict:
    """Write alias rows for both existing-bag assignments and new clusters."""
    assignments = llm_result.get("assignments") or []
    existing_bags = llm_result.get("existing_bags") or []
    clusters = llm_result.get("clusters") or []
    canonical_to_id = {b["canonical"]: UUID(b["skill_id"]) for b in existing_bags}
    canonical_to_id_lower = {k.lower(): v for k, v in canonical_to_id.items()}

    session = get_session()

    def _resolve_canonical(canonical: str) -> UUID | None:
        sid = canonical_to_id.get(canonical) or canonical_to_id_lower.get(canonical.lower())
        if sid is not None:
            return sid
        skill = (
            session.execute(select(Skill).where(func.lower(Skill.name) == canonical.lower()))
            .scalars()
            .first()
        )
        if skill is not None:
            canonical_to_id[skill.name] = skill.id
            canonical_to_id_lower[skill.name.lower()] = skill.id
            return skill.id
        return None

    applied = 0
    skipped = 0
    for item in assignments:
        skill_name = (item.get("skill_name") or "").strip()
        canonical = (item.get("assign_to_canonical") or "").strip()
        if not skill_name or not canonical:
            continue
        skill_id = _resolve_canonical(canonical)
        if skill_id is None:
            skipped += 1
            context.log.warning(
                f"Assignment canonical '{canonical}' not found in bags or skills table; "
                f"skipping '{skill_name}'"
            )
            continue
        stmt = insert(SkillAlias).values(
            alias=skill_name,
            skill_id=skill_id,
            added_by="llm",
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["alias"],
            set_={"skill_id": skill_id, "added_by": "llm"},
        )
        session.execute(stmt)
        applied += 1

    bags_created = 0
    aliases_from_clusters = 0
    for cluster in clusters:
        canonical_name = (cluster.get("canonical") or "").strip()
        aliases = cluster.get("aliases") or []
        if not canonical_name or not aliases:
            continue

        canonical_skill = (
            session.execute(select(Skill).where(func.lower(Skill.name) == canonical_name.lower()))
            .scalars()
            .first()
        )
        if canonical_skill is None:
            context.log.warning(
                f"Cluster canonical '{canonical_name}' not found in skills table; skipping"
            )
            continue

        bags_created += 1
        for alias_name in aliases:
            alias_name = alias_name.strip()
            if not alias_name or alias_name == canonical_name:
                continue
            stmt = insert(SkillAlias).values(
                alias=alias_name,
                skill_id=canonical_skill.id,
                added_by="llm",
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["alias"],
                set_={"skill_id": canonical_skill.id, "added_by": "llm"},
            )
            session.execute(stmt)
            aliases_from_clusters += 1

    session.commit()
    session.close()

    context.log.info(
        f"Applied {applied} assignments ({skipped} skipped), "
        f"created {bags_created} new bags with {aliases_from_clusters} aliases"
    )
    return {
        "applied": applied,
        "skipped": skipped,
        "assignments_count": len(assignments),
        "bags_created": bags_created,
        "aliases_from_clusters": aliases_from_clusters,
    }


@job(
    description="Normalize unprocessed skill names into existing alias bags via LLM (run periodically)",
    op_retry_policy=openrouter_retry_policy,
)
def skill_normalization_job():
    """Normalize unprocessed skill names into alias bags."""
    data = get_existing_bags_and_unprocessed_skills()
    llm_result = assign_unprocessed_skills_to_bags(data)
    llm_result_with_clusters = cluster_unmatched_skills(llm_result)
    apply_skill_assignments(llm_result_with_clusters)


skill_normalization_schedule = ScheduleDefinition(
    name="skill_normalization_daily",
    cron_schedule="0 2 * * *",
    job=skill_normalization_job,
    description="Run skill normalization daily at 02:00 UTC",
)
