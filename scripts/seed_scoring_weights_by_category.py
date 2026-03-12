"""Seed or update scoring_weights with manual per-category weights.

Run against remote DB: poetry run with-remote-db python scripts/seed_scoring_weights_by_category.py
Run against local DB:  poetry run with-local-db python scripts/seed_scoring_weights_by_category.py
On server: poetry run python scripts/seed_scoring_weights_by_category.py --local

  --list   Only list distinct job_category from normalized_jobs and from scoring_weights; do not update.
  --dry-run  Print what would be updated/inserted; do not write to DB.

Weights are defined in TUNED_WEIGHTS. Vector sub-weights and top-level weights
are normalized to sum to 1.0 before upsert (so small drift or edits still yield valid rows).
See docs/scoring-weights-by-category.md for rationale.
"""

import argparse
from copy import deepcopy
from datetime import UTC, datetime

from sqlalchemy import select

from talent_matching.db import get_session
from talent_matching.models.jobs import NormalizedJob
from talent_matching.models.scoring_weights import ScoringWeightsRecord
from talent_matching.script_env import apply_local_db

# Keys that must sum to 1.0 (normalized before write).
_VECTOR_SUB_KEYS = (
    "role_weight",
    "domain_weight",
    "culture_weight",
    "impact_weight",
    "technical_weight",
)
_TOP_LEVEL_KEYS = (
    "vector_weight",
    "skill_fit_weight",
    "compensation_weight",
    "location_weight",
    "seniority_scale_weight",
)
_SKILL_FIT_SUB_KEYS = ("skill_rating_weight", "skill_semantic_weight")


def _normalize_group(d: dict[str, float], keys: tuple[str, ...]) -> None:
    """In-place: scale values at keys so they sum to 1.0. If sum is 0, set equal."""
    total = sum(d[k] for k in keys)
    if total <= 0:
        n = len(keys)
        for k in keys:
            d[k] = 1.0 / n
        return
    for k in keys:
        d[k] = d[k] / total


def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    """Return a copy of weights with vector sub, top-level, and skill-fit sub each summing to 1.0."""
    out = deepcopy(weights)
    _normalize_group(out, _VECTOR_SUB_KEYS)
    _normalize_group(out, _TOP_LEVEL_KEYS)
    _normalize_group(out, _SKILL_FIT_SUB_KEYS)
    return out


# Default top-level and deductions (same for all categories unless overridden).
# Top-level: slightly skill-heavy default (technical roles dominate; hard skills are highly predictive).
# Skill-fit sub: small boost to semantic so near-match skills (e.g. "Python" vs "Python 3") get partial credit.
_BASE = {
    "vector_weight": 0.28,
    "skill_fit_weight": 0.42,
    "compensation_weight": 0.10,
    "location_weight": 0.15,
    "seniority_scale_weight": 0.05,
    "skill_rating_weight": 0.75,
    "skill_semantic_weight": 0.25,
    "seniority_max_deduction": 0.2,
    "seniority_level_max_deduction": 0.1,
    "tenure_instability_max_deduction": 0.1,
}

# Non-technical roles: softer signals (culture, domain, impact) matter more than exact skill checklist.
# Override top-level so vector and skill_fit are balanced (0.35 each) instead of skill-heavy.
_TOP_LEVEL_SOFT = {"vector_weight": 0.35, "skill_fit_weight": 0.35}


def _vec(
    role: float, domain: float, culture: float, impact: float, technical: float
) -> dict[str, float]:
    """Vector sub-weights must sum to 1.0."""
    return {
        "role_weight": role,
        "domain_weight": domain,
        "culture_weight": culture,
        "impact_weight": impact,
        "technical_weight": technical,
    }


# Per-category tuned vector sub-weights. Keys must match job_category in normalized_jobs.
# Rationale in docs/scoring-weights-by-category.md.
TUNED_WEIGHTS: dict[str, dict[str, float]] = {
    # Sales: revenue and deal impact; softer signals matter — use balanced top-level
    "Account Executive": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.30, 0.25, 0.20, 0.25, 0.0)},
    "Business Development": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.30, 0.25, 0.20, 0.25, 0.0)},
    # Technical — general baseline (uses _BASE: skill-heavy top-level)
    "General (technical)": {**_BASE, **_vec(0.30, 0.25, 0.15, 0.15, 0.15)},
    "Backend Developer": {**_BASE, **_vec(0.30, 0.25, 0.15, 0.15, 0.15)},
    "Full-Stack Developer": {**_BASE, **_vec(0.30, 0.25, 0.15, 0.15, 0.15)},
    # Technical — deep specialization (platform-locked or tooling-heavy)
    "AI Engineer": {**_BASE, **_vec(0.25, 0.25, 0.10, 0.15, 0.25)},
    "Mobile Engineer": {**_BASE, **_vec(0.25, 0.25, 0.10, 0.15, 0.25)},
    "Protocol Engineer": {**_BASE, **_vec(0.22, 0.28, 0.10, 0.15, 0.25)},
    "Security": {**_BASE, **_vec(0.25, 0.28, 0.12, 0.15, 0.20)},
    # Technical — tooling-heavy (Terraform, K8s, Spark, dbt, CI/CD)
    "Data Engineer": {**_BASE, **_vec(0.25, 0.27, 0.13, 0.15, 0.20)},
    "DevOps": {**_BASE, **_vec(0.25, 0.27, 0.13, 0.15, 0.20)},
    "Infrastructure Engineer": {**_BASE, **_vec(0.25, 0.27, 0.13, 0.15, 0.20)},
    # Technical — more transferable stacks; frontend has higher culture (UX/design sensibility)
    "Frontend Developer": {**_BASE, **_vec(0.30, 0.22, 0.18, 0.15, 0.15)},
    # Technical — QA: domain is critical (regulated vs consumer); tooling is transferable
    "QA Engineer": {**_BASE, **_vec(0.30, 0.30, 0.15, 0.15, 0.10)},
    # Technical — DevRel: bridges technical and community; culture high
    "DevRel": {**_BASE, **_vec(0.28, 0.25, 0.22, 0.15, 0.10)},
    # Product / Growth / Marketing: softer signals — balanced top-level
    "Product Manager": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.30, 0.20, 0.25, 0.25, 0.0)},
    "Growth": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.28, 0.25, 0.17, 0.25, 0.05)},
    "Product Marketer": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.30, 0.25, 0.25, 0.20, 0.0)},
    "Community & Marketing": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.30, 0.25, 0.25, 0.20, 0.0)},
    "Public Relations": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.30, 0.25, 0.25, 0.20, 0.0)},
    "Designer": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.35, 0.25, 0.25, 0.15, 0.0)},
    "Product Designer": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.35, 0.25, 0.25, 0.15, 0.0)},
    # Legal / Compliance / Operations / Support / PM / HR: domain and culture; balanced top-level
    "Legal": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.35, 0.35, 0.25, 0.05, 0.0)},
    "Compliance": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.35, 0.35, 0.25, 0.05, 0.0)},
    "Operations": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.35, 0.30, 0.25, 0.10, 0.0)},
    "Customer Support": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.35, 0.30, 0.25, 0.10, 0.0)},
    "Project Manager": {**_BASE, **_TOP_LEVEL_SOFT, **_vec(0.35, 0.30, 0.20, 0.15, 0.0)},
    "Talent Sourcing / Human Resources": {
        **_BASE,
        **_TOP_LEVEL_SOFT,
        **_vec(0.35, 0.30, 0.25, 0.10, 0.0),
    },
    # Research: domain-heavy, but technical matters (ML, crypto, systems research)
    "Research": {**_BASE, **_vec(0.30, 0.35, 0.15, 0.10, 0.10)},
}


def _list_categories(session) -> None:
    """Print distinct job_category from normalized_jobs and from scoring_weights."""
    from sqlalchemy import distinct

    job_cats = (
        session.execute(
            select(distinct(NormalizedJob.job_category)).where(
                NormalizedJob.job_category.isnot(None),
                NormalizedJob.job_category != "",
            )
        )
        .scalars()
        .all()
    )
    job_cats = sorted({c for c in job_cats if (c or "").strip()})

    weight_cats = session.execute(select(ScoringWeightsRecord.job_category)).scalars().all()
    weight_cats = sorted(set(weight_cats))

    print("Distinct job_category in normalized_jobs:")
    for c in job_cats:
        in_tuned = " (in TUNED_WEIGHTS)" if c in TUNED_WEIGHTS else ""
        print(f"  {c!r}{in_tuned}")
    print("\nDistinct job_category in scoring_weights:")
    for c in weight_cats:
        print(f"  {c!r}")
    only_weights = set(weight_cats) - set(job_cats)
    only_jobs = set(job_cats) - set(weight_cats)
    if only_weights:
        print(f"\nIn scoring_weights but not in normalized_jobs: {only_weights}")
    if only_jobs:
        print(
            f"In normalized_jobs but not in scoring_weights (will get defaults on first match): {only_jobs}"
        )


def _apply(session, dry_run: bool) -> None:
    """Upsert TUNED_WEIGHTS into scoring_weights. Normalizes weights to sum to 1 where required."""
    now = datetime.now(UTC)
    updated = 0
    inserted = 0
    for job_category, weights in TUNED_WEIGHTS.items():
        weights = normalize_weights(weights)
        row = session.execute(
            select(ScoringWeightsRecord).where(ScoringWeightsRecord.job_category == job_category)
        ).scalar_one_or_none()
        if row is not None:
            if dry_run:
                print(f"Would update: {job_category!r}")
            else:
                for key, value in weights.items():
                    setattr(row, key, value)
                row.updated_at = now
            updated += 1
        else:
            if dry_run:
                print(f"Would insert: {job_category!r}")
            else:
                session.add(
                    ScoringWeightsRecord(
                        job_category=job_category,
                        **weights,
                    )
                )
            inserted += 1
    if not dry_run:
        session.commit()
    print(f"Done: {updated} updated, {inserted} inserted (dry_run={dry_run}).")
    session.close()


def main() -> None:
    apply_local_db()
    parser = argparse.ArgumentParser(
        description="Seed or update scoring_weights with manual per-category weights."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (when running on the server).",
    )
    parser.add_argument(
        "--list", action="store_true", help="List job categories only; do not update."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print what would be done; do not write."
    )
    args = parser.parse_args()
    session = get_session()
    if args.list:
        _list_categories(session)
        session.close()
        return
    _apply(session, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
