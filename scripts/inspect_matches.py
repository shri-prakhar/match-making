#!/usr/bin/env python3
"""Inspect stored matches for a job by partition ID (job's airtable_record_id).

Prints match results in the same format as run_matchmaking_scoring.py:
combined score, vector (role/domain/culture), skills fit, compensation,
experience, location, matching/missing skills.

When --verify-location is passed, also checks that each matched candidate
passes the location pre-filter (job Preferred Location vs candidate region/country/city).

Usage:
    poetry run with-local-db python scripts/inspect_matches.py <partition_id>
    poetry run with-remote-db python scripts/inspect_matches.py <partition_id>
    poetry run with-remote-db python scripts/inspect_matches.py recXXXXXXXXXXXXXX --verify-location
    On server: poetry run python scripts/inspect_matches.py --local <partition_id>

Requires:
    - Matches already computed and stored (run matches asset for the job partition).
    - For remote: poetry run remote-ui or poetry run local-matchmaking must be running.
"""

import os
import sys

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

# Add project root for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from talent_matching.script_env import apply_local_db  # noqa: E402
from talent_matching.config.scoring import (  # noqa: E402
    ScoringWeights,
    get_weights_for_job_category,
)


def get_connection():
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    return psycopg2.connect(
        host=host,
        port=port,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def load_weights_for_job_category(conn, job_category: str | None) -> tuple[ScoringWeights, str]:
    """Load scoring weights from DB for job_category; fall back to config defaults.
    Returns (weights, source) where source is 'DB' or 'defaults'.
    """
    key = (job_category or "").strip()
    if not key:
        return get_weights_for_job_category(None), "defaults"
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT role_weight, domain_weight, culture_weight, impact_weight, technical_weight,
                  vector_weight, skill_fit_weight, compensation_weight, location_weight, seniority_scale_weight,
                  skill_rating_weight, skill_semantic_weight, seniority_max_deduction,
                  seniority_level_max_deduction, tenure_instability_max_deduction
           FROM scoring_weights WHERE job_category = %s""",
        (key,),
    )
    row = cur.fetchone()
    cur.close()
    if row:
        return (
            ScoringWeights(
                role_weight=float(row["role_weight"]),
                domain_weight=float(row["domain_weight"]),
                culture_weight=float(row["culture_weight"]),
                impact_weight=float(row["impact_weight"]),
                technical_weight=float(row["technical_weight"]),
                vector_weight=float(row["vector_weight"]),
                skill_fit_weight=float(row["skill_fit_weight"]),
                compensation_weight=float(row["compensation_weight"]),
                location_weight=float(row["location_weight"]),
                seniority_scale_weight=float(row["seniority_scale_weight"]),
                skill_rating_weight=float(row["skill_rating_weight"]),
                skill_semantic_weight=float(row["skill_semantic_weight"]),
                seniority_max_deduction=float(row["seniority_max_deduction"]),
                seniority_level_max_deduction=float(row["seniority_level_max_deduction"]),
                tenure_instability_max_deduction=float(row["tenure_instability_max_deduction"]),
            ),
            "DB",
        )
    return get_weights_for_job_category(key), "defaults"


def inspect_matches(partition_id: str, verify_location: bool = False) -> None:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    print(f"  DB: {host}:{port} (remote tunnel = localhost:15432 when remote-ui is running)")
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Resolve job by partition_id (airtable_record_id)
    cur.execute(
        """SELECT id, raw_job_id, job_title, company_name, airtable_record_id, job_category
           FROM normalized_jobs WHERE airtable_record_id = %s""",
        (partition_id,),
    )
    job = cur.fetchone()
    if not job:
        print(f"  No normalized job found with partition_id: {partition_id}")
        print("  Use the same Airtable record ID as in inspect_job.py (e.g. recXXXXXXXXXXXXXX).")
        cur.close()
        conn.close()
        sys.exit(1)

    job_id = job["id"]
    job_title = (job.get("job_title") or "—")[:35]
    company = (job.get("company_name") or "—")[:25]
    job_category = job.get("job_category")
    weights, weights_source = load_weights_for_job_category(conn, job_category)

    # Load job's location_raw for verification
    cur.execute(
        "SELECT location_raw FROM raw_jobs WHERE airtable_record_id = %s",
        (partition_id,),
    )
    raw_row = cur.fetchone()
    job_location_raw = (raw_row.get("location_raw") or "").strip() or None if raw_row else None

    # Load matches for this job with candidate details (incl. location for verification)
    cur.execute(
        """SELECT m.rank, m.match_score,
                  m.role_similarity_score, m.domain_similarity_score, m.culture_similarity_score,
                  m.skills_match_score, m.compensation_match_score,
                  m.experience_match_score, m.location_match_score,
                  m.matching_skills, m.missing_skills,
                  m.llm_fit_score, m.strengths, m.red_flags,
                  nc.id AS candidate_id, nc.full_name, nc.airtable_record_id AS candidate_partition_id,
                  nc.location_region, nc.location_country, nc.location_city
           FROM matches m
           JOIN normalized_candidates nc ON m.candidate_id = nc.id
           WHERE m.job_id = %s
           ORDER BY m.rank NULLS LAST, m.match_score DESC""",
        (job_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print("\n" + "=" * 80)
    print(f"  MATCHES FOR JOB: {partition_id}")
    print(f"  {job_title} @ {company}")
    if job_location_raw:
        print(f"  Job Preferred Location: {job_location_raw}")
    print("=" * 80)
    if not rows:
        print("\n  No matches found for this job.")
        print("  Run the matches asset for this job partition to compute and store matches.")
        print("=" * 80 + "\n")
        return

    print("\n" + "-" * 80)
    print("  SCORING BREAKDOWN (stored matches)")
    print(f"  Weights: from {weights_source}" + (f" (job_category={job_category})" if job_category else ""))
    print(
        f"  Formula: vector={weights.vector_weight:.2f} (role/domain/culture/impact/tech) + skill_fit={weights.skill_fit_weight:.2f} "
        f"+ comp={weights.compensation_weight:.2f} + location={weights.location_weight:.2f} + seniority_scale={weights.seniority_scale_weight:.2f} "
        f"− deductions (seniority max {weights.seniority_max_deduction:.2f}, level {weights.seniority_level_max_deduction:.2f}, tenure {weights.tenure_instability_max_deduction:.2f})"
    )
    print("  Inspect a candidate: python scripts/inspect_candidate.py <partition_id>")
    print("-" * 80)

    for r in rows:
        rank = r["rank"] or "—"
        match_score_0_1 = float(r["match_score"]) if r["match_score"] is not None else 0.0
        combined = round(match_score_0_1 * 100.0, 2)
        rs = r["role_similarity_score"]
        ds = r["domain_similarity_score"]
        cs = r["culture_similarity_score"]
        if rs is not None and ds is not None and cs is not None:
            vector_weighted = (
                weights.role_weight * rs + weights.domain_weight * ds + weights.culture_weight * cs
            )
            vec_rescaled_100 = round(vector_weighted * 100.0, 2)
        else:
            vec_rescaled_100 = "—"

        def _fmt(v):
            return f"{v:.4f}" if v is not None else "—"

        skill_fit = r["skills_match_score"]
        comp_score = r["compensation_match_score"]
        exp_score = r["experience_match_score"]
        loc_score = r["location_match_score"]
        name = (r["full_name"] or "—")[:40]
        candidate_partition_id = r["candidate_partition_id"] or "—"
        matching = r["matching_skills"] or []
        missing = r["missing_skills"] or []

        loc_region = r.get("location_region") or "—"
        loc_country = r.get("location_country") or "—"
        loc_city = r.get("location_city") or "—"
        loc_str = (
            f"{loc_city}, {loc_country}"
            if loc_city != "—"
            else (loc_country if loc_country != "—" else loc_region)
        )

        print(f"\n  Job: {job_title} @ {company}")
        print(f"  Rank {rank}: {name}  (partition: {candidate_partition_id})")
        print(f"    Candidate location: {loc_str}")
        print(
            f"    Combined score: {combined:.2f}  (stored match_score 0–1: {match_score_0_1:.4f})"
        )
        if vec_rescaled_100 != "—":
            print(
                f"    Vector (weighted 0–100): {vec_rescaled_100}  |  Role: {_fmt(rs)}  Domain: {_fmt(ds)}  Culture: {_fmt(cs)}"
            )
        else:
            print("    Vector: —  |  Role: —  Domain: —  Culture: —")
        print(
            f"    Skills fit: {_fmt(skill_fit)}  Compensation: {_fmt(comp_score)}  Experience: {_fmt(exp_score)}  Location: {_fmt(loc_score)}"
        )
        llm_score = r.get("llm_fit_score")
        if llm_score is not None:
            print(f"    LLM fit score: {llm_score}/10")
        strengths = r.get("strengths")
        if strengths:
            s = ", ".join(strengths) if isinstance(strengths, list) else str(strengths)
            print(f"    Pros: {s[:200]}{'...' if len(s) > 200 else ''}")
        red_flags = r.get("red_flags")
        if red_flags:
            s = ", ".join(red_flags) if isinstance(red_flags, list) else str(red_flags)
            print(f"    Cons: {s[:200]}{'...' if len(s) > 200 else ''}")
        print("    Seniority penalty: — (not stored)")
        if matching:
            print(f"    Matching skills: {', '.join(matching)}")
        if missing:
            print(f"    Missing skills:  {', '.join(missing)}")

    # Location verification (when job has Preferred Location filter)
    if verify_location and job_location_raw:
        from talent_matching.matchmaking.location_filter import (
            candidate_matches_location,
            parse_job_preferred_locations,
        )

        job_locations = parse_job_preferred_locations(job_location_raw)
        if job_locations is not None:
            failed: list[tuple[int, str, dict]] = []
            for r in rows:
                cand = {
                    "location_region": r.get("location_region"),
                    "location_country": r.get("location_country"),
                    "location_city": r.get("location_city"),
                }
                if not candidate_matches_location(cand, job_locations):
                    failed.append((r.get("rank") or 0, r.get("full_name") or "?", cand))
            print("\n" + "-" * 80)
            print("  LOCATION PRE-FILTER VERIFICATION")
            print("-" * 80)
            if failed:
                print(f"  ⚠ {len(failed)}/{len(rows)} candidates FAIL location filter:")
                for rank, name, loc in failed:
                    print(
                        f"    Rank {rank}: {name}  "
                        f"(region={loc.get('location_region') or '—'} "
                        f"country={loc.get('location_country') or '—'} "
                        f"city={loc.get('location_city') or '—'})"
                    )
            else:
                print(f"  ✓ All {len(rows)} candidates pass location filter.")
        else:
            print("\n  (No location filter: job has Global/No hard requirements)")

    print("\n" + "=" * 80)
    print(f"  Total match rows: {len(rows)}")
    print("=" * 80 + "\n")


def main():
    apply_local_db()
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = [a for a in sys.argv[1:] if a.startswith("--")]
    verify_location = "--verify-location" in flags

    if len(args) < 1:
        print("Usage: python scripts/inspect_matches.py <partition_id> [--verify-location]")
        print("Example: python scripts/inspect_matches.py recXXXXXXXXXXXXXX")
        print("  partition_id: job's Airtable record ID (same as in inspect_job.py)")
        print(
            "  --verify-location: verify each candidate passes the job's Preferred Location filter"
        )
        sys.exit(1)

    partition_id = args[0]
    print(f"\nInspecting matches for job partition: {partition_id}\n")
    inspect_matches(partition_id, verify_location=verify_location)


if __name__ == "__main__":
    main()
