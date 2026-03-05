#!/usr/bin/env python3
"""Inspect stored matches for a job by partition ID (job's airtable_record_id).

Prints match results in the same format as run_matchmaking_scoring.py:
combined score, vector (role/domain/culture), skills fit, compensation,
experience, location, matching/missing skills.

When --verify-location is passed, also checks that each matched candidate
passes the location pre-filter (job Preferred Location vs candidate region/country/city).

Usage:
    python scripts/inspect_matches.py <partition_id>
    python scripts/inspect_matches.py recXXXXXXXXXXXXXX
    python scripts/inspect_matches.py recXXXXXXXXXXXXXX --verify-location

Requires:
    - Matches already computed and stored (run matches asset for the job partition).

To inspect matches on the REMOTE server (when using poetry run remote-ui):
    POSTGRES_HOST=localhost POSTGRES_PORT=15432 python scripts/inspect_matches.py <partition_id>
  (15432 is the tunnel to remote Postgres; without it you default to port 5432 = local DB.)
"""

import os
import sys

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

# Add project root for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Same weights as matches asset / run_matchmaking_scoring (for vector display)
ROLE_WEIGHT = 0.4
DOMAIN_WEIGHT = 0.35
CULTURE_WEIGHT = 0.25


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


def inspect_matches(partition_id: str, verify_location: bool = False) -> None:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    print(f"  DB: {host}:{port} (remote tunnel = localhost:15432 when remote-ui is running)")
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Resolve job by partition_id (airtable_record_id)
    cur.execute(
        """SELECT id, raw_job_id, job_title, company_name, airtable_record_id
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
    print(
        "  Formula: 40% vector (raw) + 40% skill fit + 10% compensation + 10% location − seniority deduction (cap 20%)"
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
            vector_weighted = ROLE_WEIGHT * rs + DOMAIN_WEIGHT * ds + CULTURE_WEIGHT * cs
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
