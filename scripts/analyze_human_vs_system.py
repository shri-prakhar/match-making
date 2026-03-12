#!/usr/bin/env python3
"""Analyze how human-selected candidates perform in the system's scoring.

Compares candidates that recruiters manually placed in CLIENT INTRODUCTION,
Shortlisted Talent, Potential Talent Fit, or Hired against the system's
matchmaking scores and rankings for the same job.

Usage:
    poetry run with-remote-db python scripts/analyze_human_vs_system.py <ats_record_id>
    poetry run with-local-db python scripts/analyze_human_vs_system.py <ats_record_id>
    On server: poetry run python scripts/analyze_human_vs_system.py --local <ats_record_id>

Requires:
    - AIRTABLE_ATS_TABLE_ID, AIRTABLE_API_KEY in .env
    - Matches already computed for the job (run matchmaking pipeline first)
    - For remote DB: poetry run remote-ui or local-matchmaking must be running
"""

import os
import sys

import httpx
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from talent_matching.config.scoring import get_weights_for_job_category  # noqa: E402

HUMAN_SELECTION_COLUMNS = [
    "CLIENT INTRODUCTION",
    "Shortlisted Talent",
    "Potential Talent Fit",
    "Potential Talent Fit Nick",
    "Potential Talent Fit NOE",
    "Hired",
]


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


def fetch_ats_record(record_id: str) -> dict:
    """Fetch a single ATS record with all fields (no field restriction)."""
    base_id = os.environ["AIRTABLE_BASE_ID"]
    table_id = os.environ.get("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    token = os.environ["AIRTABLE_API_KEY"]
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()


def extract_linked_ids(fields: dict, column: str) -> list[str]:
    """Extract linked record IDs from an Airtable linked record field."""
    value = fields.get(column, [])
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str) and v.startswith("rec")]
    return []


def _fmt(v) -> str:
    return f"{v:.4f}" if v is not None else "--"


def _fmt2(v) -> str:
    return f"{v:.2f}" if v is not None else "--"


def analyze_one(partition_id: str, verbose: bool = True) -> dict | None:
    """Run analysis for one job. Returns summary stats dict, or None if no human selections.

    When verbose=True, prints detailed output. When verbose=False, returns stats only.
    """
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    if verbose:
        print(f"  DB: {host}:{port}")

    # 1. Fetch ATS record from Airtable
    if verbose:
        print(f"\nFetching ATS record {partition_id} from Airtable...")
    ats_record = fetch_ats_record(partition_id)
    ats_fields = ats_record.get("fields", {})
    job_title = ats_fields.get("Open Position (Job Title)", "Unknown")
    company = ats_fields.get("Company", ["Unknown"])
    if isinstance(company, list):
        company = company[0] if company else "Unknown"
    if verbose:
        print(f"  Job: {job_title} @ {company}")

    # 2. Extract human-selected candidates per column
    human_selections: dict[str, list[str]] = {}
    all_human_ids: set[str] = set()
    for col in HUMAN_SELECTION_COLUMNS:
        ids = extract_linked_ids(ats_fields, col)
        if ids:
            human_selections[col] = ids
            all_human_ids.update(ids)
            if verbose:
                print(f"  {col}: {len(ids)} candidates")

    if not all_human_ids:
        if verbose:
            print("\n  No human-selected candidates found in any column.")
            print("  Columns checked: " + ", ".join(HUMAN_SELECTION_COLUMNS))
        return None

    # Also get AI-proposed candidates for comparison
    ai_proposed_ids = extract_linked_ids(ats_fields, "AI PROPOSTED CANDIDATES")
    if verbose:
        print(f"  AI Proposed: {len(ai_proposed_ids)} candidates")

    # 3. Resolve job in Postgres
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """SELECT id, job_title, company_name, job_category
           FROM normalized_jobs WHERE airtable_record_id = %s""",
        (partition_id,),
    )
    job = cur.fetchone()
    if not job:
        if verbose:
            print(f"\n  No normalized job found for {partition_id} in Postgres.")
            print("  Run the job normalization pipeline first.")
        cur.close()
        conn.close()
        return None
    job_id = job["id"]
    weights = get_weights_for_job_category(job.get("job_category"))

    # 4. Load all matches for this job
    cur.execute(
        """SELECT m.rank, m.match_score,
                  m.role_similarity_score, m.domain_similarity_score, m.culture_similarity_score,
                  m.skills_match_score, m.compensation_match_score,
                  m.experience_match_score, m.location_match_score,
                  m.matching_skills, m.missing_skills,
                  m.llm_fit_score, m.strengths, m.red_flags,
                  nc.id AS candidate_id, nc.full_name, nc.airtable_record_id,
                  nc.location_region, nc.location_country, nc.location_city,
                  nc.current_role, nc.seniority_level, nc.years_of_experience
           FROM matches m
           JOIN normalized_candidates nc ON m.candidate_id = nc.id
           WHERE m.job_id = %s
           ORDER BY m.rank NULLS LAST, m.match_score DESC""",
        (job_id,),
    )
    all_matches = cur.fetchall()

    matches_by_airtable_id: dict[str, dict] = {}
    for m in all_matches:
        if m["airtable_record_id"]:
            matches_by_airtable_id[m["airtable_record_id"]] = m

    # 5. Resolve human-selected candidates from Postgres
    human_candidates: dict[str, dict | None] = {}
    if all_human_ids:
        placeholders = ",".join(["%s"] * len(all_human_ids))
        cur.execute(
            f"""SELECT id, full_name, airtable_record_id,
                       location_region, location_country, location_city,
                       current_role, seniority_level, years_of_experience,
                       desired_job_categories
                FROM normalized_candidates
                WHERE airtable_record_id IN ({placeholders})""",
            list(all_human_ids),
        )
        for row in cur.fetchall():
            human_candidates[row["airtable_record_id"]] = row

    cur.close()
    conn.close()

    # 6. Compute analysis
    total_matches = len(all_matches)
    if verbose:
        print(f"\n  System matches for this job: {total_matches}")
        print("\n" + "=" * 100)
        print("  HUMAN-SELECTED CANDIDATES: SYSTEM SCORING ANALYSIS")
        print("=" * 100)

    found_in_system = 0
    not_in_system = 0
    human_ranks: list[int] = []
    human_scores: list[float] = []
    human_llm_scores: list[int] = []

    for col, candidate_ids in human_selections.items():
        if verbose:
            print(f"\n  --- {col} ({len(candidate_ids)} candidates) ---")
        for at_id in candidate_ids:
            cand_info = human_candidates.get(at_id)
            match_info = matches_by_airtable_id.get(at_id)
            name = cand_info["full_name"] if cand_info else at_id

            if match_info:
                found_in_system += 1
                rank = match_info["rank"]
                score = (
                    float(match_info["match_score"]) * 100
                    if match_info["match_score"] is not None
                    else 0
                )
                llm_score = match_info.get("llm_fit_score")
                if rank is not None:
                    human_ranks.append(rank)
                human_scores.append(score)
                if llm_score is not None:
                    human_llm_scores.append(llm_score)

                rs = match_info["role_similarity_score"]
                ds = match_info["domain_similarity_score"]
                cs = match_info["culture_similarity_score"]
                if rs is not None and ds is not None and cs is not None:
                    vec_weighted = (
                        weights.role_weight * rs
                        + weights.domain_weight * ds
                        + weights.culture_weight * cs
                    )
                    vec_str = f"{vec_weighted * 100:.2f}"
                else:
                    vec_str = "--"

                if verbose:
                    print(f"\n    {name}  (partition: {at_id})")
                    print(
                        f"      FOUND in system  |  Rank: {rank or '--'}/{total_matches}  |  Combined: {_fmt2(score)}  |  LLM: {llm_score or '--'}/10"
                    )
                    print(f"      Vector: {vec_str}  (R:{_fmt(rs)} D:{_fmt(ds)} C:{_fmt(cs)})")
                    print(
                        f"      Skills: {_fmt(match_info['skills_match_score'])}  Comp: {_fmt(match_info['compensation_match_score'])}  Exp: {_fmt(match_info['experience_match_score'])}  Loc: {_fmt(match_info['location_match_score'])}"
                    )
                    matching = match_info.get("matching_skills") or []
                    missing = match_info.get("missing_skills") or []
                    if matching:
                        print(f"      Matching skills: {', '.join(matching)}")
                    if missing:
                        print(f"      Missing skills:  {', '.join(missing)}")
                    strengths = match_info.get("strengths")
                    if strengths:
                        s = ", ".join(strengths) if isinstance(strengths, list) else str(strengths)
                        print(f"      Pros: {s[:200]}")
                    red_flags = match_info.get("red_flags")
                    if red_flags:
                        s = ", ".join(red_flags) if isinstance(red_flags, list) else str(red_flags)
                        print(f"      Cons: {s[:200]}")
            else:
                not_in_system += 1
                if verbose:
                    print(f"\n    {name}  (partition: {at_id})")
                    print("      NOT in system matches")
                    if cand_info:
                        loc_city = cand_info.get("location_city") or "--"
                        loc_country = cand_info.get("location_country") or "--"
                        role = cand_info.get("current_role") or "--"
                        seniority = cand_info.get("seniority_level") or "--"
                        yoe = cand_info.get("years_of_experience")
                        cats = cand_info.get("desired_job_categories") or []
                        print(
                            f"      Role: {role}  |  Seniority: {seniority}  |  YoE: {yoe or '--'}"
                        )
                        print(f"      Location: {loc_city}, {loc_country}")
                        if cats:
                            cat_str = ", ".join(cats) if isinstance(cats, list) else str(cats)
                            print(f"      Job categories: {cat_str}")
                        print(
                            "      Possible reasons: filtered by location/category/skill threshold, or matchmaking not run"
                        )
                    else:
                        print(
                            "      Candidate not found in normalized_candidates (not yet ingested?)"
                        )

    # 7. Build summary stats
    total_human = sum(len(ids) for ids in human_selections.values())
    system_top15_ids = {
        m["airtable_record_id"] for m in all_matches[:15] if m["airtable_record_id"]
    }
    system_all_ids = {m["airtable_record_id"] for m in all_matches if m["airtable_record_id"]}
    overlap_top15 = all_human_ids & system_top15_ids
    overlap_all = all_human_ids & system_all_ids

    stats: dict = {
        "partition_id": partition_id,
        "job_title": job_title,
        "company": company,
        "total_human": total_human,
        "found_in_system": found_in_system,
        "not_in_system": not_in_system,
        "total_matches": total_matches,
        "overlap_top15": len(overlap_top15),
        "overlap_any": len(overlap_all),
    }
    if human_ranks:
        stats["avg_rank"] = sum(human_ranks) / len(human_ranks)
        stats["best_rank"] = min(human_ranks)
        stats["worst_rank"] = max(human_ranks)
        stats["in_top5"] = sum(1 for r in human_ranks if r <= 5)
        stats["in_top10"] = sum(1 for r in human_ranks if r <= 10)
        stats["in_top15"] = sum(1 for r in human_ranks if r <= 15)
        stats["human_ranks_count"] = len(human_ranks)
    if human_scores:
        stats["avg_score"] = sum(human_scores) / len(human_scores)
        stats["min_score"] = min(human_scores)
        stats["max_score"] = max(human_scores)
    if human_llm_scores:
        stats["avg_llm_score"] = sum(human_llm_scores) / len(human_llm_scores)
        stats["min_llm_score"] = min(human_llm_scores)
        stats["max_llm_score"] = max(human_llm_scores)

    if verbose:
        print("\n" + "=" * 100)
        print("  SUMMARY STATISTICS")
        print("=" * 100)
        print(f"\n  Total human-selected candidates: {total_human}")
        print(
            f"  Found in system matches: {found_in_system}/{total_human} ({100 * found_in_system / total_human:.0f}%)"
            if total_human
            else ""
        )
        print(f"  NOT in system matches: {not_in_system}/{total_human}")
        if human_ranks:
            print("\n  Rank statistics (of those found):")
            print(f"    Average rank: {stats['avg_rank']:.1f}")
            print(f"    Best rank:    {stats['best_rank']}")
            print(f"    Worst rank:   {stats['worst_rank']}")
            print(f"    In top 5:     {stats['in_top5']}/{stats['human_ranks_count']}")
            print(f"    In top 10:    {stats['in_top10']}/{stats['human_ranks_count']}")
            print(f"    In top 15:    {stats['in_top15']}/{stats['human_ranks_count']}")
        if human_scores:
            print("\n  Score statistics (combined, 0-100):")
            print(f"    Average: {stats['avg_score']:.2f}")
            print(f"    Min:     {stats['min_score']:.2f}")
            print(f"    Max:     {stats['max_score']:.2f}")
        if human_llm_scores:
            print("\n  LLM fit score statistics (1-10):")
            print(f"    Average: {stats['avg_llm_score']:.1f}")
            print(f"    Min:     {stats['min_llm_score']}")
            print(f"    Max:     {stats['max_llm_score']}")
        print("\n" + "=" * 100)
        print("  SYSTEM TOP 15 CANDIDATES (for comparison)")
        print("=" * 100)
        human_at_ids = all_human_ids
        for m in all_matches[:15]:
            rank = m["rank"] or "--"
            score = float(m["match_score"]) * 100 if m["match_score"] is not None else 0
            llm_score = m.get("llm_fit_score")
            name = (m["full_name"] or "--")[:35]
            at_id = m["airtable_record_id"] or "--"
            is_human = "  <<< HUMAN-SELECTED" if at_id in human_at_ids else ""
            is_ai = " [AI]" if at_id in ai_proposed_ids else ""
            print(f"\n    Rank {rank}: {name}  ({at_id}){is_ai}{is_human}")
            print(
                f"      Combined: {_fmt2(score)}  |  LLM: {llm_score or '--'}/10  |  Skills: {_fmt(m['skills_match_score'])}"
            )
        print("\n" + "=" * 100)
        print("  OVERLAP ANALYSIS")
        print("=" * 100)
        print(f"\n  Human selections in system top 15: {len(overlap_top15)}/{len(human_at_ids)}")
        print(f"  Human selections in any system match: {len(overlap_all)}/{len(human_at_ids)}")
        if human_at_ids - system_all_ids:
            print(
                f"  Human selections NOT matched by system at all: {len(human_at_ids - system_all_ids)}"
            )
            for at_id in human_at_ids - system_all_ids:
                cand = human_candidates.get(at_id)
                name = cand["full_name"] if cand else at_id
                print(f"    - {name} ({at_id})")
        print("\n" + "=" * 100)
        print()

    return stats


def main():
    from talent_matching.script_env import apply_local_db

    apply_local_db()
    if len(sys.argv) < 2:
        print("Usage: python scripts/analyze_human_vs_system.py <ats_record_id>")
        print(
            "Example: poetry run with-remote-db python scripts/analyze_human_vs_system.py recXXXXXXXXXXXXXX"
        )
        print()
        print("Compares human-selected candidates (Client Introduction, Shortlisted, etc.)")
        print("against the system's matchmaking scores and rankings for the same job.")
        sys.exit(1)

    partition_id = sys.argv[1]
    print(f"\nAnalyzing human vs system selections for job: {partition_id}\n")
    analyze_one(partition_id, verbose=True)


if __name__ == "__main__":
    main()
