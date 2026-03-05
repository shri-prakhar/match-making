#!/usr/bin/env python3
"""Analyze matches for a job: must-have coverage and years-of-experience accuracy.

For each matched candidate:
- Checks if all job must-have skills are covered
- Compares candidate years_experience per skill vs job min_years
- Cross-checks candidate_skills years against candidate_experiences (skills_used + tenure)

Usage:
    poetry run with-local-db python scripts/analyze_matches.py <partition_id>
    poetry run with-remote-db python scripts/analyze_matches.py recumPHbWDgLHf6jX

For remote: poetry run remote-ui or poetry run local-matchmaking must be running.
"""

import os
import sys
from textwrap import dedent

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def _skill_match(cand_skill_name: str | None, job_skill_name: str) -> bool:
    """Check if candidate skill matches job skill (case-insensitive, strip)."""
    if not cand_skill_name:
        return False
    return cand_skill_name.strip().lower() == job_skill_name.strip().lower()


def run_analysis(partition_id: str) -> None:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ─── Job ─────────────────────────────────────────────────────
    cur.execute(
        """SELECT id, job_title, company_name, airtable_record_id
           FROM normalized_jobs WHERE airtable_record_id = %s""",
        (partition_id,),
    )
    job = cur.fetchone()
    if not job:
        print(f"No normalized job found for partition_id: {partition_id}", file=sys.stderr)
        cur.close()
        conn.close()
        sys.exit(1)

    job_id = job["id"]

    # Job required skills with min_years
    cur.execute(
        """SELECT jrs.requirement_type, jrs.min_years, s.name AS skill_name
           FROM job_required_skills jrs
           LEFT JOIN skills s ON jrs.skill_id = s.id
           WHERE jrs.job_id = %s
           ORDER BY jrs.requirement_type, s.name""",
        (job_id,),
    )
    required_skills = cur.fetchall()
    must_have = [
        (r["skill_name"], r["min_years"])
        for r in required_skills
        if r["requirement_type"] == "must_have"
    ]
    nice_to_have = [
        (r["skill_name"], r["min_years"])
        for r in required_skills
        if r["requirement_type"] == "nice_to_have"
    ]

    # ─── Matches ──────────────────────────────────────────────────
    cur.execute(
        """SELECT m.rank, m.match_score, m.matching_skills, m.missing_skills,
                  nc.id AS candidate_id, nc.full_name, nc.airtable_record_id AS candidate_partition_id,
                  nc.normalized_json
           FROM matches m
           JOIN normalized_candidates nc ON m.candidate_id = nc.id
           WHERE m.job_id = %s
           ORDER BY m.rank NULLS LAST, m.match_score DESC""",
        (job_id,),
    )
    matches = cur.fetchall()

    if not matches:
        print("No matches found for this job.", file=sys.stderr)
        cur.close()
        conn.close()
        sys.exit(0)

    # Load candidate_skills for all matched candidates
    candidate_ids = [m["candidate_id"] for m in matches]
    cur.execute(
        """SELECT cs.candidate_id, cs.years_experience, cs.rating, cs.notable_achievement,
                  s.name AS skill_name
           FROM candidate_skills cs
           LEFT JOIN skills s ON cs.skill_id = s.id
           WHERE cs.candidate_id IN %s
           ORDER BY cs.candidate_id, s.name""",
        (tuple(candidate_ids),),
    )
    skills_rows = cur.fetchall()
    cand_skills_map: dict = {}
    for row in skills_rows:
        cid = row["candidate_id"]
        if cid not in cand_skills_map:
            cand_skills_map[cid] = []
        cand_skills_map[cid].append(row)

    # Load candidate_experiences for cross-checking years
    cur.execute(
        """SELECT candidate_id, company_name, position_title, years_experience,
                  skills_used, start_date, end_date, is_current
           FROM candidate_experiences
           WHERE candidate_id IN %s
           ORDER BY candidate_id, position_order""",
        (tuple(candidate_ids),),
    )
    exp_rows = cur.fetchall()
    cand_exp_map: dict = {}
    for row in exp_rows:
        cid = row["candidate_id"]
        if cid not in cand_exp_map:
            cand_exp_map[cid] = []
        cand_exp_map[cid].append(row)

    # ─── Print report ────────────────────────────────────────────
    print(
        dedent(f"""
    # Match Analysis: Must-Haves & Years of Experience
    ## Job: {job['job_title']} @ {job['company_name']}
    **Partition ID:** `{partition_id}`
    **DB:** {host}:{port}

    ### Job must-have skills (with min years)
    """).strip()
    )

    if must_have:
        for name, min_y in must_have:
            min_str = f" (min {min_y} yrs)" if min_y is not None else ""
            print(f"  - {name}{min_str}")
    else:
        print("  (none defined)")

    if nice_to_have:
        print("\n### Job nice-to-have skills")
        for name, min_y in nice_to_have:
            min_str = f" (min {min_y} yrs)" if min_y is not None else ""
            print(f"  - {name}{min_str}")

    print("\n---\n")

    for m in matches:
        rank = m["rank"] or "—"
        name = (m["full_name"] or "—").strip()
        cid = m["candidate_id"]
        cand_partition = m["candidate_partition_id"] or "—"
        cand_skills = cand_skills_map.get(cid, [])
        cand_exps = cand_exp_map.get(cid, [])
        nj = m.get("normalized_json")

        # Build candidate skill lookup: skill_name -> years_experience
        cand_skill_years: dict[str, int | None] = {}
        for s in cand_skills:
            sn = s["skill_name"]
            if sn:
                cand_skill_years[sn] = s["years_experience"]

        # Must-have coverage
        missing_must = []
        must_met = []
        for job_skill, job_min_y in must_have:
            found = False
            cand_years = None
            for cname, cyears in cand_skill_years.items():
                if _skill_match(cname, job_skill):
                    found = True
                    cand_years = cyears
                    break
            if found:
                meets_years = (
                    job_min_y is None or cand_years is not None and cand_years >= job_min_y
                )
                must_met.append((job_skill, job_min_y, cand_years, meets_years))
            else:
                missing_must.append((job_skill, job_min_y))

        all_must_covered = len(missing_must) == 0
        all_years_met = all(m[3] for m in must_met)

        print(
            dedent(f"""
        ## Rank {rank}: {name}
        **Candidate partition:** `{cand_partition}`
        """).strip()
        )

        # Must-have summary
        if all_must_covered and all_years_met:
            print("### ✅ Must-haves: ALL COVERED (with years met)")
        elif all_must_covered and not all_years_met:
            print("### ⚠️ Must-haves: ALL PRESENT but some years below job min")
        else:
            print("### ❌ Must-haves: MISSING SKILLS")

        for job_skill, job_min_y, cand_years, meets in must_met:
            min_str = f" (job min: {job_min_y} yrs)" if job_min_y is not None else ""
            yrs_str = f"{cand_years} yrs" if cand_years is not None else "no years"
            status = "✅" if meets else "⚠️ below min"
            print(f"  - {job_skill}{min_str}: candidate has {yrs_str} {status}")

        for job_skill, job_min_y in missing_must:
            min_str = f" (job min: {job_min_y} yrs)" if job_min_y is not None else ""
            print(f"  - {job_skill}{min_str}: ❌ MISSING")

        # Years accuracy: compare candidate_skills vs normalized_json vs experiences
        print(
            "\n### Years of experience per skill (candidate_skills vs normalized_json vs experiences)"
        )
        nj_skills_map = {}
        if nj and isinstance(nj, dict):
            for sk in nj.get("skills") or []:
                if isinstance(sk, dict):
                    n = sk.get("name") or sk.get("skill")
                    if n:
                        nj_skills_map[n] = sk.get("years")

        if not cand_skills:
            print("  (No candidate_skills in DB — data may be in skills_summary only)")
        else:
            for s in cand_skills:
                sn = s["skill_name"] or "Unknown"
                cs_years = s["years_experience"]
                nj_years = None
                for nj_name, y in nj_skills_map.items():
                    if _skill_match(nj_name, sn):
                        nj_years = y
                        break
                # Find experiences that mention this skill
                exp_mentions = []
                for exp in cand_exps:
                    skills_used = exp["skills_used"] or []
                    exp_years = exp["years_experience"]
                    for su in skills_used:
                        if su and sn.lower() in (su or "").lower():
                            exp_mentions.append(
                                (exp["company_name"], exp["position_title"], exp_years)
                            )
                            break
                cs_str = f"{cs_years} yrs" if cs_years is not None else "—"
                nj_str = f"{nj_years} yrs" if nj_years is not None else "—"
                # Consistency check
                mismatch = False
                if cs_years is not None and nj_years is not None and cs_years != nj_years:
                    mismatch = True
                parts = [f"candidate_skills={cs_str}", f"normalized_json={nj_str}"]
                if exp_mentions:
                    exp_str = "; ".join(
                        f"{c} ({p}): {y}y" if y else f"{c} ({p})" for c, p, y in exp_mentions[:3]
                    )
                    if len(exp_mentions) > 3:
                        exp_str += f" (+{len(exp_mentions)-3} more)"
                    parts.append(f"experiences: {exp_str}")
                line = f"  - **{sn}**: {' | '.join(parts)}"
                if mismatch:
                    line += " ⚠️ years mismatch (candidate_skills vs normalized_json)"
                elif not exp_mentions and (cs_years or nj_years):
                    line += " (no explicit experience mention — LLM-inferred)"
                print(line)

        # Normalized JSON excerpt (skills from LLM output - CV normalization)
        if nj and isinstance(nj, dict):
            skills_in_json = nj.get("skills") or nj.get("technical_skills") or []
            if skills_in_json:
                print("\n### Skills in normalized_json (LLM-extracted from CV)")
                for sk in skills_in_json[:15]:
                    if isinstance(sk, dict):
                        n = sk.get("name") or sk.get("skill") or "?"
                        y = sk.get("years") or sk.get("years_experience")
                        ystr = f" ({y} yrs)" if y is not None else ""
                        prof = sk.get("proficiency")
                        prof_str = f" prof={prof}/10" if prof is not None else ""
                        print(f"  - {n}{ystr}{prof_str}")
                    else:
                        print(f"  - {sk}")
                if len(skills_in_json) > 15:
                    print(f"  ... and {len(skills_in_json) - 15} more")

        print("\n---")

    print(f"\n**Total matches:** {len(matches)}")
    cur.close()
    conn.close()


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: poetry run with-remote-db python scripts/analyze_matches.py <partition_id>",
            file=sys.stderr,
        )
        print(
            "Example: poetry run with-remote-db python scripts/analyze_matches.py recumPHbWDgLHf6jX",
            file=sys.stderr,
        )
        sys.exit(1)
    run_analysis(sys.argv[1].strip())


if __name__ == "__main__":
    main()
