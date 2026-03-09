#!/usr/bin/env python3
"""Full matchmaking report: job, all matches, CVs, and fit scores.

For each matched candidate, loads raw CV text from the DB and prints:
- Stored scores (combined, vector, skills, comp, experience, location)
- Matching / missing skills
- Match reasoning, strengths, red flags (if present)
- CV excerpt (merged from cv_text + cv_text_pdf) and a short fit summary.

Usage:
    poetry run with-local-db python scripts/matchmaking_report.py <partition_id>
    poetry run with-remote-db python scripts/matchmaking_report.py recIqBsuF33YrIrMX

Output: printed to stdout. Redirect to a file to save:
    poetry run with-remote-db python scripts/matchmaking_report.py recIqBsuF33YrIrMX > report.md

For remote: poetry run remote-ui or poetry run local-matchmaking must be running.
"""

import os
import sys
from textwrap import dedent

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

from talent_matching.config.scoring import CULTURE_WEIGHT, DOMAIN_WEIGHT, ROLE_WEIGHT  # noqa: E402

# Max characters of CV to show per candidate (avoid huge output)
CV_EXCERPT_CHARS = 4000


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def _fmt_score(v):
    if v is None:
        return "—"
    return f"{float(v):.2f}"


def _vector_display(rs, ds, cs):
    if rs is None or ds is None or cs is None:
        return "—"
    w = ROLE_WEIGHT * rs + DOMAIN_WEIGHT * ds + CULTURE_WEIGHT * cs
    return (
        f"{w * 100:.1f}% (role {_fmt_score(rs)}, domain {_fmt_score(ds)}, culture {_fmt_score(cs)})"
    )


def _cv_text(raw: dict) -> str:
    """Merge CV sources: Airtable cv_text + extracted cv_text_pdf."""
    parts = []
    if raw.get("cv_text") and raw["cv_text"].strip():
        parts.append(raw["cv_text"].strip())
    if raw.get("cv_text_pdf") and raw["cv_text_pdf"].strip():
        pdf = raw["cv_text_pdf"].strip()
        if pdf not in (parts[0] if parts else ""):
            parts.append(pdf)
    return "\n\n---\n\n".join(parts) if parts else "(No CV text in DB)"


def run_report(partition_id: str) -> None:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ─── Job ─────────────────────────────────────────────────────
    cur.execute(
        """SELECT id, raw_job_id, job_title, company_name, airtable_record_id,
                  role_summary, job_description, seniority_level, min_years_experience
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
    cur.execute(
        """SELECT jrs.requirement_type, jrs.min_years, s.name AS skill_name
           FROM job_required_skills jrs
           LEFT JOIN skills s ON jrs.skill_id = s.id
           WHERE jrs.job_id = %s
           ORDER BY jrs.requirement_type, s.name""",
        (job_id,),
    )
    required_skills = cur.fetchall()
    must_have = [r["skill_name"] for r in required_skills if r["requirement_type"] == "must_have"]
    nice_to_have = [
        r["skill_name"] for r in required_skills if r["requirement_type"] == "nice_to_have"
    ]

    # ─── Matches with candidate IDs ───────────────────────────────
    cur.execute(
        """SELECT m.rank, m.match_score,
                  m.role_similarity_score, m.domain_similarity_score, m.culture_similarity_score,
                  m.skills_match_score, m.compensation_match_score,
                  m.experience_match_score, m.location_match_score,
                  m.matching_skills, m.missing_skills,
                  m.match_reasoning, m.strengths, m.red_flags,
                  nc.id AS candidate_id, nc.full_name, nc.airtable_record_id AS candidate_partition_id,
                  nc.professional_summary, nc.skills_summary, nc.current_role, nc.years_of_experience,
                  nc.location_country
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

    # ─── Print report ────────────────────────────────────────────
    print(
        dedent(f"""
    # Matchmaking Report
    ## Job: {job['job_title']} @ {job['company_name']}
    **Partition ID:** `{partition_id}`
    **DB:** {host}:{port}

    ### Role summary
    {job.get('role_summary') or '—'}

    ### Required skills
    - **Must-have:** {', '.join(must_have) if must_have else '—'}
    - **Nice-to-have:** {', '.join(nice_to_have) if nice_to_have else '—'}

    ### Scoring formula
    Combined = 40% vector (role/domain/culture) + 40% skills + 10% compensation + 10% location − seniority deduction (cap 20%).

    ---
    """).strip()
    )

    candidate_ids = [m["candidate_partition_id"] for m in matches if m["candidate_partition_id"]]
    raw_by_partition = {}
    if candidate_ids:
        cur.execute(
            """SELECT airtable_record_id, full_name, cv_text, cv_text_pdf, professional_summary
               FROM raw_candidates WHERE airtable_record_id = ANY(%s)""",
            (candidate_ids,),
        )
        for row in cur.fetchall():
            raw_by_partition[row["airtable_record_id"]] = dict(row)

    for m in matches:
        rank = m["rank"] or "—"
        name = (m["full_name"] or "—").strip()
        cid = m["candidate_partition_id"]
        combined = float(m["match_score"]) * 100 if m["match_score"] is not None else 0.0
        vec_str = _vector_display(
            m["role_similarity_score"],
            m["domain_similarity_score"],
            m["culture_similarity_score"],
        )
        raw = raw_by_partition.get(cid) or {}
        cv_full = _cv_text(raw)
        cv_excerpt = cv_full[:CV_EXCERPT_CHARS]
        cv_truncated = len(cv_full) > CV_EXCERPT_CHARS

        print(
            dedent(f"""
        ## Rank {rank}: {name}
        **Candidate partition:** `{cid}`

        ### Fit score
        - **Combined:** {combined:.1f}%
        - **Vector (role/domain/culture):** {vec_str}
        - **Skills fit:** {_fmt_score(m['skills_match_score'])} | **Compensation:** {_fmt_score(m['compensation_match_score'])} | **Experience:** {_fmt_score(m['experience_match_score'])} | **Location:** {_fmt_score(m['location_match_score'])}

        ### Skills vs role
        - **Matching:** {', '.join(m['matching_skills']) if m['matching_skills'] else '—'}
        - **Missing:** {', '.join(m['missing_skills']) if m['missing_skills'] else '—'}
        """).strip()
        )

        if m.get("match_reasoning"):
            print(f"\n**Match reasoning:** {m['match_reasoning']}")
        if m.get("strengths"):
            strengths = m["strengths"] if isinstance(m["strengths"], list) else [m["strengths"]]
            print(f"**Strengths:** {', '.join(strengths)}")
        if m.get("red_flags"):
            flags = m["red_flags"] if isinstance(m["red_flags"], list) else [m["red_flags"]]
            print(f"**Red flags:** {', '.join(flags)}")

        print("\n**Profile (normalized):**")
        print(f"  - Current role: {m.get('current_role') or '—'}")
        print(f"  - Years of experience: {m.get('years_of_experience') or '—'}")
        print(f"  - Location: {m.get('location_country') or '—'}")
        if m.get("professional_summary"):
            summary = (m["professional_summary"] or "")[:800]
            print(
                f"  - Summary: {summary}{'...' if len(m.get('professional_summary') or '') > 800 else ''}"
            )
        if m.get("skills_summary"):
            skills = (
                m["skills_summary"]
                if isinstance(m["skills_summary"], list)
                else [m["skills_summary"]]
            )
            print(f"  - Skills: {', '.join(skills[:25])}{' ...' if len(skills) > 25 else ''}")

        print("\n**CV excerpt:**")
        print("```")
        print(cv_excerpt)
        if cv_truncated:
            print(f"\n... [truncated; total {len(cv_full)} chars]")
        print("```")

        print("\n---")

    print(f"\n**Total matches:** {len(matches)}")
    cur.close()
    conn.close()


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: poetry run with-remote-db python scripts/matchmaking_report.py <partition_id>",
            file=sys.stderr,
        )
        print(
            "Example: poetry run with-remote-db python scripts/matchmaking_report.py recIqBsuF33YrIrMX",
            file=sys.stderr,
        )
        sys.exit(1)
    run_report(sys.argv[1].strip())


if __name__ == "__main__":
    main()
