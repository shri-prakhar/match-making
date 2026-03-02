#!/usr/bin/env python3
"""Fetch raw + normalized profile (CV/summary/skills) for a list of candidate full names.

Usage:
    poetry run python scripts/fetch_top_candidates_cvs.py

Output: prints for each candidate a block with professional_summary, skills, experience,
and normalized summary so we can judge fit from actual CV content.
"""

import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

# Top 15 from Growth Analyst match run (recIqBsuF33YrIrMX)
TOP_NAMES = [
    "Boryana Atanasova",
    "Yasen Grancharov",
    "Andrei Neda",
    "Stan Banchev",
    "Aurimas Sulnius",
    "Jack Land",
    "Ross Weinberger",
    "Neil Bamford",
    "Dan Aleksandrov",
    "Esther Ivy Felix - Ikejiofor",
    "Dag Fratric",
    "Thomas de Vries",
    "Matt C",
    "Denys Haponenko",
    "Philip Mostert",
]


def main():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )

    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Resolve names to partition ids (some names might have trailing space or different punctuation)
    name_to_partition = {}
    for name in TOP_NAMES:
        cur.execute(
            """SELECT airtable_record_id, full_name
               FROM normalized_candidates
               WHERE full_name = %s OR full_name ILIKE %s
               LIMIT 1""",
            (name, name.strip() + "%"),
        )
        row = cur.fetchone()
        if row:
            name_to_partition[row["full_name"]] = row["airtable_record_id"]
        else:
            # Try partial match
            cur.execute(
                """SELECT airtable_record_id, full_name
                   FROM normalized_candidates
                   WHERE full_name ILIKE %s
                   LIMIT 1""",
                (
                    "%"
                    + name.split()[0]
                    + "%"
                    + (name.split()[-1] if len(name.split()) > 1 else ""),
                ),
            )
            row = cur.fetchone()
            if row:
                name_to_partition[row["full_name"]] = row["airtable_record_id"]

    # For each partition_id, get raw + normalized
    for display_name in TOP_NAMES:
        partition_id = None
        for fn, pid in name_to_partition.items():
            if fn == display_name or (display_name in fn or fn in display_name):
                partition_id = pid
                break
        if not partition_id:
            # Try direct lookup
            cur.execute(
                "SELECT airtable_record_id, full_name FROM normalized_candidates WHERE full_name ILIKE %s LIMIT 1",
                ("%" + display_name[:30] + "%",),
            )
            row = cur.fetchone()
            if row:
                partition_id = row["airtable_record_id"]
                display_name = row["full_name"]

        if not partition_id:
            print(f"\n{'=' * 70}\n  NOT FOUND: {display_name}\n{'=' * 70}")
            continue

        cur.execute(
            "SELECT full_name, professional_summary, cv_text, cv_text_pdf, skills_raw, work_experience_raw FROM raw_candidates WHERE airtable_record_id = %s",
            (partition_id,),
        )
        raw = cur.fetchone()
        cur.execute(
            """SELECT full_name, professional_summary, skills_summary, current_role, companies_summary,
                      desired_job_categories, years_of_experience
               FROM normalized_candidates WHERE airtable_record_id = %s""",
            (partition_id,),
        )
        norm = cur.fetchone()

        print("\n" + "=" * 70)
        print(f"  {display_name}")
        print("=" * 70)
        if raw:
            summary = (raw.get("professional_summary") or "").strip()
            cv = (raw.get("cv_text") or raw.get("cv_text_pdf") or "").strip()
            skills_raw = (raw.get("skills_raw") or "").strip()
            work_raw = (raw.get("work_experience_raw") or "").strip()
            if summary:
                print("\n  [Professional summary]\n  " + summary[:2000].replace("\n", "\n  "))
            if cv and not summary:
                print("\n  [CV text]\n  " + cv[:2000].replace("\n", "\n  "))
            elif cv and len(cv) > 500:
                print("\n  [CV excerpt]\n  " + cv[:1200].replace("\n", "\n  ") + "...")
            if skills_raw:
                print("\n  [Skills raw] " + skills_raw[:500])
            if work_raw:
                print("\n  [Work experience raw] " + work_raw[:800].replace("\n", " "))
        if norm:
            print(
                "\n  [Normalized] current_role={} | years={} | desired_categories={}".format(
                    norm.get("current_role") or "—",
                    norm.get("years_of_experience"),
                    norm.get("desired_job_categories"),
                )
            )
            if norm.get("skills_summary"):
                print("  skills_summary: " + str(norm["skills_summary"])[:400])
            if norm.get("companies_summary"):
                print("  companies: " + str(norm["companies_summary"])[:300])
        print()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
