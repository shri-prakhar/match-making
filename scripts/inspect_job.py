#!/usr/bin/env python3
"""Inspect a job's normalized data by partition ID (airtable_record_id).

Usage:
    poetry run with-local-db python scripts/inspect_job.py <partition_id>
    poetry run with-remote-db python scripts/inspect_job.py recXXXXXXXXXXXXXX
    On server: poetry run python scripts/inspect_job.py --local <partition_id>

This script displays all normalized information about a job including:
- Raw job data
- Normalized job profile
- Required skills (job_required_skills)
- Narrative prose (for vectorization)
- Job vector embeddings status

Vector Types (v2.0.0+):
- experience: Career journey, roles, progression (pure prose)
- domain: Industries, markets, ecosystems (pure prose)
- personality: Work style, values, culture signals (pure prose)
- impact: Scope, ownership, measurable outcomes (pure prose)
- technical: Systems thinking, architecture, deep expertise (pure prose)
- role_description: Full job responsibilities and day-to-day work (pure prose)
"""

import json
import os
import sys

# Project root so "from scripts.inspect_utils import ..." works when run as python scripts/inspect_job.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from talent_matching.script_env import apply_local_db
from scripts.inspect_utils import format_value, get_connection, print_field, print_section

load_dotenv()

# Aligned with talent_matching/assets/jobs.py JOB_NARRATIVE_VECTOR_TYPES
JOB_NARRATIVE_VECTOR_TYPES = [
    "experience",
    "domain",
    "personality",
    "impact",
    "technical",
    "role_description",
]


def inspect_job(partition_id: str):
    """Inspect all data for a job."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ─────────────────────────────────────────────────────────────
    # RAW JOB
    # ─────────────────────────────────────────────────────────────
    print_section("RAW JOB DATA")

    cur.execute(
        "SELECT * FROM raw_jobs WHERE airtable_record_id = %s",
        (partition_id,),
    )
    raw = cur.fetchone()

    if not raw:
        print(f"  ❌ No raw job found with partition_id: {partition_id}")
        cur.close()
        conn.close()
        return

    print_field("ID", raw["id"])
    print_field("Airtable Record ID", raw["airtable_record_id"])
    print_field("Source", raw["source"])
    print_field("Source ID", raw["source_id"])
    print_field("Source URL", raw["source_url"])
    print_field("Job Title", raw["job_title"])
    print_field("Company Name", raw["company_name"])
    print_field("Job Description", raw["job_description"])
    print_field("Company Website URL", raw["company_website_url"])
    print_field("Experience Level Raw", raw["experience_level_raw"])
    print_field("Location Raw", raw["location_raw"])
    print_field("Work Setup Raw", raw["work_setup_raw"])
    print_field("Status Raw", raw["status_raw"])
    print_field("Job Category Raw", raw["job_category_raw"])
    print_field("X URL", raw["x_url"])
    print_field("Ingested At", raw["ingested_at"])
    print_field("Updated At", raw["updated_at"])
    print_field("Processing Status", raw["processing_status"])
    print_field("Processing Error", raw["processing_error"])

    raw_job_id = raw["id"]

    # ─────────────────────────────────────────────────────────────
    # NORMALIZED JOB
    # ─────────────────────────────────────────────────────────────
    print_section("NORMALIZED JOB PROFILE")

    cur.execute(
        "SELECT * FROM normalized_jobs WHERE airtable_record_id = %s",
        (partition_id,),
    )
    normalized = cur.fetchone()

    if not normalized:
        print("  ❌ Not yet normalized (run the normalization pipeline)")
    else:
        normalized_id = normalized["id"]

        print("\n  📋 JOB IDENTITY")
        print_field("ID", normalized["id"], 1)
        print_field("Raw Job ID", normalized["raw_job_id"], 1)
        print_field("Job Title", normalized["job_title"], 1)
        print_field("Job Category", normalized["job_category"], 1)
        print_field("Role Type", normalized["role_type"], 1)

        print("\n  🏢 COMPANY")
        print_field("Company Name", normalized["company_name"], 1)
        print_field("Company Stage", normalized["company_stage"], 1)
        print_field("Company Size", normalized["company_size"], 1)
        print_field("Company Website", normalized["company_website"], 1)
        print_field("Company X URL", normalized["company_x_url"], 1)

        print("\n  📝 JOB DETAILS")
        print_field("Job Description", normalized["job_description"], 1, max_length=400)
        print_field("Role Summary", normalized["role_summary"], 1)
        print_field("Responsibilities", normalized["responsibilities"], 1)
        print_field("Nice to Haves", normalized["nice_to_haves"], 1)
        print_field("Benefits", normalized["benefits"], 1)
        print_field("Team Context", normalized["team_context"], 1)

        print("\n  📐 REQUIREMENTS")
        print_field("Seniority Level", normalized["seniority_level"], 1)
        print_field("Education Required", normalized["education_required"], 1)
        print_field("Domain Experience", normalized["domain_experience"], 1)
        print_field("Tech Stack", normalized["tech_stack"], 1)
        print_field("Min Years Experience", normalized["min_years_experience"], 1)
        print_field("Max Years Experience", normalized["max_years_experience"], 1)

        print("\n  📍 LOCATION & WORK TYPE")
        print_field("Location Type", normalized["location_type"], 1)
        print_field("Locations", normalized["locations"], 1)
        print_field("Timezone Requirements", normalized["timezone_requirements"], 1)
        print_field(
            "Employment Type", normalized["employment_type"], 1
        )  # list: full_time, part_time, contract

        print("\n  💰 COMPENSATION")
        print_field("Salary Min", normalized["salary_min"], 1)
        print_field("Salary Max", normalized["salary_max"], 1)
        print_field("Salary Currency", normalized["salary_currency"], 1)
        print_field("Has Equity", normalized["has_equity"], 1)
        print_field("Equity Details", normalized["equity_details"], 1)
        print_field("Has Token Compensation", normalized["has_token_compensation"], 1)

        print("\n  📊 SOFT ATTRIBUTE REQUIREMENTS (min scores 1–5)")
        print_field("Min Leadership", normalized["min_leadership_score"], 1)
        print_field("Min Autonomy", normalized["min_autonomy_score"], 1)
        print_field("Min Technical Depth", normalized["min_technical_depth_score"], 1)
        print_field("Min Communication", normalized["min_communication_score"], 1)
        print_field("Min Growth Trajectory", normalized["min_growth_trajectory_score"], 1)

        print("\n  📌 STATUS & PRIORITY")
        print_field("Status", normalized["status"], 1)
        print_field("Priority", normalized["priority"], 1)
        print_field("Posted Date", normalized["posted_date"], 1)
        print_field("Deadline Date", normalized["deadline_date"], 1)
        print_field("Is Urgent", normalized["is_urgent"], 1)

        print("\n  👤 CONTACT")
        print_field("Hiring Manager Name", normalized["hiring_manager_name"], 1)
        print_field("Hiring Manager Email", normalized["hiring_manager_email"], 1)
        print_field("Application URL", normalized["application_url"], 1)

        print("\n  🤖 LLM METADATA")
        print_field("Model Version", normalized["model_version"], 1)
        print_field("Prompt Version", normalized["prompt_version"], 1)
        print_field("Confidence Score", normalized["confidence_score"], 1)
        print_field("Normalized At", normalized["normalized_at"], 1)

        # ─────────────────────────────────────────────────────────
        # REQUIRED SKILLS (Related Table)
        # ─────────────────────────────────────────────────────────
        print_section("JOB REQUIRED SKILLS (Related Table)")

        cur.execute(
            """SELECT jrs.requirement_type, jrs.min_years, s.name as skill_name
               FROM job_required_skills jrs
               LEFT JOIN skills s ON jrs.skill_id = s.id
               WHERE jrs.job_id = %s
               ORDER BY jrs.requirement_type, s.name""",
            (normalized_id,),
        )
        required_skills = cur.fetchall()

        if not required_skills:
            print("  (No required skills in related table)")
        else:
            for rs in required_skills:
                years = f", min {rs['min_years']} years" if rs["min_years"] is not None else ""
                print(f"  • {rs['skill_name'] or 'Unknown'} ({rs['requirement_type']}{years})")

        # ─────────────────────────────────────────────────────────
        # SCORING WEIGHTS (per job category; used by matches asset)
        # ─────────────────────────────────────────────────────────
        print_section("SCORING WEIGHTS (for this job category)")
        job_category = (normalized.get("job_category") or raw.get("job_category_raw") or "").strip()
        if job_category:
            cur.execute(
                """SELECT job_category, role_weight, domain_weight, culture_weight, impact_weight, technical_weight,
                          vector_weight, skill_fit_weight, compensation_weight, location_weight, seniority_scale_weight,
                          skill_rating_weight, skill_semantic_weight, seniority_max_deduction, seniority_level_max_deduction,
                          tenure_instability_max_deduction, updated_at
                   FROM scoring_weights WHERE job_category = %s""",
                (job_category,),
            )
            weights_row = cur.fetchone()
            if weights_row:
                print(f"  Category: {weights_row['job_category']}  (from DB, updated {format_value(weights_row['updated_at'])})")
                print("  Vector sub-weights (role/domain/culture/impact/technical):")
                print_field("    role", round(weights_row["role_weight"], 4), 0)
                print_field("    domain", round(weights_row["domain_weight"], 4), 0)
                print_field("    culture", round(weights_row["culture_weight"], 4), 0)
                print_field("    impact", round(weights_row["impact_weight"], 4), 0)
                print_field("    technical", round(weights_row["technical_weight"], 4), 0)
                print("  Top-level blend:")
                print_field("    vector_weight", round(weights_row["vector_weight"], 4), 0)
                print_field("    skill_fit_weight", round(weights_row["skill_fit_weight"], 4), 0)
                print_field("    compensation_weight", round(weights_row["compensation_weight"], 4), 0)
                print_field("    location_weight", round(weights_row["location_weight"], 4), 0)
                print_field("    seniority_scale_weight", round(weights_row["seniority_scale_weight"], 4), 0)
                print("  Skill fit sub-weights:")
                print_field("    skill_rating_weight", round(weights_row["skill_rating_weight"], 4), 0)
                print_field("    skill_semantic_weight", round(weights_row["skill_semantic_weight"], 4), 0)
                print("  Max deductions:")
                print_field("    seniority_max_deduction", round(weights_row["seniority_max_deduction"], 4), 0)
                print_field("    seniority_level_max_deduction", round(weights_row["seniority_level_max_deduction"], 4), 0)
                print_field("    tenure_instability_max_deduction", round(weights_row["tenure_instability_max_deduction"], 4), 0)
            else:
                print(f"  Category: {job_category}")
                print("  (No row in scoring_weights; matchmaking uses config defaults.)")
        else:
            print("  (No job_category; matchmaking uses config defaults.)")

        # ─────────────────────────────────────────────────────────
        # NARRATIVES (Pure Prose for Vectorization)
        # ─────────────────────────────────────────────────────────
        print_section("NARRATIVES (Pure Prose for Vectorization)")

        # Prefer normalized_json.narratives (experience, domain, personality, impact, technical, role_description)
        normalized_json = normalized.get("normalized_json")
        if normalized_json and isinstance(normalized_json, str):
            normalized_json = json.loads(normalized_json)
        narratives = (normalized_json or {}).get("narratives", {}) if normalized_json else {}
        display_order = [
            ("experience", "EXPERIENCE"),
            ("domain", "DOMAIN"),
            ("personality", "PERSONALITY"),
            ("impact", "IMPACT"),
            ("technical", "TECHNICAL"),
            ("role_description", "ROLE"),
            ("role", "ROLE"),
        ]
        seen_role = False
        has_any = False
        for key, label in display_order:
            if key == "role" and seen_role:
                continue
            text = narratives.get(key) or (
                normalized.get("narrative_role") if key in ("role_description", "role") else normalized.get(f"narrative_{key}")
            )
            if key in ("role_description", "role"):
                seen_role = True
            if text:
                has_any = True
                print(f"\n  📖 {label}")
                print(f"     {format_value(text, max_length=500)}")
            else:
                print(f"\n  ❌ {label}: (missing)")

        if not has_any:
            print("  ❌ No narratives in normalized_json or narrative_* columns")

    # ─────────────────────────────────────────────────────────────
    # JOB VECTORS (stored by raw_jobs.id; same key used by job_vectors asset)
    # ─────────────────────────────────────────────────────────────
    print_section("JOB VECTOR EMBEDDINGS")

    print("  ℹ️  Vector categories (narrative-based, v2.0.0+):")
    for vt in JOB_NARRATIVE_VECTOR_TYPES:
        print(f"     • {vt}")
    print()

    # job_vectors.job_id FK references raw_jobs.id; use string for reliable param binding
    cur.execute(
        """SELECT vector_type, model_version, created_at,
                  vector_dims(vector) as dimensions
           FROM job_vectors WHERE job_id = %s
           ORDER BY vector_type""",
        (str(raw_job_id),),
    )
    vectors = cur.fetchall()

    if not vectors:
        print("  ❌ No embeddings yet (run the job_vectors asset)")
        if not normalized:
            print("  (Normalized job is also missing; job_vectors depends on normalized_jobs.)")
    else:
        narrative_set = set(JOB_NARRATIVE_VECTOR_TYPES)
        narratives = [v for v in vectors if v["vector_type"] in narrative_set]
        other = [v for v in vectors if v["vector_type"] not in narrative_set]

        def print_vector(vec, indent=2):
            spaces = "  " * indent
            print(
                f"{spaces}✅ {vec['vector_type']}: {vec['dimensions']} dims "
                f"(model: {vec['model_version']})"
            )

        if narratives:
            print("  📝 NARRATIVE VECTORS")
            for vec in sorted(narratives, key=lambda x: x["vector_type"]):
                print_vector(vec, 4)
            print()

        if other:
            print("  ⚠️  OTHER VECTORS")
            for vec in sorted(other, key=lambda x: x["vector_type"]):
                print_vector(vec, 4)
            print()

        print(f"  📊 TOTAL: {len(vectors)} vectors")

        found = {v["vector_type"] for v in narratives}
        missing = narrative_set - found
        if missing:
            print(f"  ⚠️  Missing narratives: {', '.join(sorted(missing))}")

    cur.close()
    conn.close()

    print()
    print("=" * 60)
    print("  END OF REPORT")
    print("=" * 60)


def main():
    apply_local_db()
    if len(sys.argv) < 2:
        print("Usage: python scripts/inspect_job.py <partition_id>")
        print("Example: python scripts/inspect_job.py recXXXXXXXXXXXXXX")
        sys.exit(1)

    partition_id = sys.argv[1]
    print(f"\n🔍 Inspecting job: {partition_id}\n")
    inspect_job(partition_id)


if __name__ == "__main__":
    main()
