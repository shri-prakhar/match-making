#!/usr/bin/env python3
"""Inspect a candidate's normalized data by partition ID (airtable_record_id).

Usage:
    poetry run with-local-db python scripts/inspect_candidate.py <partition_id>
    poetry run with-remote-db python scripts/inspect_candidate.py rechGJvgloO4z6uYD
    On server: poetry run python scripts/inspect_candidate.py --local <partition_id>

This script displays all normalized information about a candidate including:
- Raw candidate data
- Normalized profile
- Skills, experiences, and projects (if populated)
- Vector embeddings status (pure prose narratives)

Vector Types (v3.0.0+):
- experience: Career journey, roles, progression (pure prose)
- domain: Industries, markets, ecosystems, protocols (pure prose)
- personality: Work style, values, culture signals (pure prose)
- impact: Scope, ownership, scale, measurable outcomes (pure prose)
- technical: Systems thinking, architecture, deep expertise (pure prose)
"""

import json
import os
import sys

# Project root so "from scripts.inspect_utils import ..." works when run as python scripts/inspect_candidate.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from talent_matching.script_env import apply_local_db
from scripts.inspect_utils import format_value, get_connection, print_field, print_section

load_dotenv()


def inspect_candidate(partition_id: str):
    """Inspect all data for a candidate."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ─────────────────────────────────────────────────────────────
    # RAW CANDIDATE
    # ─────────────────────────────────────────────────────────────
    print_section("RAW CANDIDATE DATA")

    cur.execute(
        "SELECT * FROM raw_candidates WHERE airtable_record_id = %s",
        (partition_id,),
    )
    raw = cur.fetchone()

    if not raw:
        print(f"  ❌ No raw candidate found with partition_id: {partition_id}")
        cur.close()
        conn.close()
        return

    # Show ALL raw candidate fields
    print_field("ID", raw["id"])
    print_field("Airtable Record ID", raw["airtable_record_id"])
    print_field("Source", raw["source"])
    print_field("Source ID", raw["source_id"])
    print_field("Full Name", raw["full_name"])
    print_field("Location", raw["location_raw"])
    print_field("Job Categories", raw["desired_job_categories_raw"])
    print_field("Skills", raw["skills_raw"])
    print_field("CV URL", raw["cv_url"])
    print_field("CV Text", raw["cv_text"])
    print_field("CV Text (PDF extracted)", raw.get("cv_text_pdf"), 0, 200)
    if raw.get("cv_extraction_method") or raw.get("cv_extraction_pages") is not None:
        print_field("CV Extraction Method", raw.get("cv_extraction_method"), 0)
        print_field("CV Extraction Pages", raw.get("cv_extraction_pages"), 0)
        print_field("CV Extraction Cost (USD)", raw.get("cv_extraction_cost_usd"), 0)
        print_field("CV Extraction Model", raw.get("cv_extraction_model"), 0)
    print_field("Professional Summary", raw["professional_summary"])
    print_field("Proof of Work", raw["proof_of_work"])
    print_field("Salary Range", raw["salary_range_raw"])
    print_field("X Profile", raw["x_profile_url"])
    print_field("LinkedIn", raw["linkedin_url"])
    print_field("Earn Profile", raw["earn_profile_url"])
    print_field("GitHub", raw["github_url"])
    print_field("Work Experience Raw", raw["work_experience_raw"])
    print_field("Job Status Raw", raw.get("job_status_raw"))
    print_field("Ingested At", raw["ingested_at"])
    print_field("Updated At", raw["updated_at"])
    print_field("Processing Status", raw["processing_status"])
    print_field("Processing Error", raw["processing_error"])

    # ─────────────────────────────────────────────────────────────
    # NORMALIZED CANDIDATE
    # ─────────────────────────────────────────────────────────────
    print_section("NORMALIZED CANDIDATE PROFILE")

    cur.execute(
        "SELECT * FROM normalized_candidates WHERE airtable_record_id = %s",
        (partition_id,),
    )
    normalized = cur.fetchone()

    if not normalized:
        print("  ❌ Not yet normalized (run the normalization pipeline)")
    else:
        normalized_id = normalized["id"]

        print("\n  📋 BASIC INFO")
        print_field("ID", normalized["id"], 1)
        print_field("Raw Candidate ID", normalized["raw_candidate_id"], 1)
        print_field("Full Name", normalized["full_name"], 1)
        print_field("Email", normalized["email"], 1)
        print_field("Phone", normalized["phone"], 1)
        print_field("Current Role", normalized["current_role"], 1)
        print_field("Seniority Level", normalized["seniority_level"], 1)
        print_field("Years of Experience", normalized["years_of_experience"], 1)

        print("\n  📍 LOCATION")
        print_field("City", normalized["location_city"], 1)
        print_field("Country", normalized["location_country"], 1)
        print_field("Region", normalized["location_region"], 1)
        print_field("Timezone", normalized["timezone"], 1)

        print("\n  💼 PROFESSIONAL")
        print_field("Summary", normalized["professional_summary"], 1)
        print_field("Job Categories", normalized["desired_job_categories"], 1)
        print_field("Skills", normalized["skills_summary"], 1)
        print_field("Companies", normalized["companies_summary"], 1)
        print_field("Notable Achievements", normalized["notable_achievements"], 1)
        print_field("Verified Communities", normalized["verified_communities"], 1)

        print("\n  📊 CAREER STATS")
        print_field("Job Count", normalized["job_count"], 1)
        print_field("Job Switches", normalized["job_switches_count"], 1)
        print_field("Average Tenure (months)", normalized["average_tenure_months"], 1)
        print_field("Longest Tenure (months)", normalized["longest_tenure_months"], 1)

        print("\n  💰 COMPENSATION")
        print_field("Min", normalized["compensation_min"], 1)
        print_field("Max", normalized["compensation_max"], 1)
        print_field("Currency", normalized["compensation_currency"], 1)

        print("\n  🎓 EDUCATION")
        print_field("Highest Degree", normalized["education_highest_degree"], 1)
        print_field("Field", normalized["education_field"], 1)
        print_field("Institution", normalized["education_institution"], 1)

        print("\n  🏆 HACKATHONS")
        print_field("Wins Count", normalized["hackathon_wins_count"], 1)
        print_field("Total Prize (USD)", normalized["hackathon_total_prize_usd"], 1)
        print_field("Solana Wins", normalized["solana_hackathon_wins"], 1)

        print("\n  🔗 SOCIAL HANDLES")
        print_field("X/Twitter", normalized["x_handle"], 1)
        print_field("LinkedIn", normalized["linkedin_handle"], 1)
        print_field("GitHub", normalized["github_handle"], 1)
        print_field("Total Followers", normalized["social_followers_total"], 1)

        print("\n  ✅ VERIFICATION")
        print_field("Status", normalized["verification_status"], 1)
        print_field("Notes", normalized["verification_notes"], 1)
        print_field("Verified By", normalized["verified_by"], 1)
        print_field("Verified At", normalized["verified_at"], 1)

        print("\n  🤖 LLM METADATA")
        print_field("Model Version", normalized["model_version"], 1)
        print_field("Prompt Version", normalized["prompt_version"], 1)
        print_field("Confidence Score", normalized["confidence_score"], 1)
        print_field("Skill Verification Score", normalized.get("skill_verification_score"), 1)
        print_field("Normalized At", normalized["normalized_at"], 1)

        # ─────────────────────────────────────────────────────────
        # GITHUB COMMIT HISTORY (Skill Verification)
        # ─────────────────────────────────────────────────────────
        cur.execute(
            """SELECT github_username, commit_history, fetched_at
               FROM candidate_github_commit_history
               WHERE candidate_id = %s""",
            (normalized_id,),
        )
        gh_commit = cur.fetchone()
        if gh_commit:
            print_section("GITHUB COMMIT HISTORY (Skill Verification)")
            print_field("Username", gh_commit["github_username"], 1)
            print_field("Fetched At", gh_commit["fetched_at"], 1)
            ch = gh_commit.get("commit_history") or {}
            repos = ch.get("repos", [])
            print_field("Repos Cloned", len(repos), 1)
            total_commits = sum(len(r.get("commits", [])) for r in repos)
            print_field("Total Commits", total_commits, 1)
        else:
            print_section("GITHUB COMMIT HISTORY (Skill Verification)")
            print("  (Not yet fetched - run candidate_github_commit_history asset)")

        # ─────────────────────────────────────────────────────────
        # SKILLS (Related Table)
        # ─────────────────────────────────────────────────────────
        print_section("CANDIDATE SKILLS (Related Table)")

        cur.execute(
            """SELECT cs.rating, cs.years_experience, cs.notable_achievement,
                      cs.verification_status, cs.verification_evidence, cs.verified_at,
                      s.name as skill_name
               FROM candidate_skills cs
               LEFT JOIN skills s ON cs.skill_id = s.id
               WHERE cs.candidate_id = %s
               ORDER BY cs.rating DESC NULLS LAST""",
            (normalized_id,),
        )
        skills = cur.fetchall()

        if not skills:
            print("  (No skills in related table - data is in skills_summary array)")
        else:
            for skill in skills:
                ver = skill.get("verification_status") or "—"
                ev = skill.get("verification_evidence")
                conf = ev.get("confidence") if isinstance(ev, dict) else None
                conf_str = ""
                if conf is not None:
                    try:
                        conf_str = f", confidence: {float(conf):.2f}"
                    except (TypeError, ValueError):
                        conf_str = f", confidence: {conf}"
                print(
                    f"  • {skill['skill_name'] or 'Unknown'} "
                    f"(rating: {skill['rating']}/10, years: {skill['years_experience']}, verified: {ver}{conf_str})"
                )
                if skill["notable_achievement"]:
                    print(f"    Achievement: {format_value(skill['notable_achievement'])}")
                if ev:
                    if isinstance(ev, dict):
                        print(f"    Evidence: {ev.get('snippet', ev)}")
                    else:
                        print(f"    Evidence: {format_value(ev)}")

        # ─────────────────────────────────────────────────────────
        # WORK EXPERIENCE (Related Table)
        # ─────────────────────────────────────────────────────────
        print_section("WORK EXPERIENCE (Related Table)")

        cur.execute(
            """SELECT company_name, position_title, years_experience, description,
                      skills_used, is_current, start_date, end_date
               FROM candidate_experiences WHERE candidate_id = %s
               ORDER BY position_order, start_date DESC NULLS LAST""",
            (normalized_id,),
        )
        experiences = cur.fetchall()

        if not experiences:
            print("  (No experiences in related table - data is in companies_summary array)")
        else:
            for i, exp in enumerate(experiences, 1):
                current = " (current)" if exp["is_current"] else ""
                print(f"\n  [{i}] {exp['position_title']} at {exp['company_name']}{current}")
                print_field("Years", exp["years_experience"], 2)
                end_str = "present" if exp["is_current"] else (exp["end_date"] or "—")
                print_field("Period", f"{exp['start_date'] or '—'} to {end_str}", 2)
                print_field("Description", exp["description"], 2)
                print_field("Skills Used", exp["skills_used"], 2)

        # ─────────────────────────────────────────────────────────
        # PROJECTS (Related Table)
        # ─────────────────────────────────────────────────────────
        print_section("PROJECTS (Related Table)")

        cur.execute(
            """SELECT project_name, description, url, technologies,
                      is_hackathon, hackathon_name, prize_won, prize_amount_usd, year
               FROM candidate_projects WHERE candidate_id = %s
               ORDER BY project_order, year DESC NULLS LAST""",
            (normalized_id,),
        )
        projects = cur.fetchall()

        if not projects:
            print("  (No projects in related table)")
        else:
            for i, proj in enumerate(projects, 1):
                hackathon = " 🏆" if proj["is_hackathon"] else ""
                print(f"\n  [{i}] {proj['project_name']}{hackathon}")
                print_field("Description", proj["description"], 2)
                print_field("URL", proj["url"], 2)
                print_field("Technologies", proj["technologies"], 2)
                print_field("Year", proj["year"], 2)
                if proj["is_hackathon"]:
                    print_field("Hackathon", proj["hackathon_name"], 2)
                    print_field("Prize Won", proj["prize_won"], 2)
                    print_field("Prize Amount (USD)", proj["prize_amount_usd"], 2)

        # ─────────────────────────────────────────────────────────
        # SOFT ATTRIBUTES (Related Table)
        # ─────────────────────────────────────────────────────────
        print_section("SOFT ATTRIBUTES (LLM-Assessed)")

        cur.execute(
            """SELECT leadership_score, autonomy_score, technical_depth_score,
                      communication_score, growth_trajectory_score, reasoning,
                      rating_model, rated_at
               FROM candidate_attributes WHERE candidate_id = %s""",
            (normalized_id,),
        )
        attributes = cur.fetchone()

        if not attributes:
            print("  (No soft attributes yet - run normalization pipeline)")
        else:
            print("  Scores (1-5 scale):\n")
            print_field("Leadership", f"{attributes['leadership_score']}/5", 1)
            print_field("Autonomy", f"{attributes['autonomy_score']}/5", 1)
            print_field("Technical Depth", f"{attributes['technical_depth_score']}/5", 1)
            print_field("Communication", f"{attributes['communication_score']}/5", 1)
            print_field("Growth Trajectory", f"{attributes['growth_trajectory_score']}/5", 1)
            print()
            print_field("Rating Model", attributes["rating_model"], 1)
            print_field("Rated At", attributes["rated_at"], 1)
            if attributes["reasoning"]:
                print("\n  📝 Reasoning (JSON):")
                print(f"    {format_value(attributes['reasoning'], max_length=500)}")

        # ─────────────────────────────────────────────────────────
        # NARRATIVES (Pure Prose for Vectorization)
        # ─────────────────────────────────────────────────────────
        print_section("NARRATIVES (Pure Prose for Vectorization)")

        # Narratives are stored in normalized_json
        normalized_json = normalized.get("normalized_json")
        if normalized_json:
            if isinstance(normalized_json, str):
                normalized_json = json.loads(normalized_json)

            narratives = normalized_json.get("narratives", {})
            if narratives:
                narrative_types = ["experience", "domain", "personality", "impact", "technical"]
                for ntype in narrative_types:
                    text = narratives.get(ntype)
                    if text:
                        print(f"\n  📖 {ntype.upper()}")
                        # Wrap text nicely
                        wrapped = format_value(text, max_length=500)
                        print(f"     {wrapped}")
                    else:
                        print(f"\n  ❌ {ntype.upper()}: (missing)")
            else:
                print("  ❌ No narratives found in normalized_json")
                print("     (Prompt version may be < 4.0.0, or LLM didn't generate them)")
        else:
            print("  ❌ No normalized_json data available")

    # ─────────────────────────────────────────────────────────────
    # VECTOR EMBEDDINGS
    # ─────────────────────────────────────────────────────────────
    print_section("VECTOR EMBEDDINGS")

    print("  ℹ️  Vector categories:")
    print("     📝 Narratives: experience, domain, personality, impact, technical")
    print("     🔧 Skills: skill_{name} - structured with proficiency level")
    print("     💼 Positions: position_{n} - job descriptions")
    print("     🚀 Projects: project_{n} - description + technologies")
    print()

    # Expected narrative vector types
    narrative_vectors = {"experience", "domain", "personality", "impact", "technical"}

    if normalized:
        # Vectors are stored by raw_candidate_id, not normalized_candidates.id
        raw_cand_id = normalized["raw_candidate_id"]
        cur.execute(
            """SELECT vector_type, model_version, created_at,
                      vector_dims(vector) as dimensions
               FROM candidate_vectors WHERE candidate_id = %s
               ORDER BY vector_type""",
            (raw_cand_id,),
        )
        vectors = cur.fetchall()

        if not vectors:
            print("  ❌ No embeddings yet (run the candidate_vectors asset)")
        else:
            # Categorize vectors
            narratives = []
            skills = []
            positions = []
            projects = []
            other = []

            for vec in vectors:
                vtype = vec["vector_type"]
                if vtype in narrative_vectors:
                    narratives.append(vec)
                elif vtype.startswith("skill_"):
                    skills.append(vec)
                elif vtype.startswith("position_"):
                    positions.append(vec)
                elif vtype.startswith("project_"):
                    projects.append(vec)
                else:
                    other.append(vec)

            def print_vector(vec, indent=2):
                spaces = " " * indent
                print(
                    f"{spaces}✅ {vec['vector_type']}: {vec['dimensions']} dims "
                    f"(model: {vec['model_version']})"
                )

            # Print narratives
            if narratives:
                print("  📝 NARRATIVE VECTORS")
                for vec in sorted(narratives, key=lambda x: x["vector_type"]):
                    print_vector(vec, 4)
                print()

            # Print skills
            if skills:
                print(f"  🔧 SKILL VECTORS ({len(skills)} skills)")
                for vec in sorted(skills, key=lambda x: x["vector_type"]):
                    # Extract skill name from skill_xxx format
                    skill_name = vec["vector_type"].replace("skill_", "").replace("_", " ").title()
                    print(f"    ✅ {skill_name}: {vec['dimensions']} dims")
                print()

            # Print positions
            if positions:
                print(f"  💼 POSITION VECTORS ({len(positions)} jobs)")
                for vec in sorted(positions, key=lambda x: int(x["vector_type"].split("_")[1])):
                    idx = vec["vector_type"].split("_")[1]
                    print(f"    ✅ Position {idx}: {vec['dimensions']} dims")
                print()

            # Print projects
            if projects:
                print(f"  🚀 PROJECT VECTORS ({len(projects)} projects)")
                for vec in sorted(projects, key=lambda x: int(x["vector_type"].split("_")[1])):
                    idx = vec["vector_type"].split("_")[1]
                    print(f"    ✅ Project {idx}: {vec['dimensions']} dims")
                print()

            # Print legacy/other vectors
            if other:
                print("  ⚠️  OTHER/LEGACY VECTORS (will be replaced on re-materialization)")
                for vec in sorted(other, key=lambda x: x["vector_type"]):
                    print_vector(vec, 4)
                print()

            # Summary
            total = len(vectors)
            print(f"  📊 TOTAL: {total} vectors")

            # Check for missing narratives
            found_narratives = {v["vector_type"] for v in narratives}
            missing = narrative_vectors - found_narratives
            if missing:
                print(f"  ⚠️  Missing narratives: {', '.join(sorted(missing))}")
    else:
        print("  ❌ Candidate not normalized yet - no vectors possible")

    cur.close()
    conn.close()

    print()
    print("=" * 60)
    print("  END OF REPORT")
    print("=" * 60)


def main():
    apply_local_db()
    if len(sys.argv) < 2:
        print("Usage: python scripts/inspect_candidate.py <partition_id>")
        print("Example: python scripts/inspect_candidate.py rechGJvgloO4z6uYD")
        sys.exit(1)

    partition_id = sys.argv[1]
    print(f"\n🔍 Inspecting candidate: {partition_id}\n")
    inspect_candidate(partition_id)


if __name__ == "__main__":
    main()
