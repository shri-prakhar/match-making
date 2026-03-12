#!/usr/bin/env python3
"""Run full ATS matchmaking pipeline for a single record: ingest → normalize → vectorize → score.

Usage:
    poetry run python scripts/run_ats_matchmaking.py recfZWUHZX43pVhTX
    On server: poetry run python scripts/run_ats_matchmaking.py --local recfZWUHZX43pVhTX

Steps:
    1. Fetch the ATS record from Airtable
    2. Ingest raw job to Postgres (with non_negotiables / nice_to_have)
    3. Normalize via LLM (new prompt with min_years / min_level per skill)
    4. Generate job vectors
    5. Score matches against all candidates
    6. Print results
"""

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from talent_matching.script_env import apply_local_db  # noqa: E402
from talent_matching.db import get_session  # noqa: E402
from talent_matching.llm.operations.embed_text import embed_text  # noqa: E402
from talent_matching.llm.operations.normalize_job import (  # noqa: E402
    DEFAULT_MODEL,
    PROMPT_VERSION,
    normalize_job,
)
from talent_matching.resources.matchmaking import MatchmakingResource  # noqa: E402
from talent_matching.resources.openrouter import OpenRouterResource  # noqa: E402
from talent_matching.sensors.ats_matchmaking_sensor import (  # noqa: E402
    _ingest_raw_job,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("ats_matchmaking")


def fetch_ats_record(record_id: str) -> dict:
    """Fetch a single ATS record from Airtable (returns raw Airtable JSON with 'id' and 'fields')."""
    from talent_matching.resources.airtable import AirtableATSResource

    ats = AirtableATSResource(
        base_id=os.environ["AIRTABLE_BASE_ID"],
        table_id=os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV"),
        api_key=os.environ["AIRTABLE_API_KEY"],
        write_api_key=os.getenv("AIRTABLE_WRITE_TOKEN"),
    )
    return ats.fetch_record_by_id(record_id)


def ingest_raw_job(ats_record: dict) -> dict:
    """Ingest raw job data to Postgres from ATS record, return mapped fields.

    The returned dict includes job_description resolved from Notion when the
    ATS record has a Job Description Link and no Job Description Text, so the
    LLM normalization step receives the same content that was stored.
    """
    from talent_matching.resources.notion import NotionResource

    notion = NotionResource(api_key=os.getenv("NOTION_API_KEY", ""))
    result = _ingest_raw_job(ats_record, notion, log)
    if not result:
        print("  FAILED: Could not ingest raw job (no description?)")
        sys.exit(1)
    _record_id, mapped = result
    return mapped


def run_normalize(
    raw_job_text: str,
    non_negotiables: str | None,
    nice_to_have: str | None,
    location_raw: str | None = None,
    projected_salary: str | None = None,
) -> dict:
    """Normalize job via LLM."""
    openrouter = OpenRouterResource(
        api_key=os.environ["OPENROUTER_API_KEY"],
        default_model="openai/gpt-4o-mini",
    )
    result = asyncio.run(
        normalize_job(
            openrouter,
            raw_job_text,
            non_negotiables=non_negotiables,
            nice_to_have=nice_to_have,
            location_raw=location_raw,
            projected_salary=projected_salary,
        )
    )
    print(f"  LLM cost: ${result.cost_usd:.4f}  tokens: {result.total_tokens}")
    return result.data


def store_normalized_job(record_id: str, data: dict, prompt_version: str, model: str):
    """Store normalized job via the IO manager logic."""
    from unittest.mock import MagicMock

    from talent_matching.io_managers.postgres import PostgresMetricsIOManager

    payload = {
        "airtable_record_id": record_id,
        **data,
        "normalized_json": data,
        "prompt_version": prompt_version,
        "model_version": model,
    }

    ctx = MagicMock()
    ctx.partition_key = record_id
    ctx.log = log

    io = PostgresMetricsIOManager()
    io._store_normalized_job(ctx, payload)


def generate_vectors(record_id: str, normalized_data: dict) -> list[dict]:
    """Generate job vectors (narratives + skill expected_capability)."""
    from talent_matching.assets.jobs import JOB_NARRATIVE_VECTOR_TYPES
    from talent_matching.skills.resolver import load_alias_map, resolve_skill_name, skill_vector_key

    narratives = normalized_data.get("narratives") or {}

    def _text(key: str) -> str:
        if key == "role_description":
            return narratives.get("role") or "No role description."
        return narratives.get(key) or f"No {key} narrative."

    texts_to_embed = [
        _text("experience"),
        _text("domain"),
        _text("personality"),
        _text("impact"),
        _text("technical"),
        _text("role_description"),
    ]
    vector_types = list(JOB_NARRATIVE_VECTOR_TYPES)

    requirements = normalized_data.get("requirements") or {}
    must_have = requirements.get("must_have_skills") or []
    nice_to_have = requirements.get("nice_to_have_skills") or []

    session = get_session()
    alias_map = load_alias_map(session)
    session.close()

    for entry in must_have + nice_to_have:
        if isinstance(entry, dict):
            name = (entry.get("name") or "").strip()
            cap = entry.get("expected_capability")
            if name and isinstance(cap, str) and cap.strip():
                canonical_name = resolve_skill_name(name, alias_map)
                texts_to_embed.append(f"{canonical_name}: {cap.strip()}")
                vector_types.append(skill_vector_key(canonical_name))

    openrouter = OpenRouterResource(
        api_key=os.environ["OPENROUTER_API_KEY"],
        default_model="openai/gpt-4o-mini",
    )
    result = asyncio.run(embed_text(openrouter, texts_to_embed))
    print(f"  Embedding cost: ${result.cost_usd:.4f}  vectors: {len(result.embeddings)}")

    from sqlalchemy import delete, select

    from talent_matching.models.raw import RawJob
    from talent_matching.models.vectors import JobVector

    session = get_session()
    raw_job = session.execute(
        select(RawJob).where(RawJob.airtable_record_id == record_id)
    ).scalar_one()
    raw_job_id = raw_job.id

    session.execute(delete(JobVector).where(JobVector.job_id == raw_job_id))

    for i, vt in enumerate(vector_types):
        session.add(
            JobVector(
                job_id=raw_job_id,
                vector_type=vt,
                vector=result.embeddings[i],
                model_version=result.model,
            )
        )
    session.commit()
    session.close()

    return [{"vector_type": vt} for vt in vector_types]


def print_normalized_skills(data: dict):
    """Print the structured skill requirements."""
    requirements = data.get("requirements") or {}
    must_have = requirements.get("must_have_skills") or []
    nice_to_have = requirements.get("nice_to_have_skills") or []

    print("\n  MUST-HAVE SKILLS:")
    for s in must_have:
        if isinstance(s, dict):
            name = s.get("name", "?")
            years = s.get("min_years")
            level = s.get("min_level")
            cap = s.get("expected_capability", "")
            parts = [f"    - {name}"]
            if years is not None:
                parts.append(f"min_years={years}")
            if level is not None:
                parts.append(f"min_level={level}")
            print("  ".join(parts))
            if cap:
                print(f"        {cap[:120]}")

    print("\n  NICE-TO-HAVE SKILLS:")
    for s in nice_to_have:
        if isinstance(s, dict):
            name = s.get("name", "?")
            years = s.get("min_years")
            level = s.get("min_level")
            parts = [f"    - {name}"]
            if years is not None:
                parts.append(f"min_years={years}")
            if level is not None:
                parts.append(f"min_level={level}")
            print("  ".join(parts))


def run_scoring_for_job(record_id: str):
    """Run the matchmaking scoring script for this specific job."""
    import psycopg2
    from pgvector.psycopg2 import register_vector
    from psycopg2.extras import RealDictCursor

    from scripts.run_matchmaking_scoring import (
        load_candidate_vectors,
        load_job_vectors,
        load_normalized_candidates,
        run_scoring,
    )

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    register_vector(conn)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """SELECT id, raw_job_id, job_title, job_category, company_name,
           salary_min, salary_max, min_years_experience, max_years_experience,
           location_type, timezone_requirements
           FROM normalized_jobs WHERE airtable_record_id = %s""",
        (record_id,),
    )
    jobs = [dict(r) for r in cur.fetchall()]
    cur.close()

    if not jobs:
        print("  No normalized job found!")
        conn.close()
        return

    candidates = load_normalized_candidates(conn)
    job_vecs = load_job_vectors(conn)
    cand_vecs = load_candidate_vectors(conn)
    conn.close()

    print(
        f"\n  Candidates: {len(candidates)}  Job vectors: {len(job_vecs)}  Candidate vectors: {len(cand_vecs)}"
    )

    matchmaking = MatchmakingResource()
    job_ids = [str(j["id"]) for j in jobs]
    cand_ids = [str(c["id"]) for c in candidates]
    job_required_skills = matchmaking.get_job_required_skills(job_ids)
    candidate_skills_map = matchmaking.get_candidate_skills(cand_ids)

    # Show required skills with min_years/min_level
    for jid, skills in job_required_skills.items():
        print(f"\n  Required skills for job {jid[:8]}...:")
        for s in skills:
            parts = [f"    {s['skill_name']} ({s['requirement_type']})"]
            if s.get("min_years") is not None:
                parts.append(f"min_years={s['min_years']}")
            if s.get("min_level") is not None:
                parts.append(f"min_level={s['min_level']}")
            print("  ".join(parts))

    matches = run_scoring(
        jobs, candidates, job_vecs, cand_vecs, job_required_skills, candidate_skills_map
    )

    if not matches:
        print("\n  No matches produced.")
        return

    print(f"\n{'=' * 80}")
    print(f"  TOP {min(15, len(matches))} MATCHES")
    print(f"{'=' * 80}")
    for m in matches[:15]:
        print(
            f"  #{m['rank']:2d}  {m['candidate_name']:<35s}  "
            f"score={m['combined_score']:5.2f}  "
            f"role={m['role_sim']:.3f}  domain={m['domain_sim']:.3f}  "
            f"skill={m['skills_match_score']:.3f}  exp={m['experience_match_score']:.3f}  "
            f"loc={m['location_match_score']:.3f}"
        )
        if m.get("matching_skills"):
            print(f"       matching: {', '.join(m['matching_skills'][:8])}")
        if m.get("missing_skills"):
            print(f"       missing:  {', '.join(m['missing_skills'][:5])}")


def main():
    apply_local_db()
    if len(sys.argv) < 2:
        print("Usage: poetry run python scripts/run_ats_matchmaking.py <ats_record_id>")
        sys.exit(1)

    record_id = sys.argv[1]
    print(f"\n{'=' * 80}")
    print(f"  ATS MATCHMAKING PIPELINE: {record_id}")
    print(f"{'=' * 80}")

    # Step 1: Fetch ATS record (returns raw Airtable JSON with "id" and "fields")
    print("\n[1/5] Fetching ATS record from Airtable...")
    ats_record = fetch_ats_record(record_id)
    fields = ats_record.get("fields", {})
    title = fields.get("Open Position (Job Title)", "Unknown")
    company = fields.get("Company", ["Unknown"])
    non_neg = fields.get("Non Negotiables") or None
    nice = fields.get("Nice-to-have") or None
    loc_values = fields.get("Preferred Location ", fields.get("Preferred Location", []))
    location_raw = ", ".join(loc_values) if isinstance(loc_values, list) and loc_values else None
    projected_salary = fields.get("Projected Salary") or None
    link = fields.get("Job Description Link")
    print(f"  Title: {title}")
    print(f"  Company: {company}")
    if link:
        print(f"  Job Description Link: {link[:120]}")
    if location_raw:
        print(f"  Location (raw): {location_raw[:200]}")
    if projected_salary:
        print(f"  Projected Salary: {projected_salary}")
    if non_neg:
        print(f"  Non-negotiables: {non_neg[:200]}")
    if nice:
        print(f"  Nice-to-have: {nice[:200]}")

    # Step 2: Ingest raw job (includes Notion fetch if link present)
    print("\n[2/5] Ingesting raw job to Postgres...")
    mapped = ingest_raw_job(ats_record)
    job_desc = mapped.get("job_description", "")
    print(f"  Job description: {len(job_desc)} chars")

    # Use recruiter fields from mapped data (may differ from raw Airtable fields)
    non_neg = mapped.get("non_negotiables") or non_neg
    nice = mapped.get("nice_to_have") or nice
    location_raw = mapped.get("location_raw") or location_raw
    projected_salary = mapped.get("projected_salary") or projected_salary

    # Step 3: Normalize via LLM
    print(f"\n[3/5] Normalizing job via LLM (prompt v{PROMPT_VERSION})...")
    normalized = run_normalize(
        job_desc, non_neg, nice, location_raw=location_raw, projected_salary=projected_salary
    )
    print_normalized_skills(normalized)

    # Step 4: Store normalized job
    print("\n[3b/5] Storing normalized job to Postgres...")
    store_normalized_job(record_id, normalized, PROMPT_VERSION, DEFAULT_MODEL)
    print("  Stored.")

    # Step 5: Generate vectors
    print("\n[4/5] Generating job vectors...")
    vectors = generate_vectors(record_id, normalized)
    print(f"  Generated {len(vectors)} vectors")

    # Step 6: Score matches
    print("\n[5/5] Scoring matches...")
    run_scoring_for_job(record_id)

    print(f"\n{'=' * 80}")
    print("  DONE")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    main()
