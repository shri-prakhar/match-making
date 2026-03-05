"""Candidate pipeline assets.

This module defines the asset graph for processing candidate data:
1. airtable_candidates: Candidates fetched from Airtable (partitioned per record)
2. raw_candidates: Raw candidate data stored in PostgreSQL (with PDF extraction)
3. normalized_candidates: LLM-normalized structured profiles
4. candidate_vectors: Semantic embeddings for matching

The pipeline uses dynamic partitions to process each candidate independently,
enabling incremental processing and per-record change tracking.
"""

import asyncio
import json
import os
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetIn,
    DataVersion,
    DynamicPartitionsDefinition,
    Output,
    asset,
)

from talent_matching.db import get_session
from talent_matching.llm import PDFEngine, embed_text, extract_pdf_from_url, normalize_cv
from talent_matching.models.enums import PROFICIENCY_LEVELS
from talent_matching.skills.github_verification import (
    clone_and_extract_commits,
    fetch_repo_metadata,
    verify_skills_llm,
)
from talent_matching.skills.resolver import load_alias_map, resolve_skill_name, skill_vector_key
from talent_matching.utils.airtable_mapper import normalized_candidate_to_airtable_fields
from talent_matching.utils.dagster_async import run_with_interrupt_check

# Dynamic partition definition for candidates
# Each candidate record gets its own partition key (Airtable record ID)
candidate_partitions = DynamicPartitionsDefinition(name="candidates")


@asset(
    partitions_def=candidate_partitions,
    description="Single candidate record fetched from Airtable",
    group_name="candidates",
    required_resource_keys={"airtable"},
    op_tags={"dagster/concurrency_key": "airtable_api"},
    metadata={
        "source": "airtable",
    },
)
def airtable_candidates(context: AssetExecutionContext) -> Output[dict[str, Any]]:
    """Fetch a single candidate from Airtable by partition key.

    Each partition key corresponds to an Airtable record ID.
    The asset tracks data versions per record to enable change detection.

    Returns:
        Output with the candidate data and a DataVersion for staleness tracking.
    """
    record_id = context.partition_key
    context.log.info(f"Fetching candidate record: {record_id}")

    airtable = context.resources.airtable
    candidate = airtable.fetch_record_by_id(record_id)

    # Extract data version for Dagster's change detection
    data_version = candidate.pop("_data_version", None)

    context.log.info(
        f"Fetched candidate: {candidate.get('full_name', 'Unknown')} (version: {data_version})"
    )

    return Output(
        value=candidate,
        data_version=DataVersion(data_version) if data_version else None,
    )


@asset(
    partitions_def=candidate_partitions,
    ins={"airtable_candidates": AssetIn()},
    description="Raw candidate data with PDF extraction (always extracts when cv_url exists)",
    group_name="candidates",
    io_manager_key="postgres_io",
    required_resource_keys={"openrouter"},
    code_version="1.3.0",  # v1.3.0: Handle PDF extraction failures explicitly
    op_tags={
        "dagster/concurrency_key": "openrouter_api",
    },
    metadata={
        "table": "raw_candidates",
    },
)
def raw_candidates(
    context: AssetExecutionContext,
    airtable_candidates: dict[str, Any],
) -> dict[str, Any]:
    """Store raw candidate data with PDF text extraction.

    This asset:
    1. Preserves original cv_text from Airtable (if any)
    2. ALWAYS extracts text from cv_url PDF (if available)
    3. Stores both separately: cv_text (Airtable) + cv_text_pdf (extracted)

    The normalization step will merge both sources to capture all information.
    PDF extraction uses OpenRouter's pdf-text engine (free) with GPT-4o-mini.
    """
    record_id = context.partition_key
    openrouter = context.resources.openrouter
    context.log.info(f"Processing raw candidate: {record_id}")

    # Preserve original Airtable cv_text
    cv_text_airtable = airtable_candidates.get("cv_text")
    cv_text_pdf = None

    # Initialize extraction metadata
    cv_extraction_method = None
    cv_extraction_cost_usd = None
    cv_extraction_model = None
    cv_extraction_pages = None

    # Always extract from PDF if cv_url exists
    cv_url = airtable_candidates.get("cv_url")

    if cv_url:
        context.log.info(f"Extracting text from PDF: {cv_url[:60]}...")

        # Set context for cost tracking
        openrouter.set_context(
            run_id=context.run_id,
            asset_key="raw_candidates",
            partition_key=record_id,
        )

        # Extract text using OpenRouter
        result = asyncio.run(
            extract_pdf_from_url(
                openrouter,
                cv_url,
                model="openai/gpt-4o-mini",
                pdf_engine=PDFEngine.PDF_TEXT,  # Free engine
            )
        )

        cv_extraction_cost_usd = result.cost_usd
        cv_extraction_model = result.model
        cv_extraction_pages = result.pages_processed

        if result.failed:
            cv_extraction_method = "failed"
            context.log.warning(f"PDF extraction failed for {record_id}: {result.error}")
        elif result.text and len(result.text.strip()) > 50:
            cv_text_pdf = result.text
            cv_extraction_method = "pdf_text"
            context.log.info(
                f"PDF extraction successful: {len(cv_text_pdf)} chars, "
                f"${result.cost_usd:.6f}, {result.pages_processed} pages"
            )
        else:
            cv_extraction_method = "failed"
            context.log.warning(f"PDF extraction returned empty/short text for {record_id}")

        extraction_metadata: dict[str, Any] = {
            "cv_extraction_method": cv_extraction_method,
            "cv_extraction_cost_usd": cv_extraction_cost_usd or 0,
            "cv_extraction_model": cv_extraction_model or "none",
            "cv_extraction_pages": cv_extraction_pages or 0,
            "cv_text_pdf_length": len(cv_text_pdf) if cv_text_pdf else 0,
            "cv_text_airtable_length": len(cv_text_airtable) if cv_text_airtable else 0,
        }
        if result.error:
            extraction_metadata["cv_extraction_error"] = result.error
        context.add_output_metadata(extraction_metadata)
    else:
        context.log.info("No cv_url available for PDF extraction")

    # Build raw data for storage
    raw_data = {
        **airtable_candidates,
        "source": airtable_candidates.get("source", "airtable"),
        # Keep original Airtable cv_text
        "cv_text": cv_text_airtable,
        # Add extracted PDF text separately
        "cv_text_pdf": cv_text_pdf,
        # Add extraction metadata
        "cv_extraction_method": cv_extraction_method,
        "cv_extraction_cost_usd": cv_extraction_cost_usd,
        "cv_extraction_model": cv_extraction_model,
        "cv_extraction_pages": cv_extraction_pages,
    }

    has_airtable = bool(cv_text_airtable and len(cv_text_airtable.strip()) > 50)
    has_pdf = bool(cv_text_pdf and len(cv_text_pdf.strip()) > 50)
    sources = []
    if has_airtable:
        sources.append("airtable")
    if has_pdf:
        sources.append("pdf")

    context.log.info(
        f"Raw candidate ready: {raw_data.get('full_name', 'Unknown')} "
        f"(sources: {', '.join(sources) if sources else 'none'})"
    )

    return raw_data


@asset(
    partitions_def=candidate_partitions,
    ins={"raw_candidates": AssetIn()},
    description="LLM-normalized candidate profiles (merges Airtable + PDF sources)",
    group_name="candidates",
    required_resource_keys={"openrouter"},
    io_manager_key="postgres_io",
    code_version="2.1.1",  # bump after file changes; no logic change
    op_tags={
        "dagster/concurrency_key": "openrouter_api",
    },
    metadata={
        "table": "normalized_candidates",
        "llm_operation": "normalize_cv",
    },
)
def normalized_candidates(
    context: AssetExecutionContext,
    raw_candidates: dict[str, Any],
) -> dict[str, Any]:
    """Normalize raw candidate data into structured profiles using LLM.

    Supports dual-source CV extraction:
    - cv_text: Original text from Airtable (may include manually curated info)
    - cv_text_pdf: Text extracted from PDF attachment

    When both sources are available, the LLM merges them to create
    a comprehensive profile that captures ALL available information.
    """
    record_id = context.partition_key
    openrouter = context.resources.openrouter

    # Set context for cost tracking
    openrouter.set_context(
        run_id=context.run_id,
        asset_key="normalized_candidates",
        partition_key=record_id,
    )

    # Build Airtable text from ALL available raw fields
    airtable_parts = []
    if raw_candidates.get("full_name"):
        airtable_parts.append(f"Name: {raw_candidates['full_name']}")
    if raw_candidates.get("professional_summary"):
        airtable_parts.append(f"Summary: {raw_candidates['professional_summary']}")
    if raw_candidates.get("skills_raw"):
        airtable_parts.append(f"Skills: {raw_candidates['skills_raw']}")
    if raw_candidates.get("work_experience_raw"):
        airtable_parts.append(f"Experience: {raw_candidates['work_experience_raw']}")
    if raw_candidates.get("cv_text"):
        airtable_parts.append(f"CV Content:\n{raw_candidates['cv_text']}")
    if raw_candidates.get("location_raw"):
        airtable_parts.append(f"Location: {raw_candidates['location_raw']}")
    if raw_candidates.get("proof_of_work"):
        airtable_parts.append(f"Proof of Work: {raw_candidates['proof_of_work']}")
    if raw_candidates.get("desired_job_categories_raw"):
        airtable_parts.append(f"Desired Roles: {raw_candidates['desired_job_categories_raw']}")
    if raw_candidates.get("salary_range_raw"):
        airtable_parts.append(
            "Salary Expectations (interpret 'k' as thousands, e.g. 60-70k = 60,000-70,000 yearly): "
            f"{raw_candidates['salary_range_raw']}"
        )
    # Social profiles (helps LLM extract handles)
    if raw_candidates.get("github_url"):
        airtable_parts.append(f"GitHub: {raw_candidates['github_url']}")
    if raw_candidates.get("linkedin_url"):
        airtable_parts.append(f"LinkedIn: {raw_candidates['linkedin_url']}")
    if raw_candidates.get("x_profile_url"):
        airtable_parts.append(f"Twitter/X: {raw_candidates['x_profile_url']}")
    if raw_candidates.get("earn_profile_url"):
        airtable_parts.append(f"Earn Profile: {raw_candidates['earn_profile_url']}")

    cv_text_airtable = "\n\n".join(airtable_parts) if airtable_parts else None

    # Get PDF-extracted text (separate source)
    cv_text_pdf = raw_candidates.get("cv_text_pdf")

    # Check if we have any content
    has_airtable = cv_text_airtable and len(cv_text_airtable.strip()) > 50
    has_pdf = cv_text_pdf and len(cv_text_pdf.strip()) > 50

    if not has_airtable and not has_pdf:
        context.log.warning(f"No CV data available for candidate: {record_id}")
        context.add_output_metadata(
            {
                "llm_cost_usd": 0.0,
                "llm_tokens_input": 0,
                "llm_tokens_output": 0,
                "llm_model": "skipped",
                "skip_reason": "no_cv_data",
                "cv_sources": "none",
            }
        )
        return {
            "candidate_id": record_id,
            "airtable_record_id": raw_candidates.get("airtable_record_id"),
            "normalized_json": {
                "name": raw_candidates.get("full_name", "Unknown"),
                "years_of_experience": None,
                "skills": {"languages": [], "frameworks": [], "tools": [], "domains": []},
                "current_role": None,
                "location": None,
                "professional_summary": None,
            },
            "model_version": None,
        }

    # Log which sources we're using
    sources = []
    if has_airtable:
        sources.append("airtable")
    if has_pdf:
        sources.append("pdf")
    context.log.info(
        f"Normalizing candidate {record_id} via OpenRouter API (sources: {', '.join(sources)})"
    )

    # Fail fast if API key is not configured
    if not os.getenv("OPENROUTER_API_KEY"):
        raise ValueError(
            "OPENROUTER_API_KEY environment variable is not set. "
            "Set it in .env or export it before running the pipeline."
        )

    # Run with interrupt check so cancellation in the UI exits the step promptly
    # (avoids runs stuck in CANCELING when blocked on OpenRouter).
    result = asyncio.run(
        run_with_interrupt_check(
            context,
            normalize_cv(
                openrouter,
                raw_cv_text=cv_text_airtable,
                cv_text_pdf=cv_text_pdf,
            ),
        )
    )

    # Add LLM cost metadata to the materialization (visible in Dagster UI)
    context.add_output_metadata(
        {
            "llm_cost_usd": result.cost_usd,
            "llm_tokens_input": result.input_tokens,
            "llm_tokens_output": result.output_tokens,
            "llm_tokens_total": result.total_tokens,
            "llm_model": result.model,
            "cv_sources": ", ".join(sources),
        }
    )

    context.log.info(
        f"Normalized candidate: {result.data.get('name', 'Unknown')} "
        f"(sources: {', '.join(sources)}, cost: ${result.cost_usd:.6f}, "
        f"tokens: {result.total_tokens})"
    )

    return {
        "candidate_id": record_id,
        "airtable_record_id": raw_candidates.get("airtable_record_id"),
        "normalized_json": result.data,
        "model_version": result.model,
        "prompt_version": result.prompt_version,
    }


def _get_proficiency_label(score: int) -> str:
    """Convert numeric proficiency (1-10) to qualitative label."""
    return PROFICIENCY_LEVELS.get(score, "Unknown")


@asset(
    partitions_def=candidate_partitions,
    ins={"normalized_candidates": AssetIn()},
    description="Semantic embeddings for candidate profiles (narratives + skills + positions + projects)",
    group_name="candidates",
    required_resource_keys={"openrouter"},
    io_manager_key="pgvector_io",
    code_version="4.2.0",  # v4.2.0: Canonicalize skill vector keys via alias resolver
    op_tags={
        # Limit concurrent OpenRouter API calls to avoid rate limits
        # Shares concurrency pool with normalized_candidates
        "dagster/concurrency_key": "openrouter_api",
    },
    metadata={
        "table": "candidate_vectors",
        "vector_types": [
            "experience",
            "domain",
            "personality",
            "impact",
            "technical",
            "skill_*",
            "position_*",
            "project_*",
        ],
    },
)
def candidate_vectors(
    context: AssetExecutionContext,
    normalized_candidates: dict[str, Any],
) -> list[dict[str, Any]]:
    """Generate semantic embeddings for a candidate profile using OpenRouter.

    Generates multiple vector types:

    **Narrative vectors** (pure LLM-generated prose):
    - experience: Career journey, roles, progression
    - domain: Industries, markets, ecosystems, protocols
    - personality: Work style, values, culture fit signals
    - impact: Scope, ownership, scale, measurable outcomes
    - technical: Systems thinking, architecture, deep expertise

    **Skill vectors** (one per skill):
    - skill_{name}: "{Skill}: {Proficiency Level} - {Evidence}"
    - Uses structured format for precise skill matching

    **Position vectors** (one per job):
    - position_{index}: Job description/responsibilities

    **Project vectors** (one per project):
    - project_{index}: Description + technologies

    Uses OpenRouter's embeddings API with text-embedding-3-small (1536 dims).
    """
    record_id = context.partition_key
    openrouter = context.resources.openrouter

    # Fail fast if API key is not configured
    if not os.getenv("OPENROUTER_API_KEY"):
        raise ValueError(
            "OPENROUTER_API_KEY environment variable is not set. "
            "Set it in .env or export it before running the pipeline."
        )

    # Set context for cost tracking
    openrouter.set_context(
        run_id=context.run_id,
        asset_key="candidate_vectors",
        partition_key=record_id,
    )

    normalized_json = normalized_candidates.get("normalized_json", {})
    narratives = normalized_json.get("narratives", {})

    # ═══════════════════════════════════════════════════════════════════
    # NARRATIVE VECTORS (pure prose)
    # ═══════════════════════════════════════════════════════════════════
    texts_to_embed = {}

    texts_to_embed["experience"] = narratives.get("experience") or "No experience narrative"
    texts_to_embed["domain"] = narratives.get("domain") or "No domain narrative"
    texts_to_embed["personality"] = narratives.get("personality") or "No personality narrative"
    texts_to_embed["impact"] = narratives.get("impact") or "No impact narrative"
    texts_to_embed["technical"] = narratives.get("technical") or "No technical narrative"

    # ═══════════════════════════════════════════════════════════════════
    # SKILL VECTORS (structured: "Skill: Level - Evidence")
    # ═══════════════════════════════════════════════════════════════════
    session = get_session()
    alias_map = load_alias_map(session)
    session.close()

    skills = normalized_json.get("skills", [])
    for skill in skills:
        raw_name = skill.get("name", "Unknown")
        canonical_name = resolve_skill_name(raw_name, alias_map)
        proficiency = skill.get("proficiency", 5)
        evidence = skill.get("evidence", "")

        level_label = _get_proficiency_label(proficiency)
        skill_text = f"{canonical_name}: {level_label} - {evidence}"
        texts_to_embed[skill_vector_key(canonical_name)] = skill_text

    # ═══════════════════════════════════════════════════════════════════
    # POSITION VECTORS (job descriptions)
    # ═══════════════════════════════════════════════════════════════════
    experiences = normalized_json.get("experience", [])
    for i, exp in enumerate(experiences):
        description = exp.get("description", "")
        if description:
            # Include role context for better semantic matching
            role = exp.get("role", "")
            company = exp.get("company", "")
            position_text = f"{role} at {company}: {description}" if role else description
            texts_to_embed[f"position_{i}"] = position_text

    # ═══════════════════════════════════════════════════════════════════
    # PROJECT VECTORS (description + technologies)
    # ═══════════════════════════════════════════════════════════════════
    projects = normalized_json.get("projects", [])
    for i, proj in enumerate(projects):
        description = proj.get("description", "")
        technologies = proj.get("technologies", [])
        name = proj.get("name", "")

        if description or technologies:
            # Concat description + technologies
            tech_str = ", ".join(technologies) if technologies else ""
            project_text = f"{name}: {description}"
            if tech_str:
                project_text += f" Technologies: {tech_str}"
            texts_to_embed[f"project_{i}"] = project_text

    context.log.info(
        f"Generating {len(texts_to_embed)} embeddings for candidate: {record_id} "
        f"(5 narratives, {len(skills)} skills, {len(experiences)} positions, {len(projects)} projects)"
    )

    # Batch embed all texts in a single API call
    text_list = list(texts_to_embed.values())
    vector_types = list(texts_to_embed.keys())

    result = asyncio.run(embed_text(openrouter, text_list))

    # Add metadata for cost tracking
    context.add_output_metadata(
        {
            "embedding_cost_usd": result.cost_usd,
            "embedding_tokens": result.input_tokens,
            "embedding_dimensions": result.dimensions,
            "embedding_model": result.model,
            "vectors_generated": len(result.embeddings),
            "narrative_vectors": 5,
            "skill_vectors": len(skills),
            "position_vectors": len([e for e in experiences if e.get("description")]),
            "project_vectors": len(
                [p for p in projects if p.get("description") or p.get("technologies")]
            ),
        }
    )

    context.log.info(
        f"Generated {len(result.embeddings)} vectors for candidate: {record_id} "
        f"(cost: ${result.cost_usd:.6f}, dims: {result.dimensions}, model: {result.model})"
    )

    # Build output records
    vectors = []
    for i, vector_type in enumerate(vector_types):
        vectors.append(
            {
                "candidate_id": record_id,
                "airtable_record_id": normalized_candidates.get("airtable_record_id"),
                "vector_type": vector_type,
                "vector": result.embeddings[i],
                "model_version": result.model,
            }
        )

    return vectors


# Algorithm version for candidate_role_fitness (bump when prompt or logic changes)
ROLE_FITNESS_ALGORITHM_VERSION = "notion_v1"


@asset(
    partitions_def=candidate_partitions,
    ins={"normalized_candidates": AssetIn()},
    description="Full commit history from blobless git clone for skill verification",
    group_name="candidates",
    io_manager_key="postgres_io",
    required_resource_keys={"github"},
    code_version="1.0.0",
    op_tags={"dagster/concurrency_key": "github_api"},
    metadata={"table": "candidate_github_commit_history"},
)
def candidate_github_commit_history(
    context: AssetExecutionContext,
    normalized_candidates: dict[str, Any],
) -> dict[str, Any] | None:
    """Clone repos with blobless clone and extract full commit history.

    Uses git clone --filter=blob:none --bare; no REST rate limits.
    Runs in parallel to candidate_vectors, candidate_role_fitness.
    """
    record_id = context.partition_key
    github_handle = normalized_candidates.get("github_handle")
    candidate_id = normalized_candidates.get("id")  # from loaded NormalizedCandidate

    if not github_handle or not str(github_handle).strip():
        context.log.info(f"No github_handle for {record_id}; skipping commit history")
        return None

    if not candidate_id:
        context.log.warning(f"No normalized candidate id for partition {record_id}")
        return None

    github = context.resources.github
    context.log.info(f"Extracting commit history for {github_handle} ({record_id})")

    result = clone_and_extract_commits(username=github_handle, github=github, max_repos=15)

    total_commits = sum(len(r.get("commits", [])) for r in result.get("repos", []))
    context.add_output_metadata(
        {
            "repos_cloned": len(result.get("repos", [])),
            "total_commits": total_commits,
        }
    )

    return {
        "candidate_id": str(candidate_id),
        "airtable_record_id": record_id,
        "github_username": github_handle,
        "commit_history": result,
    }


@asset(
    partitions_def=candidate_partitions,
    ins={
        "normalized_candidates": AssetIn(),
        "candidate_github_commit_history": AssetIn(),
    },
    description="Verify each skill against GitHub profile via single LLM call",
    group_name="candidates",
    io_manager_key="postgres_io",
    required_resource_keys={"github", "openrouter"},
    code_version="1.0.0",
    op_tags={"dagster/concurrency_key": "openrouter_api"},
    metadata={"table": "candidate_skill_verification"},
)
def candidate_skill_verification(
    context: AssetExecutionContext,
    normalized_candidates: dict[str, Any],
    candidate_github_commit_history: dict[str, Any] | None,
) -> dict[str, Any]:
    """Verify claimed skills against GitHub data (commit history + API metadata + LLM)."""
    from datetime import datetime

    from talent_matching.models.enums import SkillVerificationStatusEnum
    from talent_matching.utils.dagster_async import run_with_interrupt_check

    record_id = context.partition_key
    candidate_id = normalized_candidates.get("id")
    github_handle = normalized_candidates.get("github_handle")

    if not candidate_id:
        context.log.warning(f"No normalized candidate id for partition {record_id}")
        return {
            "candidate_id": None,
            "airtable_record_id": record_id,
            "skipped": True,
            "skill_verification_score": None,
            "per_skill_results": [],
        }

    if not github_handle or not str(github_handle).strip():
        context.log.info(f"No github_handle for {record_id}; skipping skill verification")
        return {
            "candidate_id": str(candidate_id),
            "airtable_record_id": record_id,
            "skipped": True,
            "skill_verification_score": None,
            "per_skill_results": [],
        }

    if not candidate_github_commit_history:
        context.log.info(f"No commit history for {record_id}; skipping skill verification")
        return {
            "candidate_id": str(candidate_id),
            "airtable_record_id": record_id,
            "skipped": True,
            "skill_verification_score": None,
            "per_skill_results": [],
        }

    skills = (normalized_candidates.get("normalized_json") or {}).get("skills", [])
    if not skills:
        context.log.info(f"No skills for {record_id}; skipping skill verification")
        return {
            "candidate_id": str(candidate_id),
            "airtable_record_id": record_id,
            "skipped": True,
            "skill_verification_score": None,
            "per_skill_results": [],
        }

    skills_with_evidence = [
        {
            "name": s.get("name", "?") if isinstance(s, dict) else str(s),
            "evidence": s.get("evidence", "") if isinstance(s, dict) else "",
        }
        for s in skills
    ]
    skills_with_evidence = [s for s in skills_with_evidence if s["name"] != "?"]

    if not skills_with_evidence:
        return {
            "candidate_id": str(candidate_id),
            "airtable_record_id": record_id,
            "skipped": True,
            "skill_verification_score": None,
            "per_skill_results": [],
        }

    github = context.resources.github
    openrouter = context.resources.openrouter

    commit_history = candidate_github_commit_history.get("commit_history") or {}
    repos = commit_history.get("repos", [])[:10]

    repo_metadata = []
    for r in repos:
        full_name = r.get("full_name", "")
        if "/" not in full_name:
            continue
        owner, repo_name = full_name.split("/", 1)
        meta = fetch_repo_metadata(owner, repo_name, github)
        repo_metadata.append(meta)

    github_data = {
        "username": github_handle,
        "repo_metadata": repo_metadata,
        "commit_history": commit_history,
    }

    openrouter.set_context(
        run_id=context.run_id,
        asset_key="candidate_skill_verification",
        partition_key=record_id,
    )

    llm_results = asyncio.run(
        run_with_interrupt_check(
            context,
            verify_skills_llm(
                skills_with_evidence=skills_with_evidence,
                github_data=github_data,
                openrouter=openrouter,
            ),
        )
    )

    now = datetime.now().isoformat()
    per_skill_results = []
    verified_count = 0
    total_verifiable = 0

    for r in llm_results:
        verified = r.get("verified", False)
        confidence = float(r.get("confidence", 0))
        evidence = r.get("evidence", "") or ""
        skill_name = r.get("skill", "?")

        if skill_name == "?":
            continue

        total_verifiable += 1
        if verified:
            verified_count += 1

        status = (
            SkillVerificationStatusEnum.VERIFIED
            if verified
            else SkillVerificationStatusEnum.UNVERIFIED
        )
        per_skill_results.append(
            {
                "skill_name": skill_name,
                "verified": verified,
                "confidence": confidence,
                "evidence": evidence,
                "verification_status": status.value,
                "verification_evidence": {
                    "source": "github",
                    "confidence": confidence,
                    "snippet": evidence[:200],
                },
                "verified_at": now,
            }
        )

    score = verified_count / total_verifiable if total_verifiable > 0 else None

    context.add_output_metadata(
        {
            "skills_verified": len(per_skill_results),
            "verified_count": verified_count,
            "skill_verification_score": score,
        }
    )

    return {
        "candidate_id": str(candidate_id),
        "airtable_record_id": record_id,
        "skipped": False,
        "skill_verification_score": score,
        "per_skill_results": per_skill_results,
    }


@asset(
    partitions_def=candidate_partitions,
    ins={"normalized_candidates": AssetIn()},
    description="LLM fitness score per candidate per desired job category (1-100 → stored 0-1)",
    group_name="candidates",
    io_manager_key="postgres_io",
    required_resource_keys={"openrouter"},
    code_version="1.0.0",
    op_tags={"dagster/concurrency_key": "openrouter_api"},
    metadata={"table": "candidate_role_fitness"},
)
def candidate_role_fitness(
    context: AssetExecutionContext,
    normalized_candidates: dict[str, Any],
) -> list[dict[str, Any]]:
    """Score each desired_job_category for this candidate; store in candidate_role_fitness."""
    from talent_matching.llm.operations.score_role_fitness import score_role_fitness

    record_id = context.partition_key
    candidate_id = normalized_candidates.get("id")
    if not candidate_id:
        context.log.warning(f"No normalized candidate id for partition {record_id}")
        return []
    roles = normalized_candidates.get("desired_job_categories") or []
    if not roles:
        context.log.info(f"No desired_job_categories for {record_id}; skipping role fitness")
        return []

    # Build a compact profile for the LLM (skills, experience, seniority)
    normalized_json = normalized_candidates.get("normalized_json") or {}
    profile = {
        "skills_summary": normalized_candidates.get("skills_summary"),
        "years_of_experience": normalized_candidates.get("years_of_experience"),
        "seniority_level": normalized_candidates.get("seniority_level"),
        "professional_summary": normalized_candidates.get("professional_summary"),
        "current_role": normalized_candidates.get("current_role"),
        "notable_achievements": normalized_candidates.get("notable_achievements"),
        "skills": normalized_json.get("skills", [])[:20],
        "experience": (normalized_json.get("experience") or [])[:5],
    }

    openrouter = context.resources.openrouter
    results = []
    for role_name in roles:
        if not role_name or not str(role_name).strip():
            continue
        role_name = str(role_name).strip()
        out = asyncio.run(score_role_fitness(openrouter, profile, role_name))
        score_1_100 = out["fitness_score"]
        reasoning = out.get("reasoning", "")
        results.append(
            {
                "candidate_id": str(candidate_id),
                "role_name": role_name,
                "fitness_score": round(score_1_100 / 100.0, 6),
                "score_breakdown": json.dumps({"reasoning": reasoning}),
                "algorithm_version": ROLE_FITNESS_ALGORITHM_VERSION,
            }
        )
    context.log.info(f"Computed {len(results)} role fitness scores for {record_id}")
    return results


@asset(
    partitions_def=candidate_partitions,
    ins={
        "normalized_candidates": AssetIn(),
    },
    description="Write normalized candidate fields back to Airtable (N)-prefixed columns",
    group_name="candidates",
    required_resource_keys={"airtable", "matchmaking"},
    op_tags={"dagster/concurrency_key": "airtable_api"},
)
def airtable_candidate_sync(
    context: AssetExecutionContext,
    normalized_candidates: dict[str, Any],
) -> dict[str, Any]:
    """Write all normalized candidate fields back to the same Airtable row under (N)-prefixed columns.

    Loads full NormalizedCandidate from Postgres by airtable_record_id and PATCHes the record.
    """
    record_id = context.partition_key
    matchmaking = context.resources.matchmaking
    candidate = matchmaking.get_normalized_candidate_by_airtable_record_id(record_id)
    if not candidate:
        context.log.warning(
            f"No normalized_candidates row for airtable_record_id={record_id}; skip sync"
        )
        return {"airtable_record_id": record_id, "synced": False, "skipped": True, "fields": {}}
    fields = normalized_candidate_to_airtable_fields(candidate)
    if not fields:
        context.log.info(f"No fields to sync for {record_id}")
        return {"airtable_record_id": record_id, "synced": False, "fields": {}}
    airtable = context.resources.airtable
    airtable.update_record(record_id, fields)
    context.log.info(f"Synced {record_id}: {len(fields)} (N) columns")
    return {"airtable_record_id": record_id, "synced": True, "fields": fields}
