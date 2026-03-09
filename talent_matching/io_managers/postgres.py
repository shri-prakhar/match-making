"""PostgreSQL IO Manager using SQLAlchemy ORM.

This IO manager handles the storage and retrieval of candidate and job data
using SQLAlchemy models for type safety and consistency.
"""

import json
from datetime import date, datetime
from typing import Any
from uuid import UUID, uuid4

from dagster import ConfigurableIOManager, InputContext, OutputContext
from sqlalchemy import delete, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from talent_matching.db import get_engine, get_session
from talent_matching.models import (
    Match,
    NormalizedCandidate,
    NormalizedJob,
    ProcessingStatusEnum,
    RawCandidate,
    RawJob,
)
from talent_matching.models.candidates import (
    CandidateAttribute,
    CandidateExperience,
    CandidateGithubCommitHistory,
    CandidateProject,
    CandidateRoleFitness,
    CandidateSkill,
)
from talent_matching.models.enums import (
    EmploymentTypeEnum,
    LocationTypeEnum,
    RequirementTypeEnum,
    SeniorityEnum,
)
from talent_matching.models.jobs import JobRequiredSkill
from talent_matching.models.skills import Skill
from talent_matching.services.timezone_resolver import resolve_candidate_timezone
from talent_matching.skills.resolver import get_or_create_skill
from talent_matching.utils.airtable_mapper import parse_comma_separated


def _parse_employment_type(value: Any) -> EmploymentTypeEnum | None:
    if value is None:
        return None
    s = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    for e in EmploymentTypeEnum:
        if e.value == s or s in e.value:
            return e
    return None


_SENIORITY_LOOKUP: dict[str, SeniorityEnum] = {e.value: e for e in SeniorityEnum}


def _parse_seniority(value: Any) -> SeniorityEnum | None:
    """Safely map an LLM-provided seniority string to the enum, or None."""
    if value is None:
        return None
    return _SENIORITY_LOOKUP.get(str(value).strip().lower())


def _parse_employment_types(value: Any) -> list[EmploymentTypeEnum] | None:
    """Parse employment_type from LLM (single string or list of strings) to list of enums."""
    if value is None:
        return None
    if isinstance(value, list):
        items = value
    else:
        items = [value]
    result = []
    for item in items:
        parsed = _parse_employment_type(item)
        if parsed is not None and parsed not in result:
            result.append(parsed)
    return result if result else None


def _parse_location_type(value: Any) -> LocationTypeEnum | None:
    if not value:
        return None
    s = str(value).strip().lower()
    for e in LocationTypeEnum:
        if e.value == s or s in e.value:
            return e
    return None


def _serialize_for_text(value: Any) -> str | None:
    """Serialize complex types (list, dict) to JSON string for Text columns.

    Args:
        value: The value to serialize

    Returns:
        JSON string if value is list/dict, string if already string, None if None
    """
    if value is None:
        return None
    if isinstance(value, list | dict):
        return json.dumps(value)
    return str(value)


class PostgresMetricsIOManager(ConfigurableIOManager):
    """IO Manager for storing structured data in PostgreSQL using SQLAlchemy ORM.

    Handles the following asset types:
    - raw_candidates: Raw ingested candidate data
    - normalized_candidates: LLM-normalized candidate profiles
    - raw_jobs: Raw job descriptions
    - normalized_jobs: LLM-normalized job requirements
    - matches: Computed match results

    Uses SQLAlchemy ORM for type safety and automatic schema validation.
    """

    @staticmethod
    def _get_engine():
        return get_engine()

    @staticmethod
    def _get_session() -> Session:
        return get_session()

    def _extract_handle_from_url(self, url: str, domain: str) -> str | None:
        """Extract username/handle from a social media URL.

        Args:
            url: Full URL like https://github.com/username/
            domain: Domain to match like 'github.com' or 'linkedin.com/in'

        Returns:
            Username/handle extracted from URL, or None if not found
        """
        if not url or domain not in url:
            return None
        # Find the part after the domain
        parts = url.split(domain)
        if len(parts) < 2:
            return None
        # Get the path after the domain, strip slashes
        path = parts[1].strip("/")
        # Get the first segment (username)
        if "/" in path:
            return path.split("/")[0]
        return path if path else None

    def _parse_salary_range(self, salary_raw: str | None) -> tuple[int | None, int | None, str]:
        """Parse salary range string into min, max, and currency (always yearly amounts).

        Handles formats like:
        - "$15,000 - $60,000"
        - "15000-60000"
        - "$50k - $80k"
        - "60-70k" or "60k-70k" (both numbers in thousands; output 60000, 70000)
        - "€40,000 - €60,000"

        Important: When the string contains "k" or "K", any bare number in the range
        is treated as thousands (e.g. "60-70k" means 60,000–70,000 per year, not 60–70).
        """
        import re

        if not salary_raw:
            return None, None, "USD"

        # Detect currency
        currency = "USD"
        if "€" in salary_raw:
            currency = "EUR"
        elif "£" in salary_raw:
            currency = "GBP"

        # Remove currency symbols and commas, normalize
        cleaned = re.sub(r"[$€£,]", "", salary_raw)
        has_k = "k" in salary_raw.lower()

        # Handle "k" notation (e.g., "50k" -> 50000)
        cleaned = re.sub(
            r"(\d+)k", lambda m: str(int(m.group(1)) * 1000), cleaned, flags=re.IGNORECASE
        )

        # Find all numbers
        numbers = [int(n) for n in re.findall(r"\d+", cleaned)]

        # If string contained "k", any number still below 1000 was a bare "60" in "60-70k"
        if has_k and numbers:
            numbers = [n * 1000 if n < 1000 else n for n in numbers]

        if len(numbers) >= 2:
            return numbers[0], numbers[1], currency
        if len(numbers) == 1:
            return numbers[0], numbers[0], currency

        return None, None, currency

    def _parse_date_string(self, date_str: str | None) -> date | None:
        """Parse YYYY-MM or YYYY-MM-DD date string to Python date.

        Args:
            date_str: Date string in YYYY-MM or YYYY-MM-DD format

        Returns:
            Python date object (defaults to 1st of month for YYYY-MM) or None
        """
        if not date_str:
            return None

        # Handle YYYY-MM format (default to 1st of month)
        if len(date_str) == 7 and date_str[4] == "-":
            return date(int(date_str[:4]), int(date_str[5:7]), 1)

        # Handle YYYY-MM-DD format
        if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
            return date(int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10]))

        return None

    def _get_table_name(self, context: OutputContext | InputContext) -> str:
        """Derive table name from asset key. For load_input, use upstream asset key when loading."""
        asset_key = context.asset_key
        if hasattr(context, "upstream_output") and context.upstream_output is not None:
            asset_key = context.upstream_output.asset_key
        if asset_key:
            table_from_path = "_".join(asset_key.path)
            if "llm_refined_shortlist" in table_from_path:
                return "matches"
            return table_from_path
        return "unknown"

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store output data in PostgreSQL using SQLAlchemy ORM.

        Args:
            context: Dagster output context with asset metadata
            obj: Data to store (dict or list of dicts)
        """
        table_name = self._get_table_name(context)

        if table_name == "raw_candidates":
            self._store_raw_candidate(context, obj)
        elif table_name == "normalized_candidates":
            self._store_normalized_candidate(context, obj)
        elif table_name == "raw_jobs":
            self._store_raw_job(context, obj)
        elif table_name == "normalized_jobs":
            self._store_normalized_job(context, obj)
        elif table_name == "matches":
            self._store_matches(context, obj if isinstance(obj, list) else [obj])
        elif table_name == "candidate_role_fitness":
            self._store_candidate_role_fitness(context, obj if isinstance(obj, list) else [obj])
        elif table_name == "candidate_github_commit_history":
            if obj is not None:
                self._store_candidate_github_commit_history(context, obj)
        elif table_name == "candidate_skill_verification":
            self._store_candidate_skill_verification(context, obj)
        else:
            context.log.warning(f"Unknown asset type: {table_name}")

        context.log.info(f"Stored data to table: {table_name}")

    def load_input(self, context: InputContext) -> Any:
        """Load data from PostgreSQL using SQLAlchemy ORM.

        Args:
            context: Dagster input context with asset metadata

        Returns:
            Retrieved data from the database (single dict for partitioned, list for non-partitioned)
        """
        table_name = self._get_table_name(context)
        session = self._get_session()

        model_map = {
            "raw_candidates": RawCandidate,
            "normalized_candidates": NormalizedCandidate,
            "raw_jobs": RawJob,
            "normalized_jobs": NormalizedJob,
            "matches": Match,
            "candidate_github_commit_history": CandidateGithubCommitHistory,
        }

        model = model_map.get(table_name)
        if not model:
            context.log.warning(f"Unknown table for load: {table_name}")
            session.close()
            return None

        # For partitioned assets, load by partition key.
        # Do not use hasattr()—it invokes the property and can raise on non-partitioned runs.
        partition_key = None
        try:
            partition_key = context.partition_key
        except Exception:
            partition_key = None

        if partition_key:
            # Matches are keyed by job_id (FK to normalized_jobs), not by
            # their own airtable_record_id. Resolve the partition key
            # (an Airtable record ID) through normalized_jobs first.
            if table_name == "matches":
                nj_id = session.execute(
                    select(NormalizedJob.id).where(
                        NormalizedJob.airtable_record_id == partition_key
                    )
                ).scalar_one_or_none()
                if nj_id:
                    results = (
                        session.execute(
                            select(model).where(Match.job_id == nj_id).order_by(Match.rank)
                        )
                        .scalars()
                        .all()
                    )
                    session.close()
                    return [self._model_to_dict(r) for r in results]
                session.close()
                return []

            # Partitioned asset - return single record by airtable_record_id
            if hasattr(model, "airtable_record_id"):
                stmt = select(model).where(model.airtable_record_id == partition_key)
            else:
                context.log.warning(f"Model {model.__name__} has no airtable_record_id column")
                session.close()
                return None

            result = session.execute(stmt).scalar_one_or_none()
            if result:
                session.close()
                out = self._model_to_dict(result)
                return out
            # No row for this partition key (e.g. job partition key passed to normalized_candidates).
            # Fall back to load-all for tables used as "all partitions" by downstream (e.g. matches).
            if table_name == "normalized_candidates":
                session.close()
                session = self._get_session()
                stmt = select(model).limit(20_000)
                results = session.execute(stmt).scalars().all()
                session.close()
                return [self._model_to_dict(r) for r in results]
            session.close()
            return []
        else:
            # Non-partitioned - load all records (limit allows 20k e.g. normalized_candidates)
            stmt = select(model).limit(20_000)
            results = session.execute(stmt).scalars().all()
            session.close()
            return [self._model_to_dict(r) for r in results]

    def _model_to_dict(self, model_instance: Any) -> dict[str, Any]:
        """Convert SQLAlchemy model instance to dictionary."""
        result = {}
        for column in model_instance.__table__.columns:
            value = getattr(model_instance, column.name)
            # Convert UUID to string for JSON compatibility
            if isinstance(value, UUID):
                value = str(value)
            result[column.name] = value
        return result

    def _store_raw_candidate(self, context: OutputContext, data: dict[str, Any]) -> None:
        """Store raw candidate data using SQLAlchemy ORM with upsert."""
        session = self._get_session()

        # Get partition key (Airtable record ID)
        partition_key = context.partition_key

        # Map the incoming data to model fields
        # Serialize complex types (lists, dicts) to JSON strings for Text columns
        values = {
            "airtable_record_id": partition_key,
            "source": data.get("source", "airtable"),
            "source_id": data.get("source_id") or partition_key,
            "full_name": data.get("full_name", "Unknown"),
            "location_raw": _serialize_for_text(data.get("location_raw")),
            "desired_job_categories_raw": _serialize_for_text(
                data.get("desired_job_categories_raw")
            ),
            "skills_raw": _serialize_for_text(data.get("skills_raw")),
            "cv_url": _serialize_for_text(data.get("cv_url")),
            "cv_text": _serialize_for_text(data.get("cv_text")),
            "professional_summary": _serialize_for_text(data.get("professional_summary")),
            "proof_of_work": _serialize_for_text(data.get("proof_of_work")),
            "salary_range_raw": _serialize_for_text(data.get("salary_range_raw")),
            "x_profile_url": _serialize_for_text(data.get("x_profile_url")),
            "linkedin_url": _serialize_for_text(data.get("linkedin_url")),
            "earn_profile_url": _serialize_for_text(data.get("earn_profile_url")),
            "github_url": _serialize_for_text(data.get("github_url")),
            "work_experience_raw": _serialize_for_text(data.get("work_experience_raw")),
            "job_status_raw": _serialize_for_text(data.get("job_status_raw")),
            "processing_status": ProcessingStatusEnum.PENDING,
        }

        # Use PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE)
        stmt = insert(RawCandidate).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["airtable_record_id"],
            set_={
                "source": stmt.excluded.source,
                "full_name": stmt.excluded.full_name,
                "location_raw": stmt.excluded.location_raw,
                "desired_job_categories_raw": stmt.excluded.desired_job_categories_raw,
                "skills_raw": stmt.excluded.skills_raw,
                "cv_url": stmt.excluded.cv_url,
                "cv_text": stmt.excluded.cv_text,
                "professional_summary": stmt.excluded.professional_summary,
                "proof_of_work": stmt.excluded.proof_of_work,
                "salary_range_raw": stmt.excluded.salary_range_raw,
                "x_profile_url": stmt.excluded.x_profile_url,
                "linkedin_url": stmt.excluded.linkedin_url,
                "earn_profile_url": stmt.excluded.earn_profile_url,
                "github_url": stmt.excluded.github_url,
                "work_experience_raw": stmt.excluded.work_experience_raw,
                "job_status_raw": stmt.excluded.job_status_raw,
            },
        )

        session.execute(stmt)
        session.commit()
        session.close()

        context.log.info(f"Upserted raw candidate: {partition_key}")

    def _store_normalized_candidate(self, context: OutputContext, data: dict[str, Any]) -> None:
        """Store normalized candidate data using SQLAlchemy ORM."""
        session = self._get_session()
        partition_key = context.partition_key

        # First, get the raw candidate ID
        raw_candidate = session.execute(
            select(RawCandidate).where(RawCandidate.airtable_record_id == partition_key)
        ).scalar_one_or_none()

        if not raw_candidate:
            context.log.error(f"Raw candidate not found for: {partition_key}")
            session.close()
            return

        # The asset returns {"normalized_json": {...}, "model_version": "..."}
        # Extract the LLM-normalized data from the nested structure
        normalized_json = data.get("normalized_json", {})

        # Skills is now a list of objects: [{"name": "Python", "years": 3}, ...]
        # Also support legacy flat list format: ["Python", ...]
        skills = normalized_json.get("skills", [])
        if isinstance(skills, list):
            skills_list = [s.get("name") if isinstance(s, dict) else s for s in skills if s]
        else:
            skills_list = []

        # Extract location fields from nested structure
        location = normalized_json.get("location", {})
        if isinstance(location, dict):
            location_city = location.get("city")
            location_country = location.get("country")
            location_region = location.get("region")
            timezone = location.get("timezone")
        else:
            location_city = None
            location_country = str(location) if location else None
            location_region = None
            timezone = None

        # Inline timezone resolution: cache lookup -> LLM fallback -> null
        timezone = resolve_candidate_timezone(session, location_city, location_country, timezone)

        # Extract social handles from LLM response
        social_handles = normalized_json.get("social_handles", {})
        github_handle = social_handles.get("github") if isinstance(social_handles, dict) else None
        linkedin_handle = (
            social_handles.get("linkedin") if isinstance(social_handles, dict) else None
        )
        x_handle = social_handles.get("twitter") if isinstance(social_handles, dict) else None

        # Fallback: Extract handles from raw candidate URLs if LLM didn't provide them
        if not github_handle and raw_candidate.github_url:
            github_handle = self._extract_handle_from_url(raw_candidate.github_url, "github.com")
        if not linkedin_handle and raw_candidate.linkedin_url:
            linkedin_handle = self._extract_handle_from_url(
                raw_candidate.linkedin_url, "linkedin.com/in"
            )
        if not x_handle and raw_candidate.x_profile_url:
            x_handle = self._extract_handle_from_url(
                raw_candidate.x_profile_url, "x.com"
            ) or self._extract_handle_from_url(raw_candidate.x_profile_url, "twitter.com")

        # Parse salary range from raw data
        compensation_min, compensation_max, compensation_currency = self._parse_salary_range(
            raw_candidate.salary_range_raw
        )

        # Extract experience data and compute metrics
        experience = normalized_json.get("experience", [])
        if isinstance(experience, list):
            companies = [exp.get("company") for exp in experience if exp.get("company")]
            job_count = len(experience)
            durations = [
                exp.get("duration_months") for exp in experience if exp.get("duration_months")
            ]
            average_tenure_months = int(sum(durations) / len(durations)) if durations else None
            longest_tenure_months = max(durations) if durations else None
        else:
            companies = []
            job_count = None
            average_tenure_months = None
            longest_tenure_months = None

        # Extract education (now a flat object)
        education = normalized_json.get("education", {})
        if isinstance(education, dict):
            education_highest_degree = education.get("highest_degree")
            education_field = education.get("field")
            education_institution = education.get("institution")
        else:
            education_highest_degree = None
            education_field = None
            education_institution = None

        # Extract hackathon stats
        hackathons = normalized_json.get("hackathons", [])
        hackathon_wins_count = len(hackathons) if isinstance(hackathons, list) else 0
        hackathon_total_prize_usd = (
            sum(h.get("prize_amount_usd", 0) or 0 for h in hackathons)
            if isinstance(hackathons, list)
            else 0
        )
        solana_hackathon_wins = (
            sum(1 for h in hackathons if h.get("is_solana")) if isinstance(hackathons, list) else 0
        )

        # Combine achievements and hackathon names for notable_achievements
        achievements = normalized_json.get("achievements", [])
        hackathon_names = (
            [h.get("name") for h in hackathons if h.get("name")]
            if isinstance(hackathons, list)
            else []
        )
        notable_achievements = (
            achievements if isinstance(achievements, list) else []
        ) + hackathon_names

        # Extract verified communities
        verified_communities = normalized_json.get("verified_communities", [])
        verified_communities = (
            verified_communities if isinstance(verified_communities, list) else []
        )

        # Map data to NormalizedCandidate model fields
        values = {
            "airtable_record_id": partition_key,
            "raw_candidate_id": raw_candidate.id,
            "full_name": normalized_json.get("name", "Unknown"),
            "email": normalized_json.get("email"),
            "phone": normalized_json.get("phone"),
            "location_city": location_city,
            "location_country": location_country,
            "location_region": location_region,
            "timezone": timezone,
            "professional_summary": normalized_json.get("summary"),
            "current_role": normalized_json.get("current_role"),
            "seniority_level": _parse_seniority(normalized_json.get("seniority_level")),
            "years_of_experience": normalized_json.get("years_of_experience"),
            "desired_job_categories": parse_comma_separated(
                raw_candidate.desired_job_categories_raw
            )
            or None,
            "skills_summary": skills_list if skills_list else None,
            "companies_summary": companies if companies else None,
            "notable_achievements": notable_achievements if notable_achievements else None,
            "verified_communities": verified_communities if verified_communities else None,
            "compensation_min": compensation_min,
            "compensation_max": compensation_max,
            "compensation_currency": compensation_currency,
            "job_count": job_count,
            "job_switches_count": job_count - 1 if job_count and job_count > 0 else None,
            "average_tenure_months": average_tenure_months,
            "longest_tenure_months": longest_tenure_months,
            "education_highest_degree": education_highest_degree,
            "education_field": education_field,
            "education_institution": education_institution,
            "hackathon_wins_count": hackathon_wins_count,
            "hackathon_total_prize_usd": hackathon_total_prize_usd,
            "solana_hackathon_wins": solana_hackathon_wins,
            "x_handle": x_handle,
            "linkedin_handle": linkedin_handle,
            "github_handle": github_handle,
            "prompt_version": data.get("prompt_version"),
            "model_version": data.get("model_version"),
            "confidence_score": normalized_json.get("confidence_score"),
            "job_status": raw_candidate.job_status_raw,
            # Store full LLM response for narratives/vectorization
            "normalized_json": normalized_json,
        }

        # Upsert normalized candidate
        stmt = insert(NormalizedCandidate).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["airtable_record_id"],
            set_={k: getattr(stmt.excluded, k) for k in values.keys() if k != "airtable_record_id"},
        )

        session.execute(stmt)
        session.commit()

        # Get the normalized candidate ID for related tables
        normalized_candidate = session.execute(
            select(NormalizedCandidate).where(
                NormalizedCandidate.airtable_record_id == partition_key
            )
        ).scalar_one()
        normalized_id = normalized_candidate.id

        # Store related data from LLM response
        self._store_candidate_experiences(
            session, normalized_id, partition_key, experience, context
        )
        self._store_candidate_skills(
            session, normalized_id, partition_key, skills, data.get("model_version"), context
        )
        # Store projects from LLM response
        projects = normalized_json.get("projects", [])
        self._store_candidate_projects(
            session, normalized_id, partition_key, projects, hackathons, context
        )

        # Store soft attributes from LLM response
        soft_attributes = normalized_json.get("soft_attributes", {})
        self._store_candidate_attributes(
            session,
            normalized_id,
            partition_key,
            soft_attributes,
            data.get("model_version"),
            context,
        )

        # Update raw candidate processing status
        raw_candidate.processing_status = ProcessingStatusEnum.COMPLETED
        session.commit()
        session.close()

        context.log.info(f"Upserted normalized candidate with related data: {partition_key}")

    def _store_candidate_experiences(
        self,
        session: Session,
        candidate_id: Any,
        partition_key: str,
        experiences: list[dict[str, Any]],
        context: OutputContext,
    ) -> None:
        """Store work experience entries from LLM response.

        LLM returns experience as:
        [{"company": "...", "role": "...", "duration_months": ..., "description": "...", "technologies": [...]}]
        """
        if not experiences or not isinstance(experiences, list):
            return

        # Delete existing experiences for this candidate (replace strategy)
        session.execute(
            delete(CandidateExperience).where(CandidateExperience.candidate_id == candidate_id)
        )

        for order, exp in enumerate(experiences, 1):
            if not exp.get("company") or not exp.get("role"):
                continue

            # Calculate years_experience from duration_months
            duration_months = exp.get("duration_months")
            years_exp = duration_months / 12.0 if duration_months else None

            # Parse dates from YYYY-MM format
            start_date = self._parse_date_string(exp.get("start_date"))
            end_date = self._parse_date_string(exp.get("end_date"))

            # Determine is_current: no end_date means still employed there
            is_current = end_date is None

            values = {
                "id": uuid4(),
                "airtable_record_id": f"{partition_key}_exp_{order}",
                "candidate_id": candidate_id,
                "company_name": exp.get("company"),
                "position_title": exp.get("role"),
                "start_date": start_date,
                "end_date": end_date,
                "years_experience": years_exp,
                "is_current": is_current,
                "description": exp.get("description"),
                "skills_used": exp.get("technologies"),
                "position_order": order,
            }

            stmt = insert(CandidateExperience).values(**values)
            session.execute(stmt)

        session.commit()
        context.log.info(f"Stored {len(experiences)} work experiences for {partition_key}")

    def _store_candidate_skills(
        self,
        session: Session,
        candidate_id: Any,
        partition_key: str,
        skills: Any,
        model_version: str | None,
        context: OutputContext,
    ) -> None:
        """Store skills from LLM response, creating skill entries as needed.

        LLM v2.2.0+ returns skills as: [{"name": "Python", "years": 3}, ...]
        Also supports legacy flat list format: ["Python", "React", ...]
        """
        if not skills or not isinstance(skills, list):
            return

        # Delete existing skill associations for this candidate (replace strategy)
        session.execute(delete(CandidateSkill).where(CandidateSkill.candidate_id == candidate_id))

        skills_stored = 0
        for skill_item in skills:
            # Handle both new format (dict with proficiency) and legacy format (string)
            if isinstance(skill_item, dict):
                skill_name = skill_item.get("name")
                years = skill_item.get("years")
                proficiency = skill_item.get("proficiency")  # 1-10 from LLM
                evidence = skill_item.get("evidence")  # Justification for rating
            elif isinstance(skill_item, str):
                skill_name = skill_item
                years = None
                proficiency = None
                evidence = None
            else:
                continue

            if not skill_name:
                continue

            skill_id = get_or_create_skill(session, skill_name)
            if not skill_id:
                continue

            # Use LLM proficiency rating if available, otherwise default to 5
            rating = int(proficiency) if proficiency else 5
            # Clamp to valid range 1-10
            rating = max(1, min(10, rating))

            values = {
                "id": uuid4(),
                "airtable_record_id": f"{partition_key}_skill_{hash(skill_name) % 10000:04d}",
                "candidate_id": candidate_id,
                "skill_id": skill_id,
                "rating": rating,
                "years_experience": int(years) if years else None,
                "notable_achievement": evidence,  # Store evidence as notable achievement
                "rating_model": model_version,
            }

            stmt = insert(CandidateSkill).values(**values)
            stmt = stmt.on_conflict_do_nothing()  # Skip duplicates
            session.execute(stmt)
            skills_stored += 1

        session.commit()
        context.log.info(f"Stored {skills_stored} skills for {partition_key}")

    def _store_candidate_projects(
        self,
        session: Session,
        candidate_id: Any,
        partition_key: str,
        projects: list[dict[str, Any]],
        hackathons: list[dict[str, Any]],
        context: OutputContext,
    ) -> None:
        """Store projects and hackathons from LLM response.

        LLM returns:
        - projects: [{"name": "...", "description": "...", "technologies": [...], "url": "..."}]
        - hackathons: [{"name": "...", "prize": "...", "prize_amount_usd": ..., "is_solana": bool}]
        """
        # Delete existing projects for this candidate (replace strategy)
        session.execute(
            delete(CandidateProject).where(CandidateProject.candidate_id == candidate_id)
        )

        projects_stored = 0
        order = 0

        # Store regular projects
        if isinstance(projects, list):
            for proj in projects:
                if not proj.get("name"):
                    continue

                order += 1
                values = {
                    "id": uuid4(),
                    "airtable_record_id": f"{partition_key}_proj_{order}",
                    "candidate_id": candidate_id,
                    "project_name": proj.get("name"),
                    "description": proj.get("description"),
                    "technologies": proj.get("technologies")
                    if isinstance(proj.get("technologies"), list)
                    else None,
                    "url": proj.get("url"),
                    "is_hackathon": False,
                    "project_order": order,
                }

                stmt = insert(CandidateProject).values(**values)
                session.execute(stmt)
                projects_stored += 1

        # Store hackathons as projects with is_hackathon=True
        if isinstance(hackathons, list):
            for hackathon in hackathons:
                if not hackathon.get("name"):
                    continue

                order += 1
                # Build description from hackathon data
                # Use LLM's description first (e.g., "Top 6 teams out of 500")
                description_parts = []
                if hackathon.get("description"):
                    description_parts.append(hackathon.get("description"))
                if hackathon.get("prize"):
                    description_parts.append(f"Prize: {hackathon.get('prize')}")
                if hackathon.get("prize_amount_usd"):
                    description_parts.append(f"${hackathon.get('prize_amount_usd')} USD")
                if hackathon.get("is_solana"):
                    description_parts.append("Solana Hackathon")
                description = " | ".join(description_parts) if description_parts else None

                values = {
                    "id": uuid4(),
                    "airtable_record_id": f"{partition_key}_hack_{order}",
                    "candidate_id": candidate_id,
                    "project_name": hackathon.get("name"),
                    "hackathon_name": hackathon.get("name"),
                    "description": description,
                    "is_hackathon": True,
                    "prize_won": hackathon.get("prize"),
                    "prize_amount_usd": hackathon.get("prize_amount_usd"),
                    "project_order": order,
                }

                stmt = insert(CandidateProject).values(**values)
                session.execute(stmt)
                projects_stored += 1

        session.commit()
        if projects_stored > 0:
            context.log.info(f"Stored {projects_stored} projects/hackathons for {partition_key}")

    def _store_candidate_attributes(
        self,
        session: Session,
        candidate_id: Any,
        partition_key: str,
        soft_attributes: dict[str, Any],
        model_version: str | None,
        context: OutputContext,
    ) -> None:
        """Store soft attributes from LLM response.

        LLM returns soft_attributes as:
        {
            "leadership": {"score": 4, "reasoning": "Led team of 10..."},
            "autonomy": {"score": 3, "reasoning": "..."},
            ...
        }
        """
        if not soft_attributes or not isinstance(soft_attributes, dict):
            return

        # Extract scores and reasoning
        leadership = soft_attributes.get("leadership", {})
        autonomy = soft_attributes.get("autonomy", {})
        technical_depth = soft_attributes.get("technical_depth", {})
        communication = soft_attributes.get("communication", {})
        growth_trajectory = soft_attributes.get("growth_trajectory", {})

        # Build combined reasoning text (for vectorization later)
        reasoning_parts = []
        for attr_name, attr_data in [
            ("Leadership", leadership),
            ("Autonomy", autonomy),
            ("Technical Depth", technical_depth),
            ("Communication", communication),
            ("Growth Trajectory", growth_trajectory),
        ]:
            if isinstance(attr_data, dict) and attr_data.get("reasoning"):
                reasoning_parts.append(f"{attr_name}: {attr_data['reasoning']}")

        reasoning_text = " | ".join(reasoning_parts) if reasoning_parts else None

        values = {
            "id": uuid4(),
            "candidate_id": candidate_id,
            "leadership_score": leadership.get("score") if isinstance(leadership, dict) else None,
            "autonomy_score": autonomy.get("score") if isinstance(autonomy, dict) else None,
            "technical_depth_score": (
                technical_depth.get("score") if isinstance(technical_depth, dict) else None
            ),
            "communication_score": (
                communication.get("score") if isinstance(communication, dict) else None
            ),
            "growth_trajectory_score": (
                growth_trajectory.get("score") if isinstance(growth_trajectory, dict) else None
            ),
            "reasoning": reasoning_text,
            "rating_model": model_version,
        }

        # Upsert: delete existing and insert new
        session.execute(
            delete(CandidateAttribute).where(CandidateAttribute.candidate_id == candidate_id)
        )
        stmt = insert(CandidateAttribute).values(**values)
        session.execute(stmt)
        session.commit()

        context.log.info(f"Stored soft attributes for {partition_key}")

    def _store_raw_job(self, context: OutputContext, data: dict[str, Any]) -> None:
        """Store raw job data using SQLAlchemy ORM with upsert."""
        session = self._get_session()
        partition_key = context.partition_key if hasattr(context, "partition_key") else None
        record_id = partition_key or data.get("airtable_record_id")

        # Serialize complex types (lists, dicts) to JSON strings for Text columns
        values = {
            "airtable_record_id": record_id,
            "source": data.get("source", "manual"),
            "source_id": _serialize_for_text(data.get("source_id")),
            "source_url": _serialize_for_text(data.get("source_url")),
            "job_title": _serialize_for_text(data.get("job_title")),
            "company_name": _serialize_for_text(data.get("company_name")),
            "job_description": _serialize_for_text(data.get("job_description")) or "",
            "company_website_url": _serialize_for_text(data.get("company_website_url")),
            "experience_level_raw": _serialize_for_text(data.get("experience_level_raw")),
            "location_raw": _serialize_for_text(data.get("location_raw")),
            "work_setup_raw": _serialize_for_text(data.get("work_setup_raw")),
            "status_raw": _serialize_for_text(data.get("status_raw")),
            "job_category_raw": _serialize_for_text(data.get("job_category_raw")),
            "x_url": _serialize_for_text(data.get("x_url")),
            "non_negotiables": _serialize_for_text(data.get("non_negotiables")),
            "nice_to_have": _serialize_for_text(data.get("nice_to_have")),
            "projected_salary": _serialize_for_text(data.get("projected_salary")),
            "processing_status": ProcessingStatusEnum.PENDING,
        }

        stmt = insert(RawJob).values(**values)
        if record_id:
            stmt = stmt.on_conflict_do_update(
                index_elements=["airtable_record_id"],
                set_={
                    k: getattr(stmt.excluded, k) for k in values.keys() if k != "airtable_record_id"
                },
            )

        session.execute(stmt)
        session.commit()
        session.close()

        context.log.info(f"Upserted raw job: {record_id}")

    def _store_normalized_job(self, context: OutputContext, data: dict[str, Any]) -> None:
        """Store normalized job data using SQLAlchemy ORM."""
        session = self._get_session()
        partition_key = context.partition_key if hasattr(context, "partition_key") else None
        record_id = partition_key or data.get("airtable_record_id")

        # Get raw job
        raw_job = session.execute(
            select(RawJob).where(RawJob.airtable_record_id == record_id)
        ).scalar_one_or_none()

        if not raw_job:
            context.log.error(f"Raw job not found for: {record_id}")
            session.close()
            return

        req = data.get("requirements") or {}
        comp = data.get("compensation") or {}
        contact = data.get("contact") or {}
        loc = data.get("location") or {}
        narratives = data.get("narratives") or {}
        soft_req = data.get("soft_attribute_requirements") or {}
        seniority_raw = (data.get("seniority_level") or "").strip().lower()
        seniority_enum = None
        if seniority_raw:
            try:
                seniority_enum = SeniorityEnum(seniority_raw)
            except ValueError:
                pass

        def _min_soft(key: str) -> int | None:
            v = soft_req.get(key)
            if v is None:
                return None
            if isinstance(v, int) and 1 <= v <= 5:
                return v
            return None

        values = {
            "airtable_record_id": record_id,
            "raw_job_id": raw_job.id,
            "job_title": data.get("title") or data.get("job_title", "Unknown"),
            "job_category": data.get("job_category"),
            "company_name": data.get("company_name") or raw_job.company_name or "Unknown",
            "job_description": data.get("job_description"),
            "role_summary": data.get("role_description") or data.get("role_summary"),
            "responsibilities": data.get("responsibilities"),
            "nice_to_haves": data.get("nice_to_haves"),
            "benefits": data.get("benefits"),
            "team_context": data.get("team_context"),
            "seniority_level": seniority_enum,
            "education_required": req.get("education_required"),
            "domain_experience": req.get("domain_experience"),
            "tech_stack": data.get("tech_stack"),
            "min_years_experience": req.get("years_of_experience_min"),
            "max_years_experience": req.get("years_of_experience_max"),
            "salary_min": comp.get("salary_min"),
            "salary_max": comp.get("salary_max"),
            "salary_currency": (comp.get("currency") or "USD")[:10],
            "has_equity": comp.get("equity"),
            "location_type": _parse_location_type(loc.get("type")),
            "locations": loc.get("locations"),
            "timezone_requirements": loc.get("timezone_requirements"),
            "employment_type": _parse_employment_types(data.get("employment_type")),
            "min_leadership_score": _min_soft("leadership"),
            "min_autonomy_score": _min_soft("autonomy"),
            "min_technical_depth_score": _min_soft("technical_depth"),
            "min_communication_score": _min_soft("communication"),
            "min_growth_trajectory_score": _min_soft("growth_trajectory"),
            "hiring_manager_name": contact.get("hiring_manager_name")
            or data.get("hiring_manager_name"),
            "hiring_manager_email": contact.get("hiring_manager_email")
            or data.get("hiring_manager_email"),
            "application_url": contact.get("application_url") or data.get("application_url"),
            "prompt_version": data.get("prompt_version"),
            "model_version": data.get("model_version"),
            "confidence_score": data.get("confidence_score"),
            "normalized_json": data.get("normalized_json"),
            "narrative_experience": narratives.get("experience")
            or data.get("narrative_experience"),
            "narrative_domain": narratives.get("domain") or data.get("narrative_domain"),
            "narrative_personality": narratives.get("personality")
            or data.get("narrative_personality"),
            "narrative_impact": narratives.get("impact") or data.get("narrative_impact"),
            "narrative_technical": narratives.get("technical") or data.get("narrative_technical"),
            "narrative_role": narratives.get("role") or data.get("narrative_role"),
        }

        stmt = insert(NormalizedJob).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["airtable_record_id"],
            set_={k: getattr(stmt.excluded, k) for k in values.keys() if k != "airtable_record_id"},
        )

        session.execute(stmt)
        session.commit()

        # Resolve required skills and write job_required_skills
        normalized_job = session.execute(
            select(NormalizedJob).where(NormalizedJob.airtable_record_id == record_id)
        ).scalar_one()
        session.execute(
            delete(JobRequiredSkill).where(JobRequiredSkill.job_id == normalized_job.id)
        )
        must_have = req.get("must_have_skills") or []
        nice_to_have = req.get("nice_to_have_skills") or []

        def _skill_entry(
            entry: Any,
        ) -> tuple[str | None, str | None, int | None, int | None]:
            """Normalize skill entry to (name, expected_capability, min_years, min_level)."""
            if isinstance(entry, dict):
                name = (entry.get("name") or "").strip() or None
                cap = entry.get("expected_capability")
                cap = (cap.strip() if isinstance(cap, str) and cap.strip() else None) or None
                min_years = entry.get("min_years")
                min_years = int(min_years) if min_years is not None else None
                min_level = entry.get("min_level")
                min_level = int(min_level) if min_level is not None else None
                if min_level is not None and not (1 <= min_level <= 10):
                    min_level = None
                return name, cap, min_years, min_level
            if isinstance(entry, str) and entry.strip():
                return entry.strip(), None, None, None
            return None, None, None, None

        added_skill_ids: set[UUID] = set()
        for entry in must_have:
            skill_name, expected_capability, min_years, min_level = _skill_entry(entry)
            if not skill_name:
                continue
            skill_id = get_or_create_skill(session, skill_name, is_requirement=True)
            if skill_id and skill_id not in added_skill_ids:
                added_skill_ids.add(skill_id)
                session.add(
                    JobRequiredSkill(
                        id=uuid4(),
                        job_id=normalized_job.id,
                        skill_id=skill_id,
                        requirement_type=RequirementTypeEnum.MUST_HAVE,
                        min_years=min_years,
                        min_level=min_level,
                        expected_capability=expected_capability,
                    )
                )
        for entry in nice_to_have:
            skill_name, expected_capability, min_years, min_level = _skill_entry(entry)
            if not skill_name:
                continue
            skill_id = get_or_create_skill(session, skill_name, is_requirement=True)
            if skill_id and skill_id not in added_skill_ids:
                added_skill_ids.add(skill_id)
                session.add(
                    JobRequiredSkill(
                        id=uuid4(),
                        job_id=normalized_job.id,
                        skill_id=skill_id,
                        requirement_type=RequirementTypeEnum.NICE_TO_HAVE,
                        min_years=min_years,
                        min_level=min_level,
                        expected_capability=expected_capability,
                    )
                )
        session.commit()

        raw_job.processing_status = ProcessingStatusEnum.COMPLETED
        session.commit()
        session.close()

        context.log.info(f"Upserted normalized job: {record_id}")

    def _store_matches(self, context: OutputContext, records: list[dict[str, Any]]) -> None:
        """Store match data (list of match dicts) using SQLAlchemy ORM.

        Replaces all existing matches for this job: deletes by job_id then inserts
        the new list (one partition = one job, so all records share the same job_id).
        """
        if not records:
            context.log.info("No matches to store")
            return
        session = self._get_session()
        job_id_raw = records[0].get("job_id")
        if job_id_raw is not None:
            job_id = UUID(str(job_id_raw)) if not isinstance(job_id_raw, UUID) else job_id_raw
            deleted = session.execute(delete(Match).where(Match.job_id == job_id))
            context.log.info(
                f"Deleted existing matches for job_id={job_id} (rowcount={deleted.rowcount})"
            )
        float_keys = {
            "match_score",
            "skills_match_score",
            "experience_match_score",
            "compensation_match_score",
            "location_match_score",
            "role_similarity_score",
            "domain_similarity_score",
            "culture_similarity_score",
        }
        for data in records:
            values = {
                "candidate_id": data.get("candidate_id"),
                "job_id": data.get("job_id"),
                "match_score": data.get("match_score", 0.0),
                "skills_match_score": data.get("skills_match_score"),
                "experience_match_score": data.get("experience_match_score"),
                "compensation_match_score": data.get("compensation_match_score"),
                "location_match_score": data.get("location_match_score"),
                "role_similarity_score": data.get("role_similarity_score"),
                "domain_similarity_score": data.get("domain_similarity_score"),
                "culture_similarity_score": data.get("culture_similarity_score"),
                "matching_skills": data.get("matching_skills"),
                "missing_skills": data.get("missing_skills"),
                "match_reasoning": data.get("match_reasoning"),
                "red_flags": data.get("red_flags"),
                "strengths": data.get("strengths"),
                "llm_fit_score": data.get("llm_fit_score"),
                "rank": data.get("rank"),
                "algorithm_version": data.get("algorithm_version"),
            }
            for k in float_keys:
                v = values.get(k)
                if v is not None:
                    values[k] = float(v)
            stmt = insert(Match).values(**values)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_candidate_job",
                set_={
                    "match_score": stmt.excluded.match_score,
                    "skills_match_score": stmt.excluded.skills_match_score,
                    "experience_match_score": stmt.excluded.experience_match_score,
                    "compensation_match_score": stmt.excluded.compensation_match_score,
                    "location_match_score": stmt.excluded.location_match_score,
                    "role_similarity_score": stmt.excluded.role_similarity_score,
                    "domain_similarity_score": stmt.excluded.domain_similarity_score,
                    "culture_similarity_score": stmt.excluded.culture_similarity_score,
                    "matching_skills": stmt.excluded.matching_skills,
                    "missing_skills": stmt.excluded.missing_skills,
                    "match_reasoning": stmt.excluded.match_reasoning,
                    "red_flags": stmt.excluded.red_flags,
                    "strengths": stmt.excluded.strengths,
                    "llm_fit_score": stmt.excluded.llm_fit_score,
                    "rank": stmt.excluded.rank,
                    "algorithm_version": stmt.excluded.algorithm_version,
                },
            )
            session.execute(stmt)
        session.commit()
        session.close()
        context.log.info(f"Stored {len(records)} matches")

    def _store_candidate_role_fitness(
        self, context: OutputContext, records: list[dict[str, Any]]
    ) -> None:
        """Store candidate_role_fitness rows; replace all for the candidate(s) in the list."""
        if not records:
            context.log.info("No candidate_role_fitness records to store")
            return
        session = self._get_session()
        candidate_ids = {r.get("candidate_id") for r in records if r.get("candidate_id")}
        for cid in candidate_ids:
            session.execute(
                delete(CandidateRoleFitness).where(
                    CandidateRoleFitness.candidate_id == UUID(str(cid))
                )
            )
        for data in records:
            cid = data.get("candidate_id")
            if not cid:
                continue
            stmt = insert(CandidateRoleFitness).values(
                candidate_id=UUID(str(cid)),
                role_name=data.get("role_name", ""),
                fitness_score=float(data.get("fitness_score", 0.0)),
                score_breakdown=data.get("score_breakdown"),
                algorithm_version=data.get("algorithm_version"),
            )
            session.execute(stmt)
        session.commit()
        session.close()
        context.log.info(f"Stored {len(records)} candidate_role_fitness rows")

    def _store_candidate_github_commit_history(
        self, context: OutputContext, data: dict[str, Any]
    ) -> None:
        """Store commit history from blobless git clone."""
        if not data:
            return
        session = self._get_session()
        candidate_id = data.get("candidate_id")
        if not candidate_id:
            session.close()
            return
        candidate_uuid = UUID(str(candidate_id))
        values = {
            "candidate_id": candidate_uuid,
            "airtable_record_id": data.get("airtable_record_id"),
            "github_username": data.get("github_username", ""),
            "commit_history": data.get("commit_history", {}),
        }
        stmt = insert(CandidateGithubCommitHistory).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["candidate_id"],
            set_={
                "github_username": stmt.excluded.github_username,
                "commit_history": stmt.excluded.commit_history,
            },
        )
        session.execute(stmt)
        session.commit()
        session.close()
        context.log.info(f"Stored candidate_github_commit_history for candidate {candidate_id}")

    def _store_candidate_skill_verification(
        self, context: OutputContext, data: dict[str, Any]
    ) -> None:
        """Update candidate_skills verification columns and normalized_candidates.skill_verification_score."""
        if not data or data.get("skipped"):
            return
        session = self._get_session()
        candidate_id_raw = data.get("candidate_id")
        if not candidate_id_raw:
            session.close()
            return
        candidate_uuid = UUID(str(candidate_id_raw))
        score = data.get("skill_verification_score")
        per_skill = data.get("per_skill_results", [])

        valid_statuses = {"verified", "unverified", "no_evidence", "skipped"}
        for item in per_skill:
            skill_name = item.get("skill_name", "").strip()
            if not skill_name:
                continue
            status_str = item.get("verification_status")
            evidence = item.get("verification_evidence")
            verified_at_str = item.get("verified_at")

            status_value = None
            if status_str:
                normalized = str(status_str).strip().lower()
                if normalized in valid_statuses:
                    status_value = normalized

            cs = session.execute(
                select(CandidateSkill)
                .join(Skill, CandidateSkill.skill_id == Skill.id)
                .where(
                    CandidateSkill.candidate_id == candidate_uuid,
                    Skill.name.ilike(skill_name),
                )
            ).scalar_one_or_none()
            if cs:
                verified_at_val = None
                if verified_at_str:
                    verified_at_val = (
                        datetime.fromisoformat(verified_at_str)
                        if isinstance(verified_at_str, str)
                        else verified_at_str
                    )
                if status_value:
                    session.execute(
                        text(
                            "UPDATE candidate_skills SET verification_status = CAST(:status AS skill_verification_status_enum), "
                            "verification_evidence = CAST(:evidence AS jsonb), verified_at = :verified_at WHERE id = CAST(:id AS uuid)"
                        ),
                        {
                            "status": status_value,
                            "evidence": json.dumps(evidence) if evidence else None,
                            "verified_at": verified_at_val,
                            "id": str(cs.id),
                        },
                    )
                else:
                    session.execute(
                        update(CandidateSkill)
                        .where(CandidateSkill.id == cs.id)
                        .values(
                            verification_evidence=evidence,
                            verified_at=verified_at_val,
                        )
                    )

        nc = session.execute(
            select(NormalizedCandidate).where(NormalizedCandidate.id == candidate_uuid)
        ).scalar_one_or_none()
        if nc and score is not None:
            nc.skill_verification_score = float(score)

        session.commit()
        session.close()
        context.log.info(
            f"Updated skill verification for candidate {candidate_id_raw} "
            f"(score={score}, {len(per_skill)} skills)"
        )
