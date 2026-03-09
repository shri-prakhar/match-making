"""Matchmaking resource: job required skills, candidate skills, and DB helpers for scoring."""

from typing import Any
from uuid import UUID, uuid4

from dagster import ConfigurableResource
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from talent_matching.db import get_session
from talent_matching.models.candidates import CandidateSkill, NormalizedCandidate
from talent_matching.models.enums import RequirementTypeEnum
from talent_matching.models.jobs import JobRequiredSkill, NormalizedJob
from talent_matching.models.skills import Skill
from talent_matching.models.vectors import CandidateVector
from talent_matching.skills.resolver import get_or_create_skill, load_alias_map
from talent_matching.utils.airtable_mapper import (
    NORMALIZED_CANDIDATE_SYNCABLE_FIELDS,
    NORMALIZED_JOB_SYNCABLE_FIELDS,
)


class MatchmakingResource(ConfigurableResource):
    """Provides job required skills (and optional helpers) for the matches asset."""

    @staticmethod
    def _get_session() -> Session:
        return get_session()

    def get_job_required_skills(
        self,
        job_ids: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """Return for each job_id the list of required skills with name, type, and expected_capability.

        Args:
            job_ids: List of normalized job UUIDs (strings).

        Returns:
            Dict mapping job_id (str) to list of {"skill_name": str, "requirement_type": "must_have"|"nice_to_have", "min_years": int|None, "expected_capability": str|None}.
        """
        if not job_ids:
            return {}
        uuids = [UUID(jid) if isinstance(jid, str) else jid for jid in job_ids]
        session = self._get_session()
        stmt = (
            select(
                JobRequiredSkill.job_id,
                Skill.name,
                JobRequiredSkill.requirement_type,
                JobRequiredSkill.min_years,
                JobRequiredSkill.min_level,
                JobRequiredSkill.expected_capability,
            )
            .join(Skill, JobRequiredSkill.skill_id == Skill.id)
            .where(JobRequiredSkill.job_id.in_(uuids))
        )
        rows = session.execute(stmt).all()
        alias_map = load_alias_map(session)
        session.close()

        result: dict[str, list[dict[str, Any]]] = {jid: [] for jid in job_ids}
        for row in rows:
            job_id, name, req_type, min_years, min_level, expected_capability = row
            jid_str = str(job_id)
            result.setdefault(jid_str, []).append(
                {
                    "skill_name": alias_map.get(name, name),
                    "requirement_type": (
                        RequirementTypeEnum.NICE_TO_HAVE.value
                        if req_type == RequirementTypeEnum.NICE_TO_HAVE
                        else RequirementTypeEnum.MUST_HAVE.value
                    ),
                    "min_years": min_years,
                    "min_level": min_level,
                    "expected_capability": expected_capability,
                }
            )
        return result

    def get_candidate_skills(
        self,
        candidate_ids: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """Return for each candidate_id the list of skills with name, rating, years_experience.

        Args:
            candidate_ids: List of normalized candidate UUIDs (strings).

        Returns:
            Dict mapping candidate_id (str) to list of {"skill_name": str, "rating": int 1-10, "years_experience": int or None}.
        """
        if not candidate_ids:
            return {}
        uuids = [UUID(cid) if isinstance(cid, str) else cid for cid in candidate_ids]
        session = self._get_session()
        stmt = (
            select(
                CandidateSkill.candidate_id,
                Skill.name,
                CandidateSkill.rating,
                CandidateSkill.years_experience,
            )
            .join(Skill, CandidateSkill.skill_id == Skill.id)
            .where(CandidateSkill.candidate_id.in_(uuids))
        )
        rows = session.execute(stmt).all()
        alias_map = load_alias_map(session)
        session.close()

        result: dict[str, list[dict[str, Any]]] = {cid: [] for cid in candidate_ids}
        for cand_id, name, rating, years in rows:
            cid_str = str(cand_id)
            result.setdefault(cid_str, []).append(
                {
                    "skill_name": alias_map.get(name, name),
                    "rating": int(rating) if rating is not None else 5,
                    "years_experience": int(years) if years is not None else None,
                }
            )
        return result

    def get_candidate_vectors(
        self,
        raw_candidate_ids: list[str],
    ) -> dict[str, dict[str, list[float]]]:
        """Load candidate vectors from DB, keyed by raw_candidate_id -> vector_type -> vector.

        Only loads vectors for the specified candidates instead of all 7k+.
        Used by the matches asset to avoid the AllPartitionMapping OOM.

        Args:
            raw_candidate_ids: List of raw_candidates.id UUIDs (as strings).

        Returns:
            Dict mapping raw_candidate_id (str) to {vector_type: vector_list}.
        """
        if not raw_candidate_ids:
            return {}
        uuids = [UUID(rid) if isinstance(rid, str) else rid for rid in raw_candidate_ids]
        session = self._get_session()
        stmt = select(
            CandidateVector.candidate_id,
            CandidateVector.vector_type,
            CandidateVector.vector,
        ).where(CandidateVector.candidate_id.in_(uuids))
        rows = session.execute(stmt).all()
        session.close()

        result: dict[str, dict[str, list[float]]] = {}
        for cand_id, vtype, vec in rows:
            cid = str(cand_id)
            if cid not in result:
                result[cid] = {}
            result[cid][vtype] = list(vec)
        return result

    def get_normalized_candidate_by_airtable_record_id(
        self, airtable_record_id: str
    ) -> dict[str, Any] | None:
        """Load a single NormalizedCandidate row by airtable_record_id as a dict of syncable fields.

        Returns None if no row exists. Keys are attribute names (e.g. full_name, professional_summary).
        Used by airtable_candidate_sync to build the Airtable PATCH payload.
        """
        session = self._get_session()
        row = session.execute(
            select(NormalizedCandidate).where(
                NormalizedCandidate.airtable_record_id == airtable_record_id
            )
        ).scalar_one_or_none()
        if row is None:
            session.close()
            return None
        candidate = {name: getattr(row, name) for name in NORMALIZED_CANDIDATE_SYNCABLE_FIELDS}
        session.close()
        return candidate

    def get_normalized_job_by_airtable_record_id(
        self, airtable_record_id: str
    ) -> dict[str, Any] | None:
        """Load a NormalizedJob row and its required_skills by airtable_record_id.

        Returns a dict of syncable fields including virtual 'must_have_skills' and
        'nice_to_have_skills' as comma-separated skill name strings.
        """
        session = self._get_session()
        row = session.execute(
            select(NormalizedJob).where(NormalizedJob.airtable_record_id == airtable_record_id)
        ).scalar_one_or_none()
        if row is None:
            session.close()
            return None

        scalar_fields = [
            f
            for f in NORMALIZED_JOB_SYNCABLE_FIELDS
            if f not in ("must_have_skills", "nice_to_have_skills")
        ]
        job: dict[str, Any] = {}
        for name in scalar_fields:
            job[name] = getattr(row, name, None)

        skills = session.execute(
            select(Skill.name, JobRequiredSkill.requirement_type)
            .join(Skill, JobRequiredSkill.skill_id == Skill.id)
            .where(JobRequiredSkill.job_id == row.id)
        ).all()
        must_have = [s.name for s in skills if s.requirement_type == RequirementTypeEnum.MUST_HAVE]
        nice_to_have = [
            s.name for s in skills if s.requirement_type == RequirementTypeEnum.NICE_TO_HAVE
        ]
        job["must_have_skills"] = must_have
        job["nice_to_have_skills"] = nice_to_have

        session.close()
        return job

    def update_normalized_job_from_airtable(
        self, airtable_record_id: str, fields: dict[str, Any]
    ) -> bool:
        """Update a normalized_jobs row (and its skills) from human-edited Airtable fields.

        Args:
            airtable_record_id: The Airtable record ID for the job.
            fields: Dict with DB column names as keys (output of airtable_normalized_job_fields_to_db).

        Returns:
            True if the row was found and updated, False if no row exists.
        """
        session = self._get_session()
        job = session.execute(
            select(NormalizedJob).where(NormalizedJob.airtable_record_id == airtable_record_id)
        ).scalar_one_or_none()
        if job is None:
            session.close()
            return False

        must_have_names: list[str] = fields.pop("must_have_skills", None) or []
        nice_to_have_names: list[str] = fields.pop("nice_to_have_skills", None) or []

        for col, value in fields.items():
            if hasattr(job, col):
                setattr(job, col, value)
        session.commit()

        if must_have_names or nice_to_have_names:
            session.execute(delete(JobRequiredSkill).where(JobRequiredSkill.job_id == job.id))
            added_ids: set[UUID] = set()
            for skill_name in must_have_names:
                skill_id = get_or_create_skill(
                    session, skill_name, created_by="airtable_feedback", is_requirement=True
                )
                if skill_id and skill_id not in added_ids:
                    added_ids.add(skill_id)
                    session.add(
                        JobRequiredSkill(
                            id=uuid4(),
                            job_id=job.id,
                            skill_id=skill_id,
                            requirement_type=RequirementTypeEnum.MUST_HAVE,
                        )
                    )
            for skill_name in nice_to_have_names:
                skill_id = get_or_create_skill(
                    session, skill_name, created_by="airtable_feedback", is_requirement=True
                )
                if skill_id and skill_id not in added_ids:
                    added_ids.add(skill_id)
                    session.add(
                        JobRequiredSkill(
                            id=uuid4(),
                            job_id=job.id,
                            skill_id=skill_id,
                            requirement_type=RequirementTypeEnum.NICE_TO_HAVE,
                        )
                    )
            session.commit()

        session.close()
        return True
