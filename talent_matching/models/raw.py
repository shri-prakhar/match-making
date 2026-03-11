"""Raw input data models - direct imports from Airtable."""

import uuid
from datetime import datetime
from uuid import uuid4

from sqlalchemy import DateTime, Enum, Float, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from talent_matching.models.base import Base
from talent_matching.models.enums import CVExtractionMethodEnum, ProcessingStatusEnum


class RawCandidate(Base):
    """Raw candidate data as imported from Airtable.

    Stores the original data exactly as received, before any normalization.
    """

    __tablename__ = "raw_candidates"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    airtable_record_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)

    # Source tracking
    source: Mapped[str] = mapped_column(
        String(50), nullable=False, default="airtable"
    )  # airtable, form, referral
    source_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Original fields from Airtable
    full_name: Mapped[str] = mapped_column(Text, nullable=False)
    location_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_job_categories_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    skills_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    cv_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    cv_text: Mapped[str | None] = mapped_column(Text, nullable=True)  # Original from Airtable
    cv_text_pdf: Mapped[str | None] = mapped_column(Text, nullable=True)  # Extracted from PDF

    # CV extraction metadata (for PDF processing)
    cv_extraction_method: Mapped[CVExtractionMethodEnum | None] = mapped_column(
        Enum(CVExtractionMethodEnum, name="cv_extraction_method_enum"),
        nullable=True,
    )  # How cv_text_pdf was obtained: pdf_text, mistral_ocr, native, failed
    cv_extraction_cost_usd: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )  # Cost of PDF extraction
    cv_extraction_model: Mapped[str | None] = mapped_column(
        String(100), nullable=True
    )  # Model used for extraction (e.g., "openai/gpt-4o-mini")
    cv_extraction_pages: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )  # Number of pages in the PDF

    professional_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    proof_of_work: Mapped[str | None] = mapped_column(Text, nullable=True)
    salary_range_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    x_profile_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    linkedin_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    earn_profile_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    github_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    work_experience_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_status_raw: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # Lead, Fraud, Hired, etc.

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    # Processing status
    processing_status: Mapped[ProcessingStatusEnum] = mapped_column(
        Enum(ProcessingStatusEnum, name="processing_status_enum"),
        default=ProcessingStatusEnum.PENDING,
    )
    processing_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class RawJob(Base):
    """Raw job description as imported from external sources.

    Stores the original job data before normalization.

    Supports two ingestion modes:
    1. **Unstructured**: Only `job_description` is provided (e.g., pasted job posting).
       LLM extracts title, company, skills, requirements, etc.
    2. **Structured**: Pre-parsed fields from Notion/Airtable with `job_title`, `company_name`, etc.

    The normalization pipeline handles both - if only job_description is present,
    all fields are extracted by the LLM.
    """

    __tablename__ = "raw_jobs"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    airtable_record_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)

    # Source tracking
    source: Mapped[str] = mapped_column(
        String(50), nullable=False, default="manual"
    )  # manual, api, partner, notion, paste
    source_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # CORE FIELDS
    # For unstructured input: only job_description is required, LLM extracts the rest
    # For structured input: fields come pre-parsed from Notion/Airtable
    # ═══════════════════════════════════════════════════════════════════
    job_title: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # Can be extracted by LLM if not provided
    company_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_description: Mapped[str] = mapped_column(
        Text, nullable=False
    )  # Raw job posting text - always required
    company_website_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ═══════════════════════════════════════════════════════════════════
    # STRUCTURED FIELDS (from Notion Job Board)
    # These are raw values before normalization
    # ═══════════════════════════════════════════════════════════════════
    experience_level_raw: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # "Junior", "Mid", "Senior" (multi-select JSON array)
    location_raw: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # "Europe", "Global", etc. (multi-select JSON array)
    work_setup_raw: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # "Remote", "Hybrid", "On-Site" (multi-select JSON array)
    status_raw: Mapped[str | None] = mapped_column(Text, nullable=True)  # "Active", "Closed"
    job_category_raw: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # "Frontend Engineer", etc. (multi-select JSON array)
    x_url: Mapped[str | None] = mapped_column(Text, nullable=True)  # Company X/Twitter profile

    # ═══════════════════════════════════════════════════════════════════
    # RECRUITER INPUT (from ATS Smart Job Profiles view)
    # Free-text fields the recruiter fills in; fed to the LLM during
    # normalization so it can prioritize must-have vs nice-to-have skills.
    # ═══════════════════════════════════════════════════════════════════
    non_negotiables: Mapped[str | None] = mapped_column(Text, nullable=True)
    nice_to_have: Mapped[str | None] = mapped_column(Text, nullable=True)
    projected_salary: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    # Processing status
    processing_status: Mapped[ProcessingStatusEnum] = mapped_column(
        Enum(ProcessingStatusEnum, name="processing_status_enum"),
        default=ProcessingStatusEnum.PENDING,
    )
    processing_error: Mapped[str | None] = mapped_column(Text, nullable=True)
