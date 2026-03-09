"""Airtable API resource for fetching candidate data.

This resource connects to Airtable to fetch candidate records from the
Tech Assignment table. It supports:
- Fetching all records
- Fetching specific records by ID
- Tracking record modification times for incremental syncs
"""

import hashlib
from typing import Any, ClassVar

import httpx
from dagster import ConfigurableResource
from pydantic import Field

from talent_matching.utils.airtable_mapper import (
    map_airtable_row_to_raw_candidate,
    map_airtable_row_to_raw_job,
)


class AirtableResource(ConfigurableResource):
    """Airtable API resource for fetching candidate and job data.

    This resource connects to the Airtable REST API to fetch records.
    It supports incremental syncs by tracking record modification times.

    Configuration:
        base_id: The Airtable base ID (starts with 'app')
        table_id: The Airtable table ID (starts with 'tbl')
        api_key: Personal access token for Airtable API

    Example:
        ```python
        airtable = AirtableResource(
            base_id="appXXXXXXXXXXXXXX",
            table_id="tblXXXXXXXXXXXXXX",
            api_key=EnvVar("AIRTABLE_API_KEY"),
        )
        ```
    """

    base_id: str = Field(
        description="Airtable base ID (starts with 'app')",
    )
    table_id: str = Field(
        description="Airtable table ID (starts with 'tbl')",
    )
    api_key: str = Field(
        description="Airtable Personal Access Token (read; used for write too unless write_api_key is set)",
    )
    write_api_key: str | None = Field(
        default=None,
        description="Optional token with data.records:write scope for PATCH. If unset, api_key is used.",
    )

    # Column name mapping from Airtable to our model fields
    COLUMN_MAPPING: dict[str, str] = {
        "Full Name": "full_name",
        "Location": "location_raw",
        "Desired Job Category": "desired_job_categories_raw",
        "Skills": "skills_raw",
        "CV": "cv_url",
        "Professional summary": "professional_summary",
        "Proof of Work": "proof_of_work",
        "Salary Range": "salary_range_raw",
        "X Profile Link": "x_profile_url",
        "LinkedIn Profile": "linkedin_url",
        "Earn Profile": "earn_profile_url",
        "Git Hub Profile": "github_url",
        "Work Experience": "work_experience_raw",
        "Job Status": "job_status_raw",
    }

    @property
    def _base_url(self) -> str:
        """Base URL for Airtable API."""
        return f"https://api.airtable.com/v0/{self.base_id}/{self.table_id}"

    def _auth_headers(self, token: str | None = None) -> dict[str, str]:
        """Build request headers; token defaults to api_key for read, or use _write_headers for PATCH."""
        raw = token if token is not None else self.api_key
        resolved = raw.get_secret_value() if hasattr(raw, "get_secret_value") else raw
        return {
            "Authorization": f"Bearer {resolved}",
            "Content-Type": "application/json",
        }

    @property
    def _headers(self) -> dict[str, str]:
        """Request headers with authentication (read)."""
        return self._auth_headers()

    def _write_headers(self) -> dict[str, str]:
        """Request headers for write (PATCH); use write_api_key if set."""
        raw = self.write_api_key or self.api_key
        return self._auth_headers(raw)

    def fetch_all_records(self) -> list[dict[str, Any]]:
        """Fetch all records from the Airtable table.

        Handles pagination automatically by following the 'offset' cursor.

        Returns:
            List of records with Airtable record IDs and mapped field names.
        """
        all_records: list[dict[str, Any]] = []
        offset: str | None = None

        with httpx.Client(timeout=30.0) as client:
            while True:
                params: dict[str, str] = {}
                if offset:
                    params["offset"] = offset

                response = client.get(
                    self._base_url,
                    headers=self._headers,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

                for record in data.get("records", []):
                    mapped_record = self._map_record(record)
                    all_records.append(mapped_record)

                # Check for more pages
                offset = data.get("offset")
                if not offset:
                    break

        return all_records

    def fetch_record_by_id(self, record_id: str) -> dict[str, Any]:
        """Fetch a single record by its Airtable record ID.

        Args:
            record_id: The Airtable record ID (starts with 'rec')

        Returns:
            Mapped record dictionary.
        """
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self._base_url}/{record_id}",
                headers=self._headers,
            )
            response.raise_for_status()
            record = response.json()

        return self._map_record(record)

    def fetch_records_modified_since(self, since_iso: str) -> list[dict[str, Any]]:
        """Fetch records modified since a given timestamp.

        Uses Airtable's filterByFormula to get only recently modified records.

        Args:
            since_iso: ISO 8601 timestamp (e.g., "2024-01-15T10:30:00.000Z")

        Returns:
            List of records modified after the given timestamp.
        """
        formula = f"LAST_MODIFIED_TIME() > '{since_iso}'"

        all_records: list[dict[str, Any]] = []
        offset: str | None = None

        with httpx.Client(timeout=30.0) as client:
            while True:
                params: dict[str, str] = {"filterByFormula": formula}
                if offset:
                    params["offset"] = offset

                response = client.get(
                    self._base_url,
                    headers=self._headers,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

                for record in data.get("records", []):
                    mapped_record = self._map_record(record)
                    all_records.append(mapped_record)

                offset = data.get("offset")
                if not offset:
                    break

        return all_records

    def _map_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Map an Airtable record to our internal field names.

        Args:
            record: Raw Airtable record with 'id', 'fields', 'createdTime'

        Returns:
            Mapped record with standardized field names.
        """
        # Use shared mapper utility
        mapped = map_airtable_row_to_raw_candidate(record, self.COLUMN_MAPPING)

        # Add created_time from Airtable metadata
        mapped["created_time"] = record.get("createdTime")

        # Compute a data version hash for this record
        mapped["_data_version"] = self._compute_record_hash(mapped)

        return mapped

    def _compute_record_hash(self, mapped_record: dict[str, Any]) -> str:
        """Compute a hash of the record content for change detection.

        This enables Dagster to skip re-processing unchanged records.
        The hash excludes metadata fields that don't affect processing.

        Args:
            mapped_record: The mapped record dictionary

        Returns:
            SHA-256 hash of the content fields.
        """
        # Fields that affect processing (exclude metadata)
        content_fields = [
            "full_name",
            "location_raw",
            "desired_job_categories_raw",
            "skills_raw",
            "cv_url",
            "professional_summary",
            "proof_of_work",
            "salary_range_raw",
            "x_profile_url",
            "linkedin_url",
            "earn_profile_url",
            "github_url",
            "work_experience_raw",
            "job_status_raw",
        ]

        content = {k: mapped_record.get(k) for k in content_fields}
        content_str = str(sorted(content.items()))

        return hashlib.sha256(content_str.encode()).hexdigest()[:16]

    def get_all_record_ids(self) -> list[str]:
        """Get all record IDs from the table.

        This is useful for initializing dynamic partitions.
        Only fetches the record ID field to minimize data transfer.

        Returns:
            List of Airtable record IDs.
        """
        record_ids: list[str] = []
        offset: str | None = None

        with httpx.Client(timeout=30.0) as client:
            while True:
                params: dict[str, Any] = {"fields[]": []}  # Empty fields = only IDs
                if offset:
                    params["offset"] = offset

                response = client.get(
                    self._base_url,
                    headers=self._headers,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

                for record in data.get("records", []):
                    record_ids.append(record["id"])

                offset = data.get("offset")
                if not offset:
                    break

        return record_ids

    def update_record(self, record_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        """Update an Airtable candidate record with the given fields (PATCH).

        Args:
            record_id: Airtable record ID (e.g. recXXX).
            fields: Dict of Airtable column names to values.

        Returns:
            The updated Airtable record as returned by the API.

        Raises:
            httpx.HTTPStatusError: On API errors. 403 with a hint to use a token
                with data.records:write scope (or set AIRTABLE_WRITE_TOKEN).
        """
        with httpx.Client(timeout=30.0) as client:
            response = client.patch(
                f"{self._base_url}/{record_id}",
                headers=self._write_headers(),
                json={"fields": fields},
            )
            if response.status_code == 403:
                raise httpx.HTTPStatusError(
                    "403 Forbidden: Airtable token does not have write access. "
                    "Create a token at https://airtable.com/create/tokens with scope "
                    "'data.records:write' (and access to this base), then set AIRTABLE_API_KEY "
                    "or AIRTABLE_WRITE_TOKEN in .env and restart Dagster.",
                    request=response.request,
                    response=response,
                )
            if response.status_code == 422:
                body = response.text
                raise httpx.HTTPStatusError(
                    f"422 Unprocessable Entity for record {record_id}: {body}",
                    request=response.request,
                    response=response,
                )
            response.raise_for_status()
            return response.json()


class AirtableJobsResource(ConfigurableResource):
    """Airtable API resource for fetching job records (e.g. Customers STT table).

    Same interface as AirtableResource but uses jobs table and job field mapping.
    Configure with AIRTABLE_BASE_ID, AIRTABLE_JOBS_TABLE_ID, AIRTABLE_API_KEY.
    """

    base_id: str = Field(description="Airtable base ID (starts with 'app')")
    table_id: str = Field(description="Airtable jobs table ID (starts with 'tbl')")
    api_key: str = Field(description="Airtable Personal Access Token")
    write_api_key: str | None = Field(
        default=None,
        description="Optional token with data.records:write scope for PATCH.",
    )

    @property
    def _base_url(self) -> str:
        return f"https://api.airtable.com/v0/{self.base_id}/{self.table_id}"

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _write_headers(self) -> dict[str, str]:
        raw = self.write_api_key or self.api_key
        token = raw.get_secret_value() if hasattr(raw, "get_secret_value") else raw
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def fetch_record_by_id(self, record_id: str) -> dict[str, Any]:
        """Fetch a single job record by Airtable record ID. Returns mapped job fields."""
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self._base_url}/{record_id}",
                headers=self._headers,
            )
            response.raise_for_status()
            record = response.json()
        return self._map_record(record)

    def _map_record(self, record: dict[str, Any]) -> dict[str, Any]:
        mapped = map_airtable_row_to_raw_job(record)
        mapped["created_time"] = record.get("createdTime")
        mapped["_data_version"] = self._compute_record_hash(mapped)
        return mapped

    def _compute_record_hash(self, mapped_record: dict[str, Any]) -> str:
        content_fields = [
            "job_description_link",
            "job_title_raw",
            "company_name",
            "x_url",
            "company_website_url",
            "non_negotiables",
            "nice_to_have",
            "location_raw",
            "projected_salary",
        ]
        content = {k: mapped_record.get(k) for k in content_fields}
        content_str = str(sorted(content.items()))
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]

    def get_all_record_ids(self) -> list[str]:
        """Get all record IDs from the jobs table for dynamic partitions."""
        record_ids: list[str] = []
        offset: str | None = None
        with httpx.Client(timeout=30.0) as client:
            while True:
                params: dict[str, Any] = {"fields[]": []}
                if offset:
                    params["offset"] = offset
                response = client.get(
                    self._base_url,
                    headers=self._headers,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()
                for record in data.get("records", []):
                    record_ids.append(record["id"])
                offset = data.get("offset")
                if not offset:
                    break
        return record_ids

    def fetch_record_raw_fields(self, record_id: str) -> dict[str, Any]:
        """Fetch a single record and return the raw Airtable fields dict (not mapped).

        Useful for reading (N)-prefixed fields.
        """
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self._base_url}/{record_id}",
                headers=self._headers,
            )
            response.raise_for_status()
            record = response.json()
        return record.get("fields", {})

    def update_record(self, record_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        """Update an Airtable job record with the given fields (PATCH).

        Args:
            record_id: Airtable record ID (e.g. recXXX).
            fields: Dict of Airtable column names to values (e.g. {"Hiring Job Title": "Senior Engineer"}).

        Returns:
            The updated Airtable record as returned by the API.
        """
        with httpx.Client(timeout=30.0) as client:
            response = client.patch(
                f"{self._base_url}/{record_id}",
                headers=self._write_headers(),
                json={"fields": fields},
            )
            if response.status_code == 403:
                raise httpx.HTTPStatusError(
                    "403 Forbidden: Airtable token does not have write access. "
                    "Create a token at https://airtable.com/create/tokens with scope "
                    "'data.records:write', then set AIRTABLE_API_KEY or AIRTABLE_WRITE_TOKEN.",
                    request=response.request,
                    response=response,
                )
            if response.status_code == 422:
                body = response.text
                raise httpx.HTTPStatusError(
                    f"422 Unprocessable Entity for record {record_id}: {body}",
                    request=response.request,
                    response=response,
                )
            response.raise_for_status()
            return response.json()


class AirtableATSResource(ConfigurableResource):
    """Airtable API resource for the ATS table (job process / matchmaking workflow).

    The ATS table drives the recruiting workflow. This resource polls for
    Job Status changes and reads/writes match-related columns.
    """

    base_id: str = Field(description="Airtable base ID (starts with 'app')")
    table_id: str = Field(description="ATS table ID (tblrbhITEIBOxwcQV)")
    api_key: str = Field(description="Airtable Personal Access Token")
    write_api_key: str | None = Field(
        default=None,
        description="Optional token with data.records:write scope for PATCH.",
    )
    matches_table_id: str | None = Field(
        default=None,
        description="Optional Matches table ID for storing score, pros, cons per candidate-job.",
    )
    matches_view_url: str | None = Field(
        default=None,
        description="Base URL of shared Matches table view (e.g. https://airtable.com/shrXXX). "
        "Pipeline appends ?filter_Job=<record_id> so each job links to its filtered matches.",
    )

    ATS_JOB_FIELDS: ClassVar[list[str]] = [
        "Open Position (Job Title)",
        "Job Status",
        "Job Description Text",
        "Job Description Link",
        "Company",
        "Preferred Location ",
        "Level",
        "Desired Job Category",
        "Work Set Up Preference",
        "Projected Salary",
        "Non Negotiables",
        "Nice-to-have",
        "Notes",
        "Last Job Status Change",
    ]

    @property
    def _base_url(self) -> str:
        return f"https://api.airtable.com/v0/{self.base_id}/{self.table_id}"

    @property
    def _headers(self) -> dict[str, str]:
        raw = self.api_key
        token = raw.get_secret_value() if hasattr(raw, "get_secret_value") else raw
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _write_headers(self) -> dict[str, str]:
        raw = self.write_api_key or self.api_key
        token = raw.get_secret_value() if hasattr(raw, "get_secret_value") else raw
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def fetch_records_by_status(self, status: str) -> list[dict[str, Any]]:
        """Fetch ATS records matching a specific Job Status value.

        Returns list of raw Airtable records (with 'id' and 'fields').
        """
        formula = f'{{Job Status}} = "{status}"'
        records: list[dict[str, Any]] = []
        offset: str | None = None

        with httpx.Client(timeout=30.0) as client:
            while True:
                params: list[tuple[str, str]] = [("filterByFormula", formula)]
                params.extend(("fields[]", f) for f in self.ATS_JOB_FIELDS)
                if offset:
                    params.append(("offset", offset))
                response = client.get(
                    self._base_url,
                    headers=self._headers,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()
                records.extend(data.get("records", []))
                offset = data.get("offset")
                if not offset:
                    break
        return records

    def fetch_record_by_id(self, record_id: str) -> dict[str, Any]:
        """Fetch a single ATS record by ID."""
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self._base_url}/{record_id}",
                headers=self._headers,
            )
            response.raise_for_status()
            return response.json()

    def update_record(self, record_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        """Update an ATS record (PATCH)."""
        with httpx.Client(timeout=30.0) as client:
            response = client.patch(
                f"{self._base_url}/{record_id}",
                headers=self._write_headers(),
                json={"fields": fields},
            )
            if response.status_code == 403:
                raise httpx.HTTPStatusError(
                    "403 Forbidden: token needs data.records:write scope for ATS table.",
                    request=response.request,
                    response=response,
                )
            if response.status_code == 422:
                body = response.text
                raise httpx.HTTPStatusError(
                    f"422 Unprocessable Entity for record {record_id}: {body}",
                    request=response.request,
                    response=response,
                )
            response.raise_for_status()
            return response.json()

    def map_ats_record_to_raw_job(self, record: dict[str, Any]) -> dict[str, Any]:
        """Map an ATS record to RawJob-compatible fields for Postgres ingestion."""
        fields = record.get("fields", {})

        company_links = fields.get("Company", [])
        company_name = None
        if isinstance(company_links, list) and company_links:
            company_name = company_links[0] if isinstance(company_links[0], str) else None

        location_values = fields.get("Preferred Location ", fields.get("Preferred Location", []))
        location_raw = ", ".join(location_values) if isinstance(location_values, list) else None

        level_values = fields.get("Level", [])
        level_raw = ", ".join(level_values) if isinstance(level_values, list) else None

        category_values = fields.get("Desired Job Category", [])
        category_raw = ", ".join(category_values) if isinstance(category_values, list) else None

        work_setup = fields.get("Work Set Up Preference", [])
        work_setup_raw = ", ".join(work_setup) if isinstance(work_setup, list) else None

        return {
            "airtable_record_id": record.get("id"),
            "source": "airtable_ats",
            "source_id": record.get("id"),
            "source_url": fields.get("Job Description Link"),
            "job_title": fields.get("Open Position (Job Title)"),
            "company_name": company_name,
            "job_description": fields.get("Job Description Text") or "",
            "company_website_url": None,
            "experience_level_raw": level_raw,
            "location_raw": location_raw,
            "work_setup_raw": work_setup_raw,
            "status_raw": fields.get("Job Status"),
            "job_category_raw": category_raw,
            "x_url": None,
        }

    def replace_matches_for_job(
        self,
        job_ats_record_id: str,
        matches: list[dict[str, Any]],
        run_timestamp: str | None = None,
    ) -> None:
        """Replace match records for a job in the Matches table.

        Deletes existing records where Job links to this ATS record, then creates
        new records. Requires matches_table_id to be set.

        Args:
            job_ats_record_id: ATS record ID (the job row)
            matches: List of dicts with candidate_airtable_id, score, pros, cons, rank
            run_timestamp: ISO 8601 timestamp for Date Created on each match record
        """
        if not self.matches_table_id:
            return
        url = f"https://api.airtable.com/v0/{self.base_id}/{self.matches_table_id}"
        headers = self._write_headers()

        with httpx.Client(timeout=30.0) as client:
            formula = f"FIND('{job_ats_record_id}', ARRAYJOIN({{Job}}))"
            offset: str | None = None
            to_delete: list[str] = []
            while True:
                params: list[tuple[str, str]] = [("filterByFormula", formula)]
                if offset:
                    params.append(("offset", offset))
                response = client.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                for rec in data.get("records", []):
                    to_delete.append(rec["id"])
                offset = data.get("offset")
                if not offset:
                    break

            for rec_id in to_delete:
                client.delete(f"{url}/{rec_id}", headers=headers).raise_for_status()

            if not matches:
                return

            def _join(v: Any) -> str:
                if v is None:
                    return ""
                if isinstance(v, list):
                    return "\n".join(str(x) for x in v) if v else ""
                return str(v)

            records = []
            for m in matches:
                fields: dict[str, Any] = {
                    "Name": m.get("name", ""),
                    "Job": [job_ats_record_id],
                    "Candidate": [m["candidate_airtable_id"]],
                    "Score": m.get("score"),
                    "Pros": _join(m.get("pros")),
                    "Cons": _join(m.get("cons")),
                    "Rank": m.get("rank"),
                    "Combined Score": m.get("combined_score"),
                    "Role Similarity": m.get("role_similarity"),
                    "Domain Similarity": m.get("domain_similarity"),
                    "Culture Similarity": m.get("culture_similarity"),
                    "Skills Fit": m.get("skills_fit"),
                    "Compensation Fit": m.get("compensation_fit"),
                    "Experience Fit": m.get("experience_fit"),
                    "Location Fit": m.get("location_fit"),
                    "Matching Skills": _join(m.get("matching_skills")),
                    "Missing Skills": _join(m.get("missing_skills")),
                }
                if run_timestamp:
                    fields["Date Created"] = run_timestamp
                fields = {k: v for k, v in fields.items() if v is not None}
                records.append({"fields": fields})
            for i in range(0, len(records), 10):
                batch = records[i : i + 10]
                client.post(url, headers=headers, json={"records": batch}).raise_for_status()
