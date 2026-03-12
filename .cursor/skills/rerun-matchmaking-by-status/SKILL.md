---
name: rerun-matchmaking-by-status
description: Rerun matchmaking partitions for ATS jobs by Job Status. Fetches jobs with status Matchmaking Ready, Matchmaking Done, Ongoing Recruiting, Client Introduction, or In Interview and launches ats_matchmaking_pipeline backfill. Use when the user asks to rerun matchmaking for jobs by status, backfill matchmaking for active/recruiting jobs, or rerun matchmaking for specific ATS statuses.
---

# Rerun Matchmaking by Job Status

## When to use

Use this workflow when the user wants to rerun matchmaking (recompute matches and optionally re-upload to ATS) for all jobs that have a given set of **Job Status** values on the ATS table.

Default statuses: **Matchmaking Ready**, **Matchmaking Done**, **Ongoing Recruiting**, **Client Introduction**, **In Interview**.

## Quick steps

1. **Prerequisites**: `.env` has `REMOTE_HOST`, `AIRTABLE_BASE_ID`, `AIRTABLE_API_KEY`. Remote server has the project deployed.
2. **Run the script** (from project root):
   - Full rerun by status (no deploy/sync): `poetry run python scripts/launch_ats_matchmaking_backfill_by_status.py`
   - Sync partitions on remote first, then backfill: `poetry run python scripts/launch_ats_matchmaking_backfill_by_status.py --sync`
   - Backfill only specific partitions: `poetry run python scripts/launch_ats_matchmaking_backfill_by_status.py --partitions recA,recB,recC`
3. **Backfill safety**: If the backfill is large (>100 partitions), follow the workspace backfill-safety rule: test with ~100 partitions first, verify success, then run the rest.

## What the script does

- **Step 1 (only with `--sync`)**: Runs `sync_airtable_jobs_job` on the remote so all ATS record IDs exist as Dagster dynamic partitions; waits 60s for partitions to register. By default this step is skipped (no deploy/sync).
- **Step 2**: Fetches ATS record IDs where **Job Status** is one of the configured statuses (see `STATUSES` in `scripts/launch_ats_matchmaking_backfill_by_status.py`).
- **Step 3**: Submits a **remote** backfill for `ats_matchmaking_pipeline` for those partition IDs (full pipeline: normalize → vectors → matches → upload to ATS).

Backfills run on the **remote** Dagster instance; the script SSHs to `REMOTE_HOST` and runs the backfill there. To monitor: `poetry run remote-ui` then http://localhost:3000 → Backfills.

## Changing which statuses are included

Edit `STATUSES` in `scripts/launch_ats_matchmaking_backfill_by_status.py`. Values must match the ATS table’s **Job Status** single-select field exactly (e.g. "Matchmaking Ready", "In Interview").

## Reference

- Script: `scripts/launch_ats_matchmaking_backfill_by_status.py`
- Job: `ats_matchmaking_pipeline` (defined in `talent_matching/jobs/asset_jobs.py`)
- Workspace rules: `.cursor/rules/scripts-and-db-access.mdc`, `.cursor/rules/backfill-safety.mdc`
