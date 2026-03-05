# Matchmaking Test Report – Location Pre-Filter

## Deployment

- **Yes, we deployed to remote.** Ran `./scripts/run_remote_matchmaking.sh recIqBsuF33YrIrMX` which does `git pull` and `docker compose up --build -d` on the remote server.
- **Test ran on remote data.** `inspect_matches.py` was run with `POSTGRES_HOST=localhost POSTGRES_PORT=15432`, which uses the SSH tunnel to the remote Postgres.

## Run Status

| Run ID | Partition | Status | Timestamp (UTC) |
|--------|-----------|--------|-----------------|
| 63b6de02-... | recIqBsuF33YrIrMX | **FAILURE** | 2026-03-05 08:38 |
| ffda1a8a-... | recIqBsuF33YrIrMX | SUCCESS | 2026-03-03 22:48 |

The March 5 run **failed at `upload_matches_to_ats`** (Airtable 422 Unprocessable Entity). It completed through `llm_refined_shortlist`, which writes the matches to Postgres. So the matches we inspected are from the **March 5 08:38 run**.

## Timing: Was This a Fresh Run With Our Code?

- **Deploy:** ~14:31–14:38 UTC (from `run_remote_matchmaking.sh` output).
- **March 5 run:** 08:38 UTC.

The March 5 run is **~6 hours before** our deploy. So the matches we inspected are from a run that **did not include the location pre-filter**.

## Launch Script (Fixed)

```bash
poetry run remote-ui
poetry run python scripts/launch_matchmaking_run.py recIqBsuF33YrIrMX
```

Uses DagsterGraphQLClient with repository_name="__repository__". Run 6813228a was launched successfully.

## Results (From Pre-Deploy Run)

**Job:** Growth Analyst @ Radarblock
**Preferred Location:** Middle East, Europe, India

- **8/15 candidates pass** location filter: UK, Poland, Croatia, India (2), Portugal, Netherlands, Germany.
- **7/15 candidates fail** location filter: Canada (2), South Korea, Singapore, US (2), Colombia.

## Next Steps to Test the Location Filter

1. **Launch** (with `poetry run remote-ui` running): `poetry run python scripts/launch_matchmaking_run.py recIqBsuF33YrIrMX`
2. **Wait ~12–15 minutes** for the pipeline to finish (LLM refinement is slow).
3. **Inspect results:**
   ```bash
   set -a && source .env && set +a && export POSTGRES_HOST=localhost POSTGRES_PORT=15432
   poetry run python scripts/inspect_matches.py recIqBsuF33YrIrMX --verify-location
   ```

If the Airtable upload still fails with 422, matches will still be written to Postgres by `llm_refined_shortlist`, so inspection will work.
