# Road to Recommendation Improvement

This document outlines how we improve match quality over time using the data we now collect and the tools we have.

---

## Foundation (What We Have Now)

### 1. Tracking & Observability

| Asset | Purpose |
|-------|---------|
| **Matchmaking Result** (ATS) | "No suitable candidates found" or "X candidates proposed" — immediate visibility when the LLM selects 0 |
| **Matchmaking Last Run** (ATS) | Timestamp of last matchmaking run per job |
| **Date Created** (Matches) | When each match record was created |

### 2. Version Tracking

Each match record now stores the code versions that produced it:

| Column | Source | Purpose |
|--------|--------|---------|
| Matchmaking Version | `notion_v3` (algorithm) | Scoring formula |
| CV Normalization Version | `5.1.0` | Candidate extraction prompt |
| Job Normalization Version | `3.5.0` | Job requirements extraction |
| Vectorization Version | `1.0.0` | Embedding model |

This enables **A/B analysis**: compare outcomes by version when we change prompts or weights.

### 3. Human vs System Analysis

**Script:** `scripts/analyze_human_vs_system.py <ats_record_id>`

Compares recruiter-selected candidates (CLIENT INTRODUCTION, Shortlisted Talent, Potential Talent Fit, Hired) against system rankings for the same job. Shows:
- System score/rank for each human pick
- Overlap between human and system top-N
- Why some human picks might not appear in system matches

**Usage:** Run after matchmaking for jobs where recruiters have made selections. Use to identify patterns (e.g. "humans prefer X, system underweights it").

---

## Near-Term Improvements (Next 1–2 Sprints)

### 1. Run Human vs System Analysis Regularly

- **Done:** `scripts/batch_analyze_human_vs_system.py` runs `analyze_human_vs_system` for all ATS jobs with human selections.
- **Output:** JSON summary with per-job stats (avg rank, overlap %, in-top-5/10/15) and aggregates. Use `--output summary.json` to save.
- **Goal:** Build a dataset of human vs system alignment over time. Run periodically (e.g. weekly) and compare outputs.

### 2. Weight Tuning Based on Human Data

- **Input:** Use `analyze_human_vs_system` or `batch_analyze_human_vs_system` output to identify which score components (role, domain, culture, skills, compensation, location) correlate with human selection.
- **Action:** If human picks consistently rank higher on e.g. domain similarity, adjust weights in `talent_matching/config/scoring.py` (ROLE_WEIGHT, DOMAIN_WEIGHT, CULTURE_WEIGHT, VECTOR_WEIGHT, SKILL_FIT_WEIGHT, etc.).
- **Process:** Change weights → deploy → run matchmaking → analyze new jobs → iterate.

### 3. Fallback for Zero LLM Selections

- **Done:** When LLM selects 0 candidates (none fulfill must-haves), we now upload top 15 by fit_score. Recruiters still see candidates with pros/cons.
- **Done:** When LLM selects 0 candidates, Matchmaking Result is set to "No must-have matches; showing best candidates" to distinguish from "X candidates proposed".

### 4. Matchmaking Last Run on ATS

- **Done:** We now set `Matchmaking Last Run` on every run (success or no candidates).
- **Use:** Filter/sort jobs by "last run" in Airtable to prioritize re-runs or identify stale jobs.

---

## Medium-Term Improvements (2–4 Sprints)

### 1. Rejection Feedback Loop

See **docs/plan/match-feedback-and-skill-adjustment.md** — Phase 3a: `ats_feedback_sensor` to track `Recruiter AI Result Rejection` and update `Match.status = REJECTED`.

- **Value:** Analyze which score components are highest on rejected matches vs hired. De-emphasize misleading signals.
- **Data:** `Match.status`, `reviewer_notes` from ATS `Feedback` field.

### 2. Hired Outcome Tracking

See **match-feedback-and-skill-adjustment.md** — Phase 3b: Detect `Hired` column changes and set `Match.status = HIRED`.

- **Value:** Precision metric: % of top-N candidates that get hired. Hit rate per algorithm version.
- **Data:** `Match.status = HIRED` for jobs where we track hires.

### 3. Calibration Dashboard

- **Input:** Match records with version columns + Match status (when feedback loop is live).
- **Output:** Per-version metrics: precision@k, hire rate, rejection rate, human overlap rate.
- **Tool:** Could be a simple script or notebook that queries Postgres + Airtable.

### 4. Skill Feedback

See **match-feedback-and-skill-adjustment.md** — Phase 3c: `(F) Skill Feedback` on Talent table for recruiter corrections.

- **Value:** Fix CV extraction errors (missed skills, wrong ratings) from interview findings.
- **Flow:** Recruiter writes feedback → sensor parses → updates CandidateSkill → re-vectorize → future matches use corrected profile.

---

## Long-Term Improvements (4+ Sprints)

### 1. Stage Progression Signals

Track intermediate stages (Shortlisted → Client Introduction → Interview).

- **Value:** Stronger signals than binary hired/rejected. Learn which stages predict final hire.
- **See:** match-feedback-and-skill-adjustment.md Phase 3d.

### 2. Rejection Reason Clustering

If `reviewer_notes` contain patterns (e.g. "not enough Solana", "salary too high"):

- Extract and cluster reasons.
- Use as additional filters or negative signals for future matching.

### 3. Company-Specific Tuning

- Learn per-company preferences (e.g. Company A prefers domain-heavy, Company B prefers strong skills).
- Store per-company weight overrides or filters.

### 4. Continuous Learning

- Periodically retrain or recalibrate weights using accumulated Match + status data.
- Use version columns to compare before/after each change.

---

## Quick Reference: Scripts & Commands

| Task | Command |
|------|---------|
| Launch matchmaking run | `poetry run python scripts/launch_matchmaking_via_graphql.py <partition_id>` |
| Analyze human vs system | `poetry run with-remote-db python scripts/analyze_human_vs_system.py <ats_record_id>` |
| Batch human vs system | `poetry run with-remote-db python scripts/batch_analyze_human_vs_system.py [--limit N] [--output summary.json]` |
| Inspect matches for job | `poetry run with-remote-db python scripts/inspect_matches.py <partition_id>` |
| Add Matchmaking Result column | `poetry run python scripts/add_ats_matchmaking_result_column.py` |
| Add version columns to Matches | `poetry run python scripts/add_matches_version_columns.py` |
| Deploy | `poetry run deploy` |

---

## Summary

We now have a solid foundation for improving recommendations:

1. **Tracking** — Matchmaking Result, Last Run, Date Created.
2. **Versioning** — Matchmaking, CV, Job, Vectorization versions on each match.
3. **Human comparison** — `analyze_human_vs_system.py` to compare recruiter picks vs system.
4. **No-data fallback** — Top 15 by fit_score when LLM selects 0.

Next steps: run human vs system analysis regularly, tune weights based on findings, and implement the rejection/hired feedback loop from match-feedback-and-skill-adjustment.md.
