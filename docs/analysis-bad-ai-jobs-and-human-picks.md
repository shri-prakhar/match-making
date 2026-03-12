# Analysis: Bad AI Jobs and Human-Selected Candidates (Remote Run)

**Run:** 2026-03-10 · **Scope:** Non-technical jobs with shortlist avg LLM score ≤ 4
**Reports:** `bad_ai_jobs_report.json` (1 job, all), `bad_ai_jobs_report_nontech.json` (9 jobs, non-technical only)

## Summary

- **9 non-technical jobs** had shortlist average LLM fit score ≤ 4.0 (1.4–4.0).
- Across these jobs, **93 human-selected candidates** were **not** in the system shortlist (0 human picks appeared in the AI shortlist for these jobs).
- Exclusion reasons were diagnosed for each missing candidate via the same pipeline logic as `score_candidate_against_job`.

## Exclusion reasons (aggregate)


| Reason                 | Count | % of 93 |
| ---------------------- | ----- | ------- |
| **job_category**       | 76    | **82%** |
| **location_prefilter** | 11    | 12%     |
| **not_in_db**          | 6     | 6%      |
| skill_threshold        | 0     | 0%      |
| rank_beyond_30         | 0     | 0%      |


## Findings

### 1. Job category filter dominates

**82%** of missing human picks failed the **job_category** filter: the job’s `job_category` was not in the candidate’s `desired_job_categories`, so they were never scored by the matches asset.

- Pipeline behaviour: in `[matches](talent_matching/assets/jobs.py)`, candidates are filtered to those with `job_category in desired_job_categories` before any scoring.
- Implication: recruiters are placing people into roles (e.g. Compliance, Growth, Business Development, Operations, “Designer, Product Designer”) whose **normalized job category** doesn’t match the **desired_job_categories** we have on the candidate profile. This can be due to:
  - Taxonomy mismatch (job category vs candidate categories use different labels).
  - Candidates not having that category in their desired list even though they were manually selected for the role.
  - Multi-value or composite job categories (e.g. “Growth, Marketing, Content Writer”) not matching candidate desires.

### 2. Location prefilter (12%)

11 candidates were dropped by the **location prefilter** (job Preferred Location / timezone). They never entered the scoring pool. This is expected when job location requirements are strict and candidates are in other regions/timezones.

### 3. Not in DB (6%)

6 candidates were not in `normalized_candidates` (e.g. not yet ingested or from a different source). They cannot be scored until they exist in the DB.

### 4. No skill_threshold or rank_beyond_30

No missing human pick failed only on **skill_fit_score < 0.30** or on **rank_beyond_30**. So for these non-technical, bad-AI jobs, the main blocker is **eligibility** (category + location + presence in DB), not scoring or rank.

## Jobs covered (non-technical, bad AI)


| Job                                       | Company    | job_category                      | Avg LLM | Human picks | Missing                                      |
| ----------------------------------------- | ---------- | --------------------------------- | ------- | ----------- | -------------------------------------------- |
| Money Laundering Reporting Officer (MLRO) | Altitude   | Compliance                        | 1.4     | 17          | 17 (all job_category)                        |
| Lead, Validator & Staking Growth (Solana) | Raiku      | Growth                            | 1.73    | 10          | 10 (4 location, 6 job_category)              |
| Business Development Director             | BuidlPad   | Business Development              | 1.8     | 8           | 8 (2 location, 6 job_category)               |
| Product Marketing Manager                 | Squads     | Growth, Marketing, Content Writer | 2.0     | 5           | 5 (2 not_in_db, 2 location, 1 job_category)  |
| Growth Analyst                            | Radarblock | Growth                            | 2.0     | 13          | 13 (4 not_in_db, 9 job_category)             |
| Compliance Operations Lead                | Altitude   | Compliance                        | 2.08    | 8           | 8 (1 location, 7 job_category)               |
| Business Development Lead                 | Radarblock | Business Development              | 2.13    | 16          | 16 (3 not_in_db, 13 job_category)            |
| Operations Manager                        | VeryAI     | Operations                        | 2.5     | 11          | 11 (2 not_in_db, 1 location, 8 job_category) |
| Product Designer                          | Buidlpad   | Designer, Product Designer        | 4.0     | 4           | 4 (all job_category)                         |


## Recommendations

1. **Job category ↔ desired_job_categories alignment**
  - Review how `job_category` is set (e.g. from ATS/Notion) and how `desired_job_categories` are set on candidates.
  - Consider normalizing or mapping categories (e.g. “Product Designer” ↔ “Designer, Product Designer”) so that recruiter-selected roles are eligible when the intent matches.
  - Consider relaxing or refining the category filter for non-technical roles (e.g. allow multiple tokens or semantic match) if taxonomy gaps are confirmed.
2. **Location**
  - Keep current behaviour; optionally log or report when strong human picks are excluded by location so recruiters can adjust job requirements or candidate pools.
3. **Not in DB**
  - Ensure all candidates that can be shortlisted in Airtable are ingested into `normalized_candidates` (and that pipelines run for the relevant partitions).
4. **Re-run and extend**
  - Re-run with `scripts/find_bad_ai_jobs_and_diagnose_human_picks.py` (e.g. without `--non-technical`, or with `--max-llm 5`) to include more jobs and compare.
  - Use `--output report.json` for further analysis or dashboards.
