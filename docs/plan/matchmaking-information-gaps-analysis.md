# Matchmaking Information Gaps Analysis

**Purpose:** Identify why some jobs receive wildly non-fitting candidates during matchmaking and LLM refinement.

---

## Executive Summary

Several failure modes can cause jobs to receive poor candidate matches:

1. **Empty or thin job description** → LLM sees insufficient context; must-haves may be missing
2. **Zero must-have skills** → LLM prompt shows "(None specified)"; scoring is overly permissive
3. **Missing recruiter guidance** (non_negotiables, location_raw, nice_to_have) → Hard filters not applied
4. **Empty job_category** → No desired-role filter; candidates who don't want this role type can appear
5. **Location filter bypass** → When location_raw is empty, all global candidates pass
6. **Job vectors missing or weak** → Vector similarity scores degrade; semantic matching suffers
7. **Column name mismatches** → Airtable fields not mapped; recruiter fields never reach the pipeline

---

## Data Flow Overview

```
airtable_jobs (or ATS sensor → RawJob)
    → raw_jobs (job_description, non_negotiables, nice_to_have, location_raw)
    → normalized_jobs (LLM: requirements, narratives, job_category)
    → job_vectors (embeddings from narratives)
    → location_prefiltered_candidates (filter by location_raw)
    → matches (vector + skill scoring, top 30)
    → llm_refined_shortlist (score each, select max 15)
```

---

## Failure Modes in Detail

### 1. Job Description Empty or Too Short

**Where it happens:**
- `raw_jobs` asset: job_description from Airtable (`job_description_text`, `job_description_link`) or existing RawJob
- When `job_description` < 50 chars, `normalized_jobs` **skips the LLM** and returns a thin payload

**Impact:**
- `_store_normalized_job` gets `requirements = {}` → **zero JobRequiredSkills**
- `_build_job_description_for_scoring` falls back to "synthesized" from narratives/requirements
- With no narratives (skipped LLM), synthesized description can be < 100 chars → **Failure raised**
- Or synthesized is thin but passes → LLM refinement sees minimal job context

**Root causes:**
- Airtable: `Job Description Text` empty and `Job Description Link` not a Notion URL (or Notion fetch fails)
- ATS sensor: `Job Description Text` empty and `Job Description Link` missing or Notion returns nothing
- Column name mismatch: ATS field names (e.g. "Job Description Text", "Job Description Link") must match what the mapper expects

**Relevant code:**
- `talent_matching/assets/jobs.py`: `raw_jobs` (lines 164–165), `normalized_jobs` (lines 330–344)
- `talent_matching/resources/airtable.py`: `AirtableATSResource.map_ats_record_to_raw_job`
- `talent_matching/sensors/ats_matchmaking_sensor.py`: ATS record mapping (lines 78–81)

---

### 2. Zero Must-Have Skills

**Where it happens:**
- `get_job_required_skills` returns `[]` when JobRequiredSkill table has no rows for the job
- JobRequiredSkills are populated from `normalize_job` LLM output: `must_have_skills`, `nice_to_have_skills`

**Impact:**
- `llm_refined_shortlist` passes `must_haves = []` to `score_candidate_job_fit`
- LLM prompt shows: `MUST-HAVE SKILLS (candidate must fulfill ALL of these): (None specified)`
- No hard skill filter → LLM can score non-technical candidates highly for technical roles
- `fulfills_all_must_haves` is trivially true when there are no must-haves

**Root causes:**
- `normalized_jobs` skipped LLM (job_description < 50 chars) → no requirements extracted
- LLM `normalize_job` prompt: "Do NOT infer skills from job title alone" → e.g. "Growth Analyst" with no explicit skills in text → empty must_have_skills
- Job description is vague or doesn't mention concrete technical skills

**Relevant code:**
- `talent_matching/llm/operations/normalize_job.py`: SYSTEM_PROMPT (lines 88–94)
- `talent_matching/io_managers/postgres.py`: `_store_normalized_job` (lines 1078–1141)
- `talent_matching/llm/operations/score_candidate_job_fit.py`: must_haves_text (lines 77–86)

---

### 3. Missing Recruiter Guidance (non_negotiables, location_raw, nice_to_have)

**Where it happens:**
- `raw_job.get("non_negotiables")`, `raw_job.get("location_raw")`, `raw_job.get("nice_to_have")`
- These come from Airtable columns or ATS sensor mapping

**Impact:**
- `score_candidate_job_fit` receives `non_negotiables=None`, `location_raw=None`, `nice_to_have=None`
- No "RECRUITER NON-NEGOTIABLES" or "REQUIRED LOCATION/REGION" in the LLM prompt
- LLM cannot enforce e.g. "Must be in Europe" or "5+ years Solidity" when recruiter specified them
- `normalize_job` also receives these for extraction; if missing, requirements may be under-specified

**Root causes:**
- Airtable column names don't match: "Non Negotiables" vs "Non-Negotiables", "Preferred Location " (trailing space) vs "Preferred Location"
- ATS sensor: `fields.get("Non Negotiables")` — exact match required
- Recruiter simply didn't fill them in on the ATS record

**Relevant code:**
- `talent_matching/resources/airtable.py`: `AirtableATSResource.map_ats_record_to_raw_job` (Non Negotiables, Nice-to-have, Preferred Location)
- `talent_matching/sensors/ats_matchmaking_sensor.py`: ATS record mapping (lines 61–62, 88–89)
- `talent_matching/assets/jobs.py`: llm_refined_shortlist (lines 1153–1155)

---

### 4. Empty job_category → No Desired-Role Filter

**Where it happens:**
- `matches` asset: `job_category = (job.get("job_category") or "").strip()`
- When `job_category` is set, candidates are filtered: `job_category.lower() in desired_normalized`
- When `job_category` is **empty**, the filter is **skipped** — all candidates pass

**Impact:**
- Candidates who want "Frontend Developer" can appear in matches for "Smart Contract Engineer"
- No alignment between job type and candidate's desired roles

**Root causes:**
- `normalize_job` LLM schema outputs `title` but not `job_category`
- `_store_normalized_job` uses `data.get("job_category")` — often None
- `job_category_raw` from raw_job (e.g. "Desired Job Category" from ATS) is not passed into normalized_job payload

**Relevant code:**
- `talent_matching/assets/jobs.py`: matches (lines 742–758)
- `talent_matching/io_managers/postgres.py`: `_store_normalized_job` (line 1018)

---

### 5. Location Filter Bypass

**Where it happens:**
- `location_prefiltered_candidates`: when `location_raw` is empty, `parse_job_preferred_locations` returns `None`
- When `None`, all eligible candidates pass (no location filter)

**Impact:**
- Job requires "Europe" but recruiter didn't set Preferred Location → global candidates included
- Candidates in wrong timezones/regions get high scores

**Root causes:**
- Same as §3: column mismatch or recruiter didn't fill location
- ATS: "Preferred Location " (with space) vs "Preferred Location" — sensor checks both

**Relevant code:**
- `talent_matching/assets/jobs.py`: `location_prefiltered_candidates` (lines 597–606)
- `talent_matching/matchmaking/location_filter.py`

---

### 6. Job Vectors Missing or Weak

**Where it happens:**
- `job_vectors` built from normalized_job narratives (role, domain, personality, etc.)
- When `normalized_jobs` skips LLM, narratives are `{}`
- `job_vectors` asset may produce empty or low-quality vectors

**Impact:**
- `matches` uses `job_role_vec`, `job_domain_vec`, `job_personality_vec`
- When missing: `role_sims = zeros`, `domain_sims = zeros`, `culture_sims = zeros`
- Vector component of score is zero → matching relies only on skills (which may also be empty)

**Relevant code:**
- `talent_matching/assets/jobs.py`: `job_vectors` asset, `matches` (lines 715–724, 778–790)

---

### 7. RawJob vs airtable_jobs (ATS Only)

**Current design:**
- All job operations use the ATS table only. `airtable_jobs` and `airtable_ats` both point to `AirtableATSResource` (ATS table).
- Partition sync and single-record fetch use the same ATS table; no source mismatch.
- `raw_jobs` still prefers existing RawJob when it has `job_description` (e.g. from ATS sensor ingestion before the run).

---

## Diagnostic Queries

Use `scripts/check_matchmaking_data.py` (see below) to identify jobs with:

- Short or empty job_description
- Zero must-have skills
- Missing non_negotiables, location_raw, nice_to_have
- Empty job_category
- Missing job vectors

---

## Recommendations

1. **Add job_category fallback:** When LLM doesn't return `job_category`, use `job_title` or `job_category_raw` from raw_job.
2. **Log recruiter fields at pipeline start:** In `raw_jobs` and `llm_refined_shortlist`, log when non_negotiables/location_raw/nice_to_have are empty.
3. **Warn on zero must-haves:** When `must_haves` is empty, log a warning so operators can fix the job description or add skills manually.
4. **Column name flexibility:** Support multiple Airtable column name variants (e.g. "Non Negotiables", "Non-Negotiables") in mapper and ATS sensor.
5. **Minimum job description quality gate:** Consider failing early when job_description < 100 chars and no Notion link, rather than producing thin normalized output.
