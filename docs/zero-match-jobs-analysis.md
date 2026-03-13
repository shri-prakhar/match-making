# Zero-match jobs: proposed vs introduced candidates — full report

**Scope:** 30 ATS jobs (Job Status: Matchmaking Ready, Matchmaking Done, Ongoing Recruiting, Client Introduction, In Interview).
**Source:** `poetry run with-remote-db python scripts/diagnose_zero_match_jobs.py --verbose-location --output /tmp/zero_match_analysis.md`
**Script:** `scripts/diagnose_zero_match_jobs.py`

---

## Executive summary

- **17 of 30** jobs in scope have **zero matches** (no candidate proposed by the pipeline). **14** of those have normalized job data in the DB; **12** of the 14 have at least one human-selected candidate in CLIENT INTRODUCTION and/or Potential Talent Fit.
- **118** such human-selected candidates were diagnosed. **105** were excluded by **job category** (desired_job_categories vs job match set); **6** by **location**; **5** passed both filters but the job still had 0 matches (**possible pipeline bug**); **2** are **not in normalized_candidates** (data/sync).
- **Location expansion** is working as designed; no incorrect exclusions were found.
- **Recommendations:** (1) Extend match_category_aliases for zero-match roles (Rust Developer, Compliance/MLRO, Investment Research, etc.). (2) Re-run matchmaking for the three job partitions with the 5 “passed filters but zero matches” candidates and debug if they still don’t appear. (3) Resolve the 2 missing normalized_candidates. (4) Optionally normalize city-only job locations to country.

---

## 1. Overview

| Metric | Count |
|--------|--------|
| Total partitions (jobs) | 30 |
| **Zero-match jobs** | **17** |
| Zero-match with normalized job in DB | 14 |
| Zero-match with *no* normalized job | 3 (`recO5aPsJjNUUw66z`, `recfnHXuPdqQO01ha`, `recoEEVtVQllgm9Lq`) |
| Jobs with ≥1 match | 13 |

Of the 14 zero-match jobs that have normalized job data, **12** have at least one human-selected candidate in **CLIENT INTRODUCTION** and/or **Potential Talent Fit**. The diagnosis script analyzed **118** such candidates across those jobs.

---

## 2. Diagnosis summary (why candidates were not found)

| Reason | Count | Interpretation |
|--------|--------|----------------|
| **filtered_by_job_category** | **105** | Candidate’s `desired_job_categories` (canonical) has no overlap with the job’s match set (job_category + match_category_aliases). Pipeline correctly excluded them. |
| **filtered_by_location** | 6 | Candidate failed strict, country, and region steps. Location expansion is working as coded (no bug). |
| **passed_filters_but_zero_matches_bug** | 5 | Candidate passed both location and job-category filters but the job still had 0 matches → **pipeline bug or ordering/timing**: pool was empty for the job even though these candidates would qualify. |
| **not_in_normalized_candidates** | 2 | Candidate Airtable ID not in `normalized_candidates` (e.g. not synced, or wrong table). |

**Main finding:** The vast majority of “introduced but not proposed” cases are **job category mismatch** (105/118). A small number are location (6), a few point to a possible bug (5), and 2 are data/sync (not in normalized_candidates).

---

## 3. Zero-match jobs and CLIENT INTRODUCTION (detailed)

Jobs below are the 14 with normalized job data and 0 matches. For each we list CLIENT INTRODUCTION count and, where relevant, a short note.

| Partition ID | Job title | CLIENT INTRODUCTION | Potential Talent Fit | Notes |
|--------------|-----------|---------------------|----------------------|-------|
| rec1IwyAeeHUQitsw | Investment Research Director | 3 | 5 | All 8 diagnosed: 4 job category, 2 location (e.g. Australia, China vs US job). |
| rec2bjCVT0rRh0Bia | Operations Manager | 1 | 11 | 1 CLIENT INTRO: Jack Geller → job category. 2 passed filters but pool 0 (Zach Hake, Jake Feigs). 1 location (Serbia vs North America). |
| rec5yo8rhzVORjBDi | Money Laundering Reporting Officer (MLRO) | 4 | 14 | All 18 diagnosed: job category. |
| recBlPfTCOtkBu9Lf | Senior Frontend Software Engineer | 2 | 8 | All 10: job category. |
| recMnQ6CpvIYDVeor | Head of Institutional Sales | 2 | 3 | All 5: job category. |
| reccOvf1OIL3pb4J5 | Account Manager | 0 | 8 | 1 passed filters but pool 0 (Elena Ljubojevic). Rest: job category. |
| rece7iAGuBTfqpoJc | Product Marketing Manager | 1 | 4 | All 5: job category. |
| recjDlNrpJfRRg7b4 | Rust Developer | 7 | 22 | 2 passed filters but pool 0 (Nakshatra Nahar, Bhargav Veepuri). 2 location (Argentina, Brazil vs Europe/Middle East). Rest: job category. |
| recnuhHToY0I7s8wy | Designer | 1 | 3 | 1 CLIENT INTRO: not in normalized_candidates. Rest: job category. |
| reco3bIf05Elrddvo | Unknown | 2 | 0 | Both CLIENT INTRO: job category. |
| recpwjff4QeibeGha | Compliance Operations Lead | 1 | 7 | All 8: job category (incl. Mathieu Ladier in CLIENT INTRO). |
| recuO1geKmdxLtY5g | Senior Backend Software Engineer | 0 | 1 | 1: job category. |
| recumPHbWDgLHf6jX | Staff Backend Engineer | 0 | 7 | 1 not in normalized_candidates. Rest: job category. |
| recyYGJnWi9BNTKg5 | Head of Trading Operations | 1 | 2 | 1 location (US candidate vs Europe job). 2 job category. |

---

## 4. Location filter and expansion

- **Location expansion is behaving as designed.** No case was found where a candidate should have passed under expansion but was excluded.
- The 6 **filtered_by_location** cases are all “failed all (strict, country, region)” with consistent details:
  - Job e.g. United States / North America → candidate in Australia, China/Hong Kong, Serbia, Argentina, Brazil, or (for a Europe job) US. So correct exclusions.
- One nuance: for **Operations Manager** (rec2bjCVT0rRh0Bia), job locations include city names (e.g. San Francisco, Miami, New York). The report shows `allowed_countries` includes `'miami'` as a literal (city not resolved to USA). That does not change the outcome for the diagnosed candidate (Serbia), but it’s worth normalizing city-only tokens to country where possible so expansion is consistent.

---

## 5. “Passed filters but zero matches” (possible pipeline bug)

**5 candidates** passed both location and job-category checks yet their jobs had 0 matches:

1. **rec2bjCVT0rRh0Bia (Operations Manager):** Zach Hake, Jake Feigs
2. **reccOvf1OIL3pb4J5 (Account Manager):** Elena Ljubojevic
3. **recjDlNrpJfRRg7b4 (Rust Developer):** Nakshatra Nahar, Bhargav Veepuri

So for these jobs the **location_prefiltered_candidates** pool was either empty or no candidate in the pool passed the **job category** filter in the matches asset — yet our offline diagnosis says these candidates *do* pass job category (and location). That suggests one of:

- **Order of operations:** Location prefilter runs on the full candidate set; if the job’s partition sees a different or empty slice of candidates (e.g. partition mapping / asset inputs), the pool could be empty even though these candidates exist in the DB.
- **Timing / materialization:** Matches were computed at a time when these candidates weren’t in normalized_candidates or location_prefiltered_candidates yet.
- **Bug in matches asset:** e.g. job_category or match_categories resolved differently at run time (e.g. empty match set) so that no one passed the category filter even though our script sees a non-empty match set now.

**Recommendation:** For these 5 candidates, re-run the matchmaking pipeline for their job partitions and confirm whether they appear in location_prefiltered_candidates and then in matches. If they pass in a fresh run, the cause was likely timing/materialization; if not, debug the matches asset (category resolution and filter logic) for those partitions.

---

## 6. Job category as main driver

- **105 of 118** human-selected candidates were excluded by **job category**: their canonical `desired_job_categories` do not intersect the job’s match set (normalized job_category + match_category_aliases, restricted to canonical).
- That implies either:
  - **Taxonomy/alignment:** Job categories (and aliases) in scoring_weights don’t align well with how candidates describe their desired roles (e.g. “Rust Developer” vs “Backend” / “Software Engineering”), or
  - **Recruiter judgment:** Recruiters introduced candidates who don’t have that job’s category in their profile (acceptable for human override; pipeline is category-strict by design).

To improve proposal rate for roles like Rust Developer, Compliance, MLRO, Investment Research Director, consider:

- Adding or tuning **match_category_aliases** (e.g. Compliance → Operations, Legal; Rust Developer → Backend, Software Engineering) so that candidates with related desired categories are included.
- Checking that **normalized_jobs.job_category** and **normalized_candidates.desired_job_categories** use the same canonical list and that resolution (e.g. from raw Airtable or LLM output) is consistent.

---

## 7. Data quality

- **2 candidates** are in CLIENT INTRODUCTION / Potential Talent Fit but **not in normalized_candidates**:
  - Designer (recnuhHToY0I7s8wy): recDlDQ9iPdfF30v3
  - Staff Backend Engineer (recumPHbWDgLHf6jX): rec2kMgAkEVGxhIFP

So either they were never synced/normalized, or the link points at the wrong base/table. Worth checking sync and that linked IDs are Talent record IDs that exist in normalized_candidates.

---

## 8. Recommendations (concise)

1. **Job category (primary):** Review and extend **match_category_aliases** (and canonical list) for zero-match roles (Rust Developer, Compliance/MLRO, Investment Research, Designer, etc.) so that plausible desired categories (e.g. Backend, Operations, Legal) are in the match set.
2. **Pipeline bug (5 cases):** Re-run matchmaking for the three job partitions above and inspect location_prefiltered_candidates and matches for the 5 candidates; if they still don’t appear, debug category resolution and filter order in the matches asset.
3. **Location:** Keep current expansion logic; optionally normalize city-only job locations (e.g. Miami → USA) so allowed_countries doesn’t contain literal city names.
4. **Data:** Resolve the 2 “not in normalized_candidates” Airtable IDs (sync or correct links).

---

## Appendix A: Full partition list (all 30 jobs, match counts)

| Partition ID | Match count | Job title |
|--------------|-------------|-----------|
| rec1IwyAeeHUQitsw | 0 | Investment Research Director |
| rec2bjCVT0rRh0Bia | 0 | Operations Manager |
| rec5yo8rhzVORjBDi | 0 | Money Laundering Reporting Officer (MLRO) |
| recBlPfTCOtkBu9Lf | 0 | Senior Frontend Software Engineer |
| recIqBsuF33YrIrMX | 13 | Growth Analyst |
| recIsIJI3FbVQeZNx | 7 | Business Development Manager |
| recMnQ6CpvIYDVeor | 0 | Head of Institutional Sales |
| recMwAYTkEzTfA6dv | 2 | Product Manager |
| recO5aPsJjNUUw66z | 0 | (no normalized job) |
| recOsFRwmXD43TNbT | 5 | Senior System Engineer |
| recRXgpVWtXgf7T1M | 1 | Senior Frontend Engineer |
| recVQdnSkNXphFr4c | 2 | Sales Executive |
| recZkRwKZCl0mDIJH | 3 | Backend Engineer |
| reccOvf1OIL3pb4J5 | 0 | Account Manager |
| recch29d7MaREcrvB | 3 | Business Development Lead |
| rece7iAGuBTfqpoJc | 0 | Product Marketing Manager |
| recfZWUHZX43pVhTX | 3 | Senior Frontend Engineer |
| recfZxvhLlMnRJ2oH | 7 | Senior Frontend Engineer |
| recfnHXuPdqQO01ha | 0 | (no normalized job) |
| recjDlNrpJfRRg7b4 | 0 | Rust Developer |
| recnuhHToY0I7s8wy | 0 | Designer |
| reco3bIf05Elrddvo | 0 | Unknown |
| recoEEVtVQllgm9Lq | 0 | (no normalized job) |
| recpwjff4QeibeGha | 0 | Compliance Operations Lead |
| recsi4mTmg3Cfhzee | 1 | Lead, Validator & Staking Growth (Solana) |
| recuO1geKmdxLtY5g | 0 | Senior Backend Software Engineer |
| recumPHbWDgLHf6jX | 0 | Staff Backend Engineer |
| recyYGJnWi9BNTKg5 | 0 | Head of Trading Operations |
| recz8Zm0e3JVixdWW | 3 | Unknown |
| reczqy86gsYH3AuEA | 3 | Social Media Manager |

---

## Appendix B: All 118 diagnoses (by job, then candidate)

Each row: Job partition ID and title → Candidate Airtable ID and name → Column(s) (CLIENT INTRODUCTION and/or Potential Talent Fit) → Reason.

**Investment Research Director (rec1IwyAeeHUQitsw)**
- recC6AbcBF5zGgj4g Arisa Chelsea Ueno — CLIENT INTRODUCTION — filtered_by_job_category
- recTNDA8eAhsG502p Jeremy Osborne — Potential Talent Fit — filtered_by_job_category
- recVTx2O6LE8VT84O William Croisettier — CLIENT INTRODUCTION — filtered_by_job_category
- recVXyDAY0AQd1zH9 Chris Orza — Potential Talent Fit — filtered_by_job_category
- rece6zuwdY9ksDkQb Gua — Potential Talent Fit — filtered_by_location (Australia vs US job)
- recp2AwF5i98u3VDs Karan Rajpal — Potential Talent Fit — filtered_by_job_category
- recxpnKrbAqIds04f John Goldschmidt — CLIENT INTRODUCTION — filtered_by_job_category
- recydndAoCWHKZ0GJ Sima Zhang — Potential Talent Fit — filtered_by_location (China/Hong Kong vs US job)

**Operations Manager (rec2bjCVT0rRh0Bia)**
- rec2D8vtJrI175Kyk PHILIP SAIDELY — Potential Talent Fit — filtered_by_job_category
- recGSQbCZeMa4wlrU Zach Hake — Potential Talent Fit — **passed_filters_but_zero_matches_bug**
- recQFgbqOCaRVba5X Maria Arriola — Potential Talent Fit — filtered_by_job_category
- recSWHSbywrQ7Vbd8 Haley Cromer — Potential Talent Fit — filtered_by_job_category
- recZAlzO7XPDZGG8g Jake Feigs — Potential Talent Fit — **passed_filters_but_zero_matches_bug**
- recgsXW7cK6qO3tGh Eric Godwin — Potential Talent Fit — filtered_by_job_category
- reck0MDoX9TT5PqzH Kyla Ollinger — Potential Talent Fit — filtered_by_job_category
- recr34hpMOsQGh92b Aleksandra Pantic — Potential Talent Fit — filtered_by_location (Serbia vs North America job)
- recsBhehHwrubrquU Maria Arriola — Potential Talent Fit — filtered_by_job_category
- recurgKYYuYU5VLEq Tate Hutchinson — Potential Talent Fit — filtered_by_job_category
- recziZgX3T9SOsyG0 Jack Geller — CLIENT INTRODUCTION, Potential Talent Fit — filtered_by_job_category

**Money Laundering Reporting Officer (MLRO) (rec5yo8rhzVORjBDi)**
- All 18 candidates (4 CLIENT INTRODUCTION, 14 Potential Talent Fit) — filtered_by_job_category.
- CLIENT INTRO: recPGl5E5clSLPDbV Maxim Kon, recADqdlbG8iLM5OR Dimitrij Gede, recEpgQ3rptaktgN0 Christopher Guerra, reczJzt6P3wyAhFzK Beata Wiśnicka-Zawierucha.

**Senior Frontend Software Engineer (recBlPfTCOtkBu9Lf)**
- All 10 candidates (2 CLIENT INTRODUCTION, 8 Potential Talent Fit) — filtered_by_job_category.
- CLIENT INTRO: recxAni63LM9tTSg6 douvy, recnCoCnaRBsyE7GH Chris Jastrzebski.

**Head of Institutional Sales (recMnQ6CpvIYDVeor)**
- All 5 (2 CLIENT INTRODUCTION, 3 Potential Talent Fit) — filtered_by_job_category.
- CLIENT INTRO: recb5hGwDe93uctsD Artem Gordadze, recwTxvODUaiPET1Q Ro Chauhan.

**Account Manager (reccOvf1OIL3pb4J5)**
- recZ43NvMGDykgzap Elena Ljubojevic — Potential Talent Fit — **passed_filters_but_zero_matches_bug**
- All other 7 — filtered_by_job_category.

**Product Marketing Manager (rece7iAGuBTfqpoJc)**
- All 5 (1 CLIENT INTRODUCTION, 4 Potential Talent Fit) — filtered_by_job_category.
- CLIENT INTRO: recKTeV1o6c0k2dgP Marcus Chan.

**Rust Developer (recjDlNrpJfRRg7b4)**
- recjg1Qo0ZjMvA1VP Nakshatra Nahar — CLIENT INTRODUCTION — **passed_filters_but_zero_matches_bug**
- recw5cmSszc2YY9kk Bhargav Veepuri — Potential Talent Fit — **passed_filters_but_zero_matches_bug**
- receJlmaR1bFVFyEH Facundo La Rocca — Potential Talent Fit — filtered_by_location (Argentina vs Europe/Middle East job)
- recfZDxBIprjpuCj3 Vitor Kretiska Medeiros — Potential Talent Fit — filtered_by_location (Brazil vs Europe/Middle East job)
- All other 25 (6 CLIENT INTRODUCTION, 19 Potential Talent Fit) — filtered_by_job_category.

**Designer (recnuhHToY0I7s8wy)**
- recDlDQ9iPdfF30v3 recDlDQ9iPdfF30v3 — CLIENT INTRODUCTION — **not_in_normalized_candidates**
- All other 3 — filtered_by_job_category.

**Unknown (reco3bIf05Elrddvo)**
- recXvScy9jGxOFKT9 Maxim Fedin — CLIENT INTRODUCTION — filtered_by_job_category
- recnqgI7Q0v8vgh2u Elvis Sabanovic — CLIENT INTRODUCTION — filtered_by_job_category

**Compliance Operations Lead (recpwjff4QeibeGha)**
- All 8 (1 CLIENT INTRODUCTION, 7 Potential Talent Fit) — filtered_by_job_category.
- CLIENT INTRO: rec9aKO9CHiOQtJIB Mathieu Ladier.

**Senior Backend Software Engineer (recuO1geKmdxLtY5g)**
- recwKettIXuEgaJeA Taki Baker Alyasri — Potential Talent Fit — filtered_by_job_category

**Staff Backend Engineer (recumPHbWDgLHf6jX)**
- rec2kMgAkEVGxhIFP rec2kMgAkEVGxhIFP — Potential Talent Fit — **not_in_normalized_candidates**
- All other 6 — filtered_by_job_category.

**Head of Trading Operations (recyYGJnWi9BNTKg5)**
- rec811SMGg5FeqWKE Armin Keihani — Potential Talent Fit — filtered_by_location (US/Davis vs Europe job)
- recj9dDgwzOjiosMC Nils Engeln — CLIENT INTRODUCTION — filtered_by_job_category
- recl8s0ND4YTknl9Q Levi Jesus de Souza — Potential Talent Fit — filtered_by_job_category

---

## Appendix C: Location-filtered cases (6), full detail

| Job | Job locations | Candidate | Candidate location | Verdict |
|-----|----------------|-----------|---------------------|---------|
| Investment Research Director | United States | Gua | Australia (region + country) | Correct: not in US / North America. |
| Investment Research Director | United States | Sima Zhang | Asia, China, Hong Kong | Correct: not in US / North America. |
| Operations Manager | San Francisco, Miami, New York, North America, United States, Canada | Aleksandra Pantic | Europe, Serbia, Beograd Palilula | Correct: Serbia not in North America. |
| Rust Developer | Europe, Middle East | Facundo La Rocca | South America, Argentina, Buenos Aires | Correct: Argentina not in Europe/Middle East. |
| Rust Developer | Europe, Middle East | Vitor Kretiska Medeiros | South America, Brazil | Correct: Brazil not in Europe/Middle East. |
| Head of Trading Operations | Europe | Armin Keihani | North America, United States, Davis | Correct: US not in Europe. |

Location expansion (strict → country → region) was applied; in all 6 cases the candidate’s location is outside the job’s allowed countries/regions.

---

## Appendix D: How to reproduce this report

1. **Prerequisites:** `.env` with `AIRTABLE_BASE_ID`, `AIRTABLE_API_KEY`, `POSTGRES_*`. For remote DB, start tunnel (e.g. `poetry run remote-ui` or `poetry run local-matchmaking`).

2. **Run diagnosis (full 30 partitions, with location detail and report file):**
   ```bash
   poetry run with-remote-db python scripts/diagnose_zero_match_jobs.py --verbose-location --output /tmp/zero_match_analysis.md
   ```

3. **Run for specific partitions only:**
   ```bash
   poetry run with-remote-db python scripts/diagnose_zero_match_jobs.py --partitions recpwjff4QeibeGha,recjDlNrpJfRRg7b4 --output report.md
   ```

4. **On the server (local DB):**
   ```bash
   poetry run python scripts/diagnose_zero_match_jobs.py --local --output report.md
   ```

5. Raw script output (713 lines for this run) is written to the path given by `--output`. The summary section at the end gives counts per diagnosis reason.

---

---

## Appendix E: Remote DB — match_category_aliases check

**Question:** Do we already have mappings (match_category_aliases) on the remote DB that would help job category filtering?

**Result: No.** On the remote DB, **all 29 rows in scoring_weights have match_category_aliases = NULL**. No category has aliases set.

Zero-match jobs use these **normalized job_category** values (from normalized_jobs):

| job_category (in DB) | Example job title | match_category_aliases on remote |
|-----------------------|-------------------|-----------------------------------|
| Business Development | Investment Research Director, Head of Institutional Sales | NULL |
| Operations | Operations Manager, Head of Trading Operations | NULL |
| Compliance | MLRO, Compliance Operations Lead | NULL |
| Frontend Developer | Senior Frontend Software Engineer | NULL |
| Account Executive | Account Manager | NULL |
| Product Marketer | Product Marketing Manager | NULL |
| Backend Developer | Rust Developer, Senior/Staff Backend Engineer | NULL |
| Product Designer | Designer | NULL |

The codebase has a seed script `scripts/seed_match_category_aliases.py` that sets **Compliance → [Operations, Legal]**, but that script has **not** been run against the remote DB (or was run before a row for "Compliance" existed). To apply the existing mapping and improve matches for Compliance roles:

```bash
poetry run with-remote-db python scripts/seed_match_category_aliases.py
```

To add more mappings (e.g. Backend Developer → [Protocol Engineer], or other aliases), extend `SEED_ALIASES` in `scripts/seed_match_category_aliases.py` and run the script (use `--force` to overwrite existing aliases if needed).

---

*Full report. Raw diagnosis output (713 lines): `/tmp/zero_match_analysis.md`. Generated from `scripts/diagnose_zero_match_jobs.py`.*
