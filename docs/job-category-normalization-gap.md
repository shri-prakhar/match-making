# Job category normalization: gap vs candidate taxonomy

## Summary

**Job normalization does not use the set of job categories that candidates have in their profiles.** The job’s `job_category` and candidates’ `desired_job_categories` come from separate, unaligned sources. The matchmaking pipeline then requires an **exact string match** between the two, so taxonomy mismatches (e.g. job “Compliance” vs candidate “Legal”) or different phrasing (e.g. “Designer, Product Designer” vs “Product Designer”) cause human-selected candidates to be excluded from the shortlist.

## Current behaviour

### Where job_category comes from

1. **Normalized jobs** ([`talent_matching/io_managers/postgres.py`](talent_matching/io_managers/postgres.py) ~1024):
   - `job_category = data.get("job_category") or raw_job.job_category_raw`
   - So: LLM normalization output (if the model returns `job_category`) **or** the raw Airtable value `job_category_raw` (recruiter-provided).

2. **LLM job normalization** ([`talent_matching/llm/operations/normalize_job.py`](talent_matching/llm/operations/normalize_job.py)):
   - The **output schema** in the system prompt does **not** include `job_category`. The prompt only says: if `job_category_raw` is provided, “Use this to infer job_category when the raw description is vague.”
   - So the LLM may or may not add `job_category` in its JSON; there is **no instruction** to pick from a fixed list or to align with candidate vocabulary.

3. **Raw jobs**: `job_category_raw` is whatever the recruiter (or Airtable) stores for that job — free text, not validated against any taxonomy.

### Where desired_job_categories comes from

- **Candidates** ([`talent_matching/io_managers/postgres.py`](talent_matching/io_managers/postgres.py) ~575–577):
  - `desired_job_categories = parse_comma_separated(raw_candidate.desired_job_categories_raw) or None`
  - So: Airtable “Desired Job Category” (free text) split into a list. No shared vocabulary with jobs.

### Matchmaking filter

- In [`talent_matching/assets/jobs.py`](talent_matching/assets/jobs.py) (matches asset, ~882–888):
  - Candidates are kept only if `job_category.lower() in desired_normalized`, where `desired_normalized` is the set of the candidate’s desired categories (lowercased).
  - **Exact string match** on the **full** `job_category` (e.g. “Designer, Product Designer” is one string; it does not match a candidate who has “Designer” and “Product Designer” as two separate categories).

## Consequence

- Job categories and candidate categories are **not** aligned to a single taxonomy.
- Recruiter/LLM wording (e.g. “Compliance”, “Growth, Marketing, Content Writer”) often does not match candidate wording (e.g. “Legal”, “Product Markter”, or “Designer” + “Product Designer”).
- Result: many human-selected candidates are excluded by the job_category filter even when the role intent matches (see [job-category-breakdown.md](job-category-breakdown.md) and [analysis-bad-ai-jobs-and-human-picks.md](analysis-bad-ai-jobs-and-human-picks.md)).

## Recommendation: align job_category with candidate taxonomy

1. **Define or derive the candidate taxonomy**
   - Option A: Maintain a controlled list of job categories (e.g. from a config or DB table) and use it for both jobs and candidates where possible.
   - Option B: Derive the “allowed” set from the union of all distinct `desired_job_categories` in `normalized_candidates` (and optionally normalize spelling, e.g. “Product Markter” → “Product Marketing”).

2. **Use that taxonomy in job normalization**
   - **Option 1 – LLM**: In the normalize_job prompt, pass the list of allowed job categories and instruct the model to output `job_category` as **exactly one** of those values (or the closest match). That way job_category is always comparable to candidate desired categories.
   - **Option 2 – Post-step mapping**: After normalization, map `job_category` (and/or `job_category_raw`) to the nearest taxonomy term (e.g. string similarity, or a small mapping table: “Compliance” → “Legal” for matching only, “Rust Developer” → “Backend Developer” if desired).

3. **Multi-value job categories**
   - Today one job has one `job_category` string (sometimes composite, e.g. “Designer, Product Designer”). The match logic could be changed to: **match if any token of the job’s category (split on comma) is in the candidate’s desired set**, so that “Designer, Product Designer” matches candidates who have “Designer” or “Product Designer”. Alternatively, normalize to a single primary category from the taxonomy and keep exact match.

Implementing (1) and (2) would ensure job normalization takes into account the available job categories from candidates and reduce unnecessary exclusions in matchmaking.
