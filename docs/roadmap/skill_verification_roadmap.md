# Skill Verification Roadmap

Future improvements and recommended steps for the GitHub-based skill verification pipeline.

## Current Implementation

- **Asset 1** (`candidate_github_commit_history`): Blobless git clone extracts full commit history per repo.
- **Asset 2** (`candidate_skill_verification`): Uses commit history + GitHub API (manifests, README, languages) + single LLM call to verify each skill.
- Per-skill status: `verified`, `unverified`, `no_evidence`, `skipped`.
- Aggregate score: `verified_count / total_verifiable` (0–1) stored in `normalized_candidates.skill_verification_score`.

---

## Recommended Future Improvements

### 1. Vector Embeddings for Semantic Search

**Current**: Commits are sampled by message length and fed to the LLM.

**Improvement**: Embed commit messages and use semantic search to surface the most relevant commits for each skill before the LLM sees them.

- Embed all commit messages (or a large subset) using the same embedding model as `candidate_vectors`.
- For each skill + evidence, embed the evidence text.
- Retrieve top-K commits by cosine similarity to the skill evidence.
- Feed only those relevant commits to the verification prompt.

**Benefits**: Reduces token usage, improves signal-to-noise, and leverages existing pgvector infrastructure.

### 2. Richer Retrieval Sources

- Embed repo READMEs, file paths, or code snippets for retrieval.
- Use file path patterns (e.g. `*.py`, `src/**/*.ts`) to prioritize relevant code.
- Combine commit evidence with README and manifest evidence in a weighted score.

### 3. Per-Skill Confidence Weighting

- Weight the aggregate score by per-skill confidence (LLM output).
- Allow configurable thresholds (e.g. only count skills with confidence ≥ 0.7 as verified).

### 4. Caching and Incremental Updates

- Cache commit history per repo; only re-clone when repo has new commits (compare `HEAD` sha).
- Incremental verification: re-run LLM only for candidates whose commit history or skills changed.

### 5. Private Repos (If Ever Supported)

- GitHub API with PAT can list private repos; git clone would require SSH key or token.
- Document security considerations and access control if private repos are added.

---

## Implementation Order

1. **Vector embeddings** — Highest impact; reuses existing embedding infra.
2. **Caching** — Reduces clone time and API load for large backfills.
3. **Confidence weighting** — Quick change to aggregate score logic.
4. **Richer retrieval** — Extends the embedding approach.
