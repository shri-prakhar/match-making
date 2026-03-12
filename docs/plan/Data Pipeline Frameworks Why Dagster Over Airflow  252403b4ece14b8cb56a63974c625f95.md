# Data Pipeline Frameworks: Why Dagster Over Airflow & Luigi

## The Big Picture

When building data pipelines — the automated workflows that move, transform, and deliver data — choosing the right framework matters. Three popular options are **Apache Airflow**, **Luigi**, and **Dagster**. While all three can orchestrate data workflows, they take fundamentally different approaches.

This document explains why **Dagster** is the strongest choice for the Talent Evaluation & Matchmaking system, given both our [MVP requirements](Short-Term%20Plan%20Talent%20Evaluation%20&%20Matchmaking%20MV%202ff1abafe832806bba3ad6a502ee2cca.md) and our [long-term platform vision](Long-Term%20Vision%20Talent%20Evaluation%20&%20Matchmaking%20P%202ff1abafe83280e690fdfbae6ce0dbba.md).

---

## At a Glance

|  | **Airflow** | **Luigi** | **Dagster** |
| --- | --- | --- | --- |
| **Core idea** | Schedule and run tasks | Build task dependency chains | Manage data assets end-to-end |
| **First released** | 2014 (Airbnb) | 2012 (Spotify) | 2019 (Elementl) |
| **Philosophy** | Task-centric | Task-centric | Asset-centric |
| **Testing & local dev** | Difficult | Basic | Built-in & easy |
| **Data quality checks** | Add-on | Manual | Native |
| **UI / observability** | Good (task-focused) | Minimal | Excellent (data-focused) |
| **Learning curve** | Moderate | Low | Moderate |

---

## The Core Difference: Tasks vs. Assets

<aside>
💡

**Think of it this way:** Airflow and Luigi focus on *"what steps should run and when"* — they're like a to-do list for your computer. Dagster focuses on *"what data should exist and is it correct"* — it's like a catalog of everything your data team produces.

</aside>

This distinction is critical. With task-centric tools, you define *procedures*. With Dagster's asset-centric approach, you define *outcomes* — the tables, reports, and datasets your team actually cares about.

---

## Why Dagster Fits Our System

### 1. Our pipeline is a chain of data assets — not just tasks

Our MVP pipeline has clear, persistent data outputs at every step:

`Raw CVs` → `Normalized Profiles` → `Scores` → `Vectors` → `Matches`

In Dagster, each of these is a **software-defined asset**. The framework automatically understands the dependency graph between them. If a scoring prompt changes, Dagster knows which downstream assets (vectors, matches) need to be recomputed — without you wiring that logic manually.

In Airflow or Luigi, these are just "tasks." The framework doesn't know or care *what data* they produce. You'd have to build your own tracking for intermediate results.

<aside>
🔗

**Our architecture decision** to persist all intermediate results (raw → normalized → scored) maps *directly* to Dagster's asset model. Each table in our storage pattern — `raw_candidates`, `normalized_candidates`, `candidate_scores`, `candidate_vectors` — becomes a first-class asset with lineage, metadata, and health tracking built in.

</aside>

---

### 2. Versioned transformations are native

A core requirement of our system is **versioned transformation definitions** — tracking which prompt version and model version produced each score, so we can audit and reprocess.

Dagster handles this natively through **asset metadata and configuration**. Every time a normalization prompt or scoring rubric changes, Dagster can:

- Track which version produced each result
- Selectively re-materialize only affected assets
- Compare outputs across versions

With Airflow, you'd need to build this versioning infrastructure yourself. Luigi doesn't have a concept of it at all.

---

### 3. Dual storage (metrics + vectors) maps to IO Managers

Our system uses **dual storage**: raw queryable metrics in PostgreSQL and semantic vectors in pgvector. The same normalized profile feeds into *both* storage paths.

Dagster's **IO Manager** abstraction is designed exactly for this. You define *what* data an asset produces, and the IO Manager handles *where and how* it gets stored. This means:

- One asset definition can write to both PostgreSQL and pgvector
- Swapping storage backends (e.g., moving to a dedicated vector DB later) is a config change, not a code rewrite
- Testing locally? Point the IO Manager at SQLite instead of Postgres — zero pipeline code changes

Airflow has no equivalent. You'd wire storage logic directly into your task code, making it harder to swap or test.

---

### 4. LLM cost management through smart re-processing

LLM API credits are our primary cost driver. Our cost estimates show ~$0.12 per candidate with GPT-4, and we explicitly want to **skip re-processing when prompts haven't changed**.

Dagster's asset materialization model supports this out of the box:

- Assets are only re-materialized when their inputs or code change
- **Partitioning** lets us process candidates in batches and only reprocess specific partitions
- Freshness policies can define "this data is acceptable if less than X hours old"

In Airflow, every scheduled DAG run re-executes every task by default. Building incremental processing requires significant custom logic.

**Realistic LLM Cost Estimates Across All Phases**

Based on the LLM operations defined in our [MVP](Short-Term%20Plan%20Talent%20Evaluation%20&%20Matchmaking%20MV%202ff1abafe832806bba3ad6a502ee2cca.md) and [Long-Term Vision](Long-Term%20Vision%20Talent%20Evaluation%20&%20Matchmaking%20P%202ff1abafe83280e690fdfbae6ce0dbba.md), here's what it actually costs per candidate when using frontier models. Prices reflect GPT-4o and Claude 3.5 Sonnet tier pricing.

| **Phase** | **LLM Operation** | **Model** | **Tokens (est.)** | **Cost / Candidate** |
| --- | --- | --- | --- | --- |
| **Phase 1** (MVP) | CV Normalization | GPT-4o | ~2,000 in / 500 out | ~$0.010 |
|  | CV Scoring (rubric-based) | GPT-4o | ~1,000 in / 300 out | ~$0.006 |
|  | Embeddings (3 vectors) | text-embedding-3-large | ~1,500 | ~$0.0002 |
|  | **Phase 1 subtotal** |  |  | **~$0.02** |
| **Phase 2** (Verification) | Oversell/Undersell Detection | Claude 3.5 Sonnet | ~3,000 in / 500 out | ~$0.017 |
|  | GitHub Cross-validation | Claude | ~1,500 in / 300 out | ~$0.007 |
|  | **Phase 2 subtotal** |  |  | **~$0.02** |
| **Phase 3** (Context) | Code Quality Review (sampled files) | Claude 3.5 Sonnet | ~8,000 in / 800 out | ~$0.036 |
|  | Role Normalization | GPT-4o | ~800 in / 200 out | ~$0.004 |
|  | Impact Scope Analysis | GPT-4o | ~1,000 in / 300 out | ~$0.006 |
|  | **Phase 3 subtotal** |  |  | **~$0.05** |
| **Phase 4** (Intelligence) | Career Trajectory Prediction | GPT-4o | ~2,000 in / 500 out | ~$0.010 |
|  | Red Flag Detection | GPT-4o | ~2,000 in / 300 out | ~$0.008 |
|  | Culture/Team Fit Signals | GPT-4o | ~1,500 in / 300 out | ~$0.007 |
|  | Re-embedding (updated profile) | text-embedding-3-large | ~2,000 | ~$0.0003 |
|  | **Phase 4 subtotal** |  |  | **~$0.03** |

**Cumulative Cost Per Candidate & At Scale (5,000 candidates)**

| **Through Phase** | **LLM Calls Per Candidate** | **Cumulative / Candidate** | **5,000 Candidates** |
| --- | --- | --- | --- |
| **Phase 1** — Normalize, score, embed | 3 | ~$0.02 | ~$100 |
| **Through Phase 2** — + verification, cross-validation | 5 | ~$0.04 | ~$200 |
| **Through Phase 3** — + code review, role context | 8 | ~$0.09 | ~$450 |
| **Through Phase 4** — + predictions, re-embedding | 12 | ~$0.12 | ~$600 |

<aside>
💰

**Key takeaway:** With current frontier models (GPT-4o, Claude 3.5 Sonnet), the full Phase 4 pipeline costs roughly what Phase 1 alone used to cost with legacy GPT-4. The biggest cost jump comes in Phase 3 — code quality review sends ~8K tokens of source code per candidate through a frontier model. Job-side costs add ~$0.01 per job listing (normalization + embeddings), which is negligible.

</aside>

<aside>
⚠️

**Note on model choice:** Claude 3.5 Sonnet is used for code review and oversell detection because these tasks require careful reasoning over long context. GPT-4o handles structured extraction (normalization, scoring) where speed and structured output matter more. Embeddings use OpenAI's text-embedding-3-large at $0.13/1M tokens — nearly free. These model assignments are swappable in Dagster via the Resource abstraction described in section 5.

</aside>

---

### 5. Resources make LLM provider swaps trivial

Our tech choices note: *"Choose LLM provider based on quality vs cost tradeoffs"* — using GPT-3.5 for normalization and GPT-4 for scoring, with the option to switch to Anthropic or open-source models.

Dagster's **Resource** system is built for this. You define an `LLMResource` interface, then provide different implementations:

| Environment | LLM Resource | Database Resource | Embedding Resource |
| --- | --- | --- | --- |
| **Local dev** | Mock LLM (cached responses) | SQLite | Local sentence-transformers |
| **Staging** | GPT-3.5 (cheap) | Dev PostgreSQL | OpenAI embeddings |
| **Production** | GPT-4 / Claude | Managed PostgreSQL + pgvector | OpenAI embeddings |

The pipeline code stays identical across all environments. With Airflow, environment switching requires managing Connections and Variables — a more fragile and manual process. Luigi has no built-in concept for this.

---

### 6. The long-term vision requires a framework that grows

Our roadmap spans 4 phases over 12–24 months:

| **Phase** | **What's added** | **How Dagster handles it** |
| --- | --- | --- |
| **Phase 1** (MVP) | Basic pipelines, LLM scoring, dual storage | Define core assets, IO Managers for dual storage |
| **Phase 2** (Verification) | GitHub scraping, cross-validation, oversell detection | Add new assets to the graph — existing assets untouched |
| **Phase 3** (Context) | Company tier scoring, role normalization, LLM code quality | New assets depend on Phase 2 outputs — lineage extends naturally |
| **Phase 4** (Intelligence) | Recommender system, feedback loops, predictive matching | Sensors trigger retraining when new hire outcomes arrive |

<aside>
📈

**Key insight:** Dagster's asset graph *grows incrementally*. Adding Phase 2 verification assets doesn't require refactoring Phase 1. The dependency graph just extends. With Airflow, adding cross-cutting concerns (like verification that touches multiple existing DAGs) often requires painful DAG restructuring.

</aside>

---

### 7. Observability for non-engineers

Our system includes human-in-the-loop verification, recruiter reviews, and stakeholder oversight. Dagster's UI is **data-focused**, meaning non-engineers can:

- See whether candidate profiles are up to date
- Check if the matching pipeline ran successfully
- View data quality metrics without reading logs

Airflow's UI is task-focused — it shows whether *tasks* ran, not whether the *data* is correct. This is a poor fit for a system where recruiters and hiring managers need visibility.

---

## Where Airflow and Luigi Fall Short for This Project

| **Our Requirement** | **Airflow** | **Luigi** | **Dagster** |
| --- | --- | --- | --- |
| Persist all intermediate results | Manual — write custom storage logic per task | File-based targets only | Native — every asset is persisted via IO Managers |
| Versioned prompt/model tracking | Build it yourself | Not supported | Built-in metadata and config versioning |
| Dual storage (metrics + vectors) | Custom operators per storage backend | One target per task | IO Managers handle multi-backend writes |
| Skip re-processing unchanged data | Complex custom logic | Basic file-exists check | Asset-aware materialization |
| Swap LLM providers per environment | Fragile Connections/Variables | Hardcoded | Resource abstraction with clean config |
| Evolve from 4-step pipeline to full platform | Painful DAG restructuring | Rigid class hierarchy | Incremental asset graph extension |
| Non-engineer visibility into data health | Task-focused UI | Minimal UI | Data-focused UI with freshness and quality |

---

## Where Airflow and Luigi Still Have Merits

<aside>
⚖️

To be fair — context matters.

</aside>

- **Airflow** has a massive community and ecosystem. If we were inheriting an existing Airflow setup with hundreds of DAGs, migration wouldn't be trivial. It's also strong for pure scheduling of non-data tasks (e.g., triggering deploys or sending notifications).
- **Luigi** is simple and lightweight. For a one-off batch script, it's perfectly fine — but it's largely considered legacy and lacks the features we need for a multi-phase, evolving platform.

---

## The Bottom Line

<aside>
🎯

**Dagster was built for exactly the kind of system we're designing.** Our pipeline produces a chain of interconnected data assets (profiles, scores, vectors, matches) that need to be versioned, auditable, and incrementally extendable across 4 phases of evolution. Dagster's asset-centric model, resource abstraction, and built-in observability map directly to our architectural decisions — while Airflow and Luigi would require us to build much of this infrastructure from scratch.

</aside>
