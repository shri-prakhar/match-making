# Job Category Prompts Proposal (from Notion)

Proposed **CV extraction** and **refinement** prompt text for each job category, derived from [Data Points per Job Category](https://www.notion.so/cliff-indigo-30c3/Data-Points-per-Job-Category-31f4d743fe7380b5b10ace1b45102319). Use these to seed `job_category_prompts` (e.g. via `scripts/seed_job_category_prompts.py`). Category names should match pipeline values (from job normalization LLM and candidate `desired_job_categories`). For aliases (e.g. Compliance = Legal), add a duplicate row with the same prompt content.

**CV extraction:** Prompts emphasize **relevant skills for the category** and ask for evidence per skill. Extract generously: more skills with evidence is better.

---

## Universal context (all categories)

- **CV**: tenure, job titles (seniority), company relevancy, LinkedIn/Twitter followers where relevant.
- **Extraction**: emphasize measurable impact, ownership, and evidence; same JSON schema for all.

---

## Non-technical categories

### Account Executive

**cv_extraction_prompt**

```
For sales/account executive roles, extract and emphasize: quota ownership and attainment; average deal size; annual revenue closed; named strategic accounts; enterprise or mid-market sales experience; outbound vs inbound ownership. Prefer evidence of closing revenue and owning a quota, not just supporting sales. Note buyer types (founders, protocols, infrastructure, exchanges, enterprises) and size of accounts managed. Capture tenure, title progression, and company seniority.
```

**refinement_prompt**

```
Prioritize: actual revenue closed and quota ownership (not just relationship management). Weight size of accounts managed, relevance of network, seniority of previous employers, and evidence of closing. Distinguish enterprise/mid-market from small transactional sales.
```

---

### Business Development

**cv_extraction_prompt**

```
For business development roles, extract: number and type of partnerships signed; value of partnerships; strategic accounts and ecosystem names; cross-functional work with product and marketing; go-to-market partnership examples. Emphasize outcomes (integrations, revenue, ecosystem growth, market access) and access to relevant decision-makers. Capture public speaking, conferences, and content around partnerships and ecosystem building where present.
```

**refinement_prompt**

```
Prioritize: real partnerships that led to integrations, revenue, or ecosystem growth. Weight partnership type and value, strategic account names, and access to decision-makers. Consider cross-functional work with product and marketing.
```

---

### Community & Marketing

**cv_extraction_prompt**

```
For community and marketing roles, extract: follower growth and engagement growth achieved; campaign metrics; community and user growth from campaigns; retention or activation impact; content quality and consistency; successful launches or campaigns. Emphasize organic/social growth, brand, content, narrative, partnerships, and ecosystem campaigns. Capture posting consistency, audience quality, and topic relevance where visible.
```

**refinement_prompt**

```
Prioritize: ability to build and maintain attention, create community trust, and turn awareness into growth. Score organic growth, social growth, brand, content, narrative, partnerships, and ecosystem campaigns. Weight measurable campaign and community impact.
```

---

### Customer Support

**cv_extraction_prompt**

```
For customer support roles, extract: number of customers served; support volume; CSAT/NPS impact; reduction in ticket backlog; escalation handling; tooling used; multilingual support if relevant. Emphasize response time, resolution time, retention, and satisfaction improvements. Note experience with technical crypto users, wallets, exchanges, custody, staking, DeFi, or onboarding flows. LinkedIn followers are a weak signal; prioritize operations maturity, communication clarity, and product familiarity.
```

**refinement_prompt**

```
Prioritize: support at scale; improvements in response time, resolution time, retention, satisfaction. Weight operations maturity, communication clarity, product familiarity, and scale of user support. Public social metrics matter less here.
```

---

### Designer

**cv_extraction_prompt**

```
For design roles, extract: portfolio quality; shipped designs; product scale; design systems ownership; UX and conversion improvements; brand consistency across launches; crypto-native aesthetic where relevant. Emphasize what was actually designed (brand, product, motion, growth, or systems), whether work was shipped and used by real users, and evidence of conversion, usability, or brand impact. Capture Dribbble/Behance/website and Twitter engagement where present.
```

**refinement_prompt**

```
Prioritize: portfolio quality, relevance to crypto/web3 aesthetics and product patterns, shipped work, and visible craft level. Followers alone are a weak signal.
```

---

### Growth

**cv_extraction_prompt**

```
For growth roles, extract: user growth numbers; activation and retention improvement; CAC improvements; funnel optimization; growth loops and channels owned; experimentation velocity. Emphasize acquisition, activation, retention, referrals, monetization, funnel ownership, and token-driven or community-led growth. Capture past company growth trajectories and visible growth content where present.
```

**refinement_prompt**

```
Prioritize: measurable growth impact, experimentation mindset, channel sophistication, and crypto-native distribution knowledge.
```

---

### Legal

**cv_extraction_prompt**

```
For legal roles, extract: relevant jurisdictions; transaction types; licensing and compliance work; token/DAO/foundation experience; contract ownership; external counsel coordination; scale and complexity of matters. Emphasize legal domain (regulatory, employment, corporate, token structuring, funds, compliance, licensing), jurisdiction relevance, and whether role was execution-oriented or advisory. LinkedIn tenure and title progression are useful; public social signals are secondary.
```

**refinement_prompt**

```
Prioritize: specialization relevance, complexity of legal work, jurisdiction fit, and employer quality. Public social signals are secondary.
```

---

### Operations

**cv_extraction_prompt**

```
For operations roles, extract: functions owned; process improvements; operational scale; tooling implemented; hiring/finance/compliance/people ops exposure; cross-functional coordination; international or remote operations. Emphasize building or improving systems, managing execution across teams, operating in ambiguity, and supporting fast-moving teams.
```

**refinement_prompt**

```
Prioritize: scope, ownership, complexity, and execution reliability. Public audience metrics are low priority.
```

---

### Product Designer

**cv_extraction_prompt**

```
For product design roles, extract: user problems solved; shipped interfaces; design systems; user research; collaboration with PM and engineering; measurable product or UX impact. Emphasize product design with real usage, understanding of onboarding, dashboards, wallet/DeFi/trading UX, and mobile flows. Balance of UX quality with speed.
```

**refinement_prompt**

```
Prioritize: portfolio depth, shipped work, product thinking, and crypto UX familiarity.
```

---

### Product Manager

**cv_extraction_prompt**

```
For product management roles, extract: size of product shipped; MAU/DAU scale; adoption rates; roadmap ownership; cross-functional leadership; feature launches; user impact; retention or activation results. Emphasize roadmap and prioritization ownership, shipped products with user adoption, and stage (0→1, 1→10, scale). Note technical crypto product and ecosystem-specific constraints.
```

**refinement_prompt**

```
Distinguish: project coordinators with PM titles vs true product owners; strategy-heavy vs execution-heavy PMs. Weight shipped product scale and adoption.
```

---

### Product Marketer

**cv_extraction_prompt**

```
For product marketing roles, extract: launches owned; positioning work; GTM strategy; activation and adoption metrics; messaging frameworks; content strategy; cross-functional work with product and growth. Emphasize clear positioning, launch leadership, and connecting product, audience, and narrative to influence adoption.
```

**refinement_prompt**

```
Score both communication quality and actual launch impact.
```

---

### Project Manager

**cv_extraction_prompt**

```
For project management roles, extract: size of projects managed; timeline ownership; cross-functional coordination; budget ownership; delivery success; process improvement; stakeholder management. Emphasize outcomes (cost savings, delivery speed, revenue, process improvement), remote/async team management, and fast-moving environments.
```

**refinement_prompt**

```
Prioritize: execution discipline, complexity handled, and delivery record.
```

---

### Talent Sourcing / Human Resources

**cv_extraction_prompt**

```
For talent sourcing and HR roles, extract: jobs managed; hires completed; time-to-fill; offer acceptance rate; seniority of hires; functions hired for; hiring geography; stakeholder management; pipeline ownership. Emphasize types of roles hired for, talent market understanding (including crypto), passive sourcing, full-cycle vs sourcing-only, and hiring across technical, GTM, leadership, and ecosystem roles.
```

**refinement_prompt**

```
Score: hiring relevance, quality of hiring network, pace of execution, and web3 talent access.
```

---

### Public Relations

**cv_extraction_prompt**

```
For PR roles, use the default extraction: emphasize measurable impact, tenure, ownership, and evidence; capture relevant metrics and campaign outcomes where present.
```

**refinement_prompt**

```
Use default refinement: evaluate against must-haves and overall fit; consider skills match, experience level, and domain relevance.
```

---

### Research

**cv_extraction_prompt**

```
For research roles, use the default extraction: emphasize measurable impact, tenure, ownership, and evidence; capture publications, methods, and domain expertise where present.
```

**refinement_prompt**

```
Use default refinement: evaluate against must-haves and overall fit; consider skills match, experience level, and domain relevance.
```

---

## Technical categories

### General (technical)

**cv_extraction_prompt**

```
For technical roles generally: extract role title and seniority; tenure per role; leadership scope; company and team/product scale; ownership of services/features/modules; measurable impact (e.g. reduced latency, cost saved, throughput). Capture tech stack (languages, frameworks, infra), public recognitions, and red flags (e.g. many short stints). For code/GitHub: match to JD (languages, frameworks); recent activity (6–12 months); README, structure, tests, CI/CD, deployment; PR quality, OSS contributions; code quality, security basics, complexity. Prefer 1–2 substantial repos over quantity.
```

**refinement_prompt**

```
Evaluate technical fit from JD: must-haves vs nice-to-haves, level (Senior/Staff/Lead), success metrics, hard blockers. Weight CV evidence (tenure, ownership, impact, tech stack) and GitHub/code quality where relevant.
```

---

### AI Engineer

**cv_extraction_prompt**

```
For AI/ML engineering: extract data hygiene, reproducible training pipelines, experiment tracking (e.g. MLflow, W&B), model versioning, inference monitoring, cost/latency trade-offs, preprocessing and feature engineering.
```

**refinement_prompt**

```
Prioritize: data hygiene, reproducible pipelines, experiment tracking, model versioning, inference monitoring, and clear preprocessing and feature engineering.
```

---

### Backend Developer

**cv_extraction_prompt**

```
For backend roles: extract error handling and idempotency; transactional integrity; concurrency model; database rationale; message queues/eventing vs sync; caching strategy; sensible retry logic. Also apply general technical extraction (tenure, ownership, impact, tech stack, GitHub quality).
```

**refinement_prompt**

```
Prioritize: error handling, idempotency, transactional integrity, concurrency model, DB and eventing choices, caching, retry logic.
```

---

### Data Engineer

**cv_extraction_prompt**

```
For data engineering: extract idempotent ETL/ELT; partitioning and retention; schema versioning; data validation tests; DAGs with CI; data SLAs and freshness monitoring.
```

**refinement_prompt**

```
Prioritize: idempotent pipelines, partitioning/retention, schema versioning, validation tests, DAG/CI, and data SLAs.
```

---

### DevOps

**cv_extraction_prompt**

```
For DevOps/SRE: extract IaC modules and state management (locks, dry-runs); safe rollouts (canary, blue-green); autoscaling rules; runbooks; cost ops and backup/restore.
```

**refinement_prompt**

```
Prioritize: IaC and state management, safe rollouts, autoscaling, runbooks, cost ops and backup/restore.
```

---

### DevRel

**cv_extraction_prompt**

```
For developer relations: extract clean SDKs and example apps; quickstarts; clear docs; adoption metrics (downloads, issues triaged); public talks and tutorials.
```

**refinement_prompt**

```
Prioritize: SDK/docs quality, quickstarts, adoption metrics, and public presence (talks, tutorials).
```

---

### Frontend Developer

**cv_extraction_prompt**

```
For frontend roles: extract component structure; state and render performance (avoid over-useEffect); accessibility (a11y); bundle size and code-splitting; e2e tests; SSR/CSR trade-offs.
```

**refinement_prompt**

```
Prioritize: component structure, performance, a11y, bundle/code-splitting, e2e tests, SSR/CSR trade-offs.
```

---

### Full-Stack Developer

**cv_extraction_prompt**

```
For full-stack roles: extract end-to-end ownership (frontend, API, deployment); e2e tests; API design for frontend consumption; security basics.
```

**refinement_prompt**

```
Prioritize: end-to-end ownership, e2e coverage, API design for frontend, security basics.
```

---

### Infrastructure Engineer

**cv_extraction_prompt**

```
For infrastructure: extract cluster/region scale expertise; stateful services ops; disaster recovery tests; capacity planning; automation for scaling.
```

**refinement_prompt**

```
Prioritize: scale expertise, stateful ops, DR, capacity planning, scaling automation.
```

---

### Mobile Engineer

**cv_extraction_prompt**

```
For mobile roles: extract offline-first approach (local DB, background sync); crash and memory handling; CI for builds; store release notes and rollout strategy; ANR/crash metrics.
```

**refinement_prompt**

```
Prioritize: offline-first, crash/memory handling, CI, release/rollout strategy, ANR/crash metrics.
```

---

### Protocol Engineer

**cv_extraction_prompt**

```
For protocol/on-chain roles: extract well-tested on-chain programs; account validation; compute-budget optimization; Anchor/IDL usage; deploy/test scripts; audit history or fuzz/property tests.
```

**refinement_prompt**

```
Prioritize: tested on-chain programs, account validation, compute-budget optimization, Anchor/IDL, deploy/test scripts, audit or fuzz/property tests.
```

---

### QA Engineer

**cv_extraction_prompt**

```
For QA roles: extract reliable automation suites; flaky-test handling; CI gating rules; test-data management; regression and perf testing strategy.
```

**refinement_prompt**

```
Prioritize: automation quality, flaky-test handling, CI gating, test-data management, regression/perf strategy.
```

---

### Security

**cv_extraction_prompt**

```
For security roles: extract threat modeling; SAST/DAST and dependency scanning in CI; secret management; incident/pentest history; remediation timelines.
```

**refinement_prompt**

```
Prioritize: threat modeling, SAST/DAST and dependency scanning, secret management, incident/pentest history, remediation.
```

---

## Seeding the database

- Insert one row per category into `job_category_prompts` with `job_category`, `cv_extraction_prompt`, `refinement_prompt`, and `updated_at`.
- For categories with no custom text above (e.g. Public Relations, Research), you can omit the row so the in-code default is used, or insert a row with the default text for consistency.
- Add alias rows (e.g. `job_category = 'Compliance'` with same prompts as Legal) so lookup by pipeline value works.
