# Scoring Weights by Job Category

This document explains the manual per-category tuning of matchmaking scoring weights stored in the `scoring_weights` table. Weights are applied when the matchmaking pipeline (or scripts using the matchmaking resource) scores candidates against jobs; each job’s `job_category` selects one row from `scoring_weights`.

**Script:** `scripts/seed_scoring_weights_by_category.py`
**Run against remote DB:** `poetry run with-remote-db python scripts/seed_scoring_weights_by_category.py`

---

## How the score is built

- **Vector score** = blend of five similarity terms (each 0–1): role, domain, culture, **impact**, **technical**.
  The five **vector sub-weights** (role_weight, domain_weight, culture_weight, impact_weight, technical_weight) sum to **1.0**.
- **Combined base** = vector_weight × vector_score + skill_fit_weight × skill_fit + compensation_weight × comp + location_weight × location + seniority_scale_weight × seniority_scale_fit.
  These five **top-level weights** sum to **1.0**.
- **Deductions** (capped per type): years-based seniority, seniority level mismatch, tenure instability (high-stakes jobs only). These are subtracted from the base to get the final combined score.

We tune **vector sub-weights** per category and **top-level weights** for two regimes: a skill-heavy default (technical roles) and a balanced vector/skill-fit blend (non-technical roles).

---

## Rationale by category group

### Sales: Account Executive, Business Development

- **Impact weight raised (0.25).** These roles are evaluated on revenue closed, quota attainment, deal size, and partnership value. The impact vector encodes scale of outcomes (revenue, deals, ecosystem impact), so we give it a larger share of the vector score.
- **Role and domain** remain strong (0.30, 0.25) for title/level and industry fit.
- **Technical weight 0.** Sales success is not primarily technical similarity.

### Technical — general baseline: General (technical), Backend, Full-Stack

- **Technical weight 0.15.** The technical vector captures stack, systems, and depth; for general engineering roles we want it to affect the score without dominating.
- **Impact weight 0.15.** Scale of systems, ownership, and measurable outcomes still matter.
- **Culture 0.15** (slightly lower than default) so role, domain, and technical carry more weight.

### Technical — deep specialization: AI Engineer, Mobile, Protocol, Security

- **Technical weight 0.20–0.25.** These roles have platform-locked or deeply non-transferable expertise (ML frameworks, iOS/Android, consensus/cryptography, security tooling). The technical similarity vector should carry the most weight among engineering categories.
- **AI Engineer and Mobile Engineer** get technical 0.25 — the most platform-specific engineering disciplines.
- **Protocol Engineer** gets technical 0.25 plus higher domain (0.28) for blockchain/crypto specialization.
- **Security** gets technical 0.20 with higher domain (0.28) for specialization area (appsec, infra, crypto).

### Technical — tooling-heavy: Data Engineer, DevOps, Infrastructure Engineer

- **Technical weight 0.20.** Tooling stacks (Spark, Flink, dbt, Terraform, K8s, CI/CD) are specialized and less transferable than general backend/frontend frameworks.
- **Domain 0.27.** Industry-specific data/infra requirements (fintech compliance, healthcare HIPAA, etc.) matter.

### Technical — Frontend Developer

- **Culture 0.18** (higher than other technical roles). Frontend work intersects with design systems, UX sensibility, and product taste — partially captured by the culture vector.
- **Domain 0.22** (slightly lower) since frontend frameworks are more transferable across domains.

### Technical — QA Engineer

- **Domain 0.30** (highest among technical roles). Testing in regulated industries (fintech, healthcare) vs consumer apps requires fundamentally different expertise.
- **Technical 0.10** (lower) since QA tooling is more transferable across stacks.

### Product Manager

- **Impact weight 0.25, culture weight 0.25.** PM success depends on product scale (MAU/DAU, adoption) but also heavily on team dynamics, communication style, and decision-making culture. Domain lowered to 0.20 since PM domain expertise transfers more easily than technical domain.

### Growth

- **Impact weight 0.25, technical weight 0.05.** Growth roles are analytical and involve experimentation frameworks, data pipelines, and A/B testing tooling — justifying a small technical component.
- **Culture 0.17** (lower than PM) since growth is more metrics-driven than culture-dependent.

### Product Marketer

- **Culture weight 0.25, impact 0.20.** Brand voice, narrative alignment, and positioning are heavily culture-dependent. Impact is meaningful but less decisive than for PM/Growth.

### Community & Marketing / Public Relations

- **Impact 0.20, culture 0.25.** Campaign impact, community growth, and narrative/positioning matter (impact); culture and audience fit matter for brand and community roles.
- **Role and domain** 0.30, 0.25.

### Design: Designer, Product Designer

- **Role and culture 0.35, 0.25.** Portfolio, craft, and shipped work are captured in role/culture and skill fit; we don’t add technical weight.
- **Impact 0.15** to reflect “shipped work” and product impact.

### Legal, Compliance, Operations

- **Domain 0.35 (Legal/Compliance 0.35), impact 0.05–0.10.** Specialization (jurisdiction, practice area, ops domain) is critical; “impact” in the vector sense is less relevant than for revenue/growth roles.
- **Culture 0.25** for team and environment fit.

### Customer Support, Talent / HR, Project Manager

- **Domain and culture** emphasized; **impact 0.10.** Support, HR, and project delivery are evaluated on domain expertise, process, and fit; impact is secondary.

### Research

- **Domain 0.35, technical 0.10.** Research roles (ML, protocol, systems) are deeply technical — ignoring the technical vector misses real signal. Culture lowered to 0.15 to make room; impact stays at 0.10 (publications, citations).

### DevRel

- **Technical 0.10** (docs, SDKs, adoption); **culture 0.22** (community, talks, developer empathy). Sits between technical and community roles.

---

## Base and top-level overrides

### Default base (_BASE) — technical and “unknown” categories

- **Top-level:** vector **0.28**, skill_fit **0.42**, compensation 0.10, location 0.15, seniority_scale 0.05.
  Slightly skill-heavy: hard skills are highly predictive for engineering and many other roles.
- **Skill-fit sub-weights:** skill_rating **0.75**, skill_semantic **0.25**.
  Slightly more weight on semantic similarity so near-match skills (e.g. “Python” vs “Python 3”) get partial credit.
- **Deduction caps:** seniority_max_deduction 0.2, seniority_level_max_deduction 0.1, tenure_instability_max_deduction 0.1.

Used for all technical roles (Backend, Frontend, AI Engineer, Data Engineer, etc.), DevRel, and Research.

### Soft top-level override (_TOP_LEVEL_SOFT) — non-technical categories

- **Top-level:** vector **0.35**, skill_fit **0.35** (comp, location, seniority unchanged).
  Softer roles (Sales, PM, Marketing, Design, Legal, Support, HR, etc.) depend more on culture, domain, and impact than on an exact skill checklist; vector and skill_fit are balanced.

Applied to: Account Executive, Business Development, Product Manager, Growth, Product Marketer, Community & Marketing, Public Relations, Designer, Product Designer, Legal, Compliance, Operations, Customer Support, Project Manager, Talent Sourcing / Human Resources.

---

## Applying the weights on remote

1. Ensure the remote DB has the `scoring_weights` table and the latest columns (impact_weight, technical_weight, seniority_scale_weight, seniority_level_max_deduction, tenure_instability_max_deduction). Run migrations if needed.
2. From your machine (with tunnel to remote DB, e.g. `poetry run remote-ui` or `poetry run local-matchmaking`):
   - List categories:
     `poetry run with-remote-db python scripts/seed_scoring_weights_by_category.py --list`
   - Preview changes:
     `poetry run with-remote-db python scripts/seed_scoring_weights_by_category.py --dry-run`
   - Apply:
     `poetry run with-remote-db python scripts/seed_scoring_weights_by_category.py`
3. New job categories that appear later in `normalized_jobs` will get a row created on first use by the matchmaking resource (with in-code default weights). Re-run the seed script periodically to assign tuned weights to any new categories you add to `TUNED_WEIGHTS`.

---

## Summary table (vector sub-weights only)

| Category group              | role | domain | culture | impact | technical |
|----------------------------|------|--------|---------|--------|-----------|
| Sales (AE, BD)             | 0.30 | 0.25   | 0.20    | **0.25** | 0    |
| Technical baseline (Backend, Full-Stack, General) | 0.30 | 0.25 | 0.15 | 0.15 | 0.15 |
| AI Engineer                | 0.25 | 0.25   | 0.10    | 0.15   | **0.25** |
| Mobile Engineer            | 0.25 | 0.25   | 0.10    | 0.15   | **0.25** |
| Protocol Engineer          | 0.22 | 0.28   | 0.10    | 0.15   | **0.25** |
| Security                   | 0.25 | 0.28   | 0.12    | 0.15   | **0.20** |
| Data / DevOps / Infra      | 0.25 | 0.27   | 0.13    | 0.15   | **0.20** |
| Frontend Developer         | 0.30 | 0.22   | 0.18    | 0.15   | 0.15 |
| QA Engineer                | 0.30 | **0.30** | 0.15  | 0.15   | 0.10 |
| DevRel                     | 0.28 | 0.25   | 0.22    | 0.15   | 0.10 |
| Product Manager            | 0.30 | 0.20   | **0.25** | **0.25** | 0  |
| Growth                     | 0.28 | 0.25   | 0.17    | **0.25** | 0.05 |
| Product Marketer           | 0.30 | 0.25   | **0.25** | 0.20  | 0    |
| Community & Marketing / PR | 0.30 | 0.25   | 0.25    | 0.20   | 0    |
| Design                     | 0.35 | 0.25   | 0.25    | 0.15   | 0    |
| Legal / Compliance         | 0.35 | **0.35** | 0.25  | 0.05   | 0    |
| Operations / Support / HR / PM (Project) | 0.35 | 0.30 | 0.25 | 0.10 | 0   |
| Research                   | 0.30 | **0.35** | 0.15  | 0.10   | 0.10 |

All rows sum to 1.0.
