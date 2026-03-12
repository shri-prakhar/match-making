# Skills Taxonomy System Proposal

> **Version:** 1.0
> **Date:** February 2026
> **Purpose:** Define a unified skills taxonomy system with LLM-assisted skill normalization, rating, and matching

---

## Table of Contents

1. [Overview](#1-overview)
2. [Skills Taxonomy Design](#2-skills-taxonomy-design)
3. [Skill Normalization Pipeline](#3-skill-normalization-pipeline)
4. [Skill Rating System](#4-skill-rating-system)
5. [Role Fitness Scoring](#5-role-fitness-scoring)
6. [Database Schema](#6-database-schema)
7. [LLM Prompts](#7-llm-prompts)

---

## 1. Overview

### The Problem

Skills in candidate data come in many variants:
- "TypeScript", "Typescript", "TS", "typescript"
- "React.js", "ReactJS", "React", "react"
- "K8s", "Kubernetes", "k8s", "kubernetes"

We need a **canonical skills taxonomy** that:
1. Normalizes all skill variants to a single canonical form
2. Allows users to select from a curated list in the frontend
3. Handles new skills gracefully (map to existing or create new)
4. Rates each skill for each candidate (1-5 scale)
5. Tracks years of experience per skill

### System Goals

| Goal | How We Achieve It |
|------|-------------------|
| **Consistent skill names** | Canonical `skills` table with unique names |
| **Handle variations** | `skill_aliases` table mapping variants → canonical |
| **User-friendly selection** | Frontend uses canonical skills list |
| **New skill handling** | LLM decides: map to existing or create new |
| **Per-candidate ratings** | `candidate_skills` table with rating + years |
| **Searchability** | Filter by skill name, min rating, min years |

---

## 2. Skills Taxonomy Design

### 2.1 Canonical Skills Table

Each skill is a unique bucket. All skills are treated as equals — no hierarchy or categorization.

```
┌─────────────────────────────────────────────────────────────┐
│                      SKILLS TABLE                           │
├─────────────────────────────────────────────────────────────┤
│  id: UUID                                                   │
│  name: "TypeScript"           ← Canonical display name      │
│  slug: "typescript"           ← URL-safe identifier         │
│  description: "Typed superset of JavaScript"                │
│  created_at: timestamp                                      │
│  is_active: true              ← Can be deprecated           │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Skill Aliases (Variants Mapping)

Maps all known variants to the canonical skill:

```
┌─────────────────────────────────────────────────────────────┐
│                   SKILL_ALIASES TABLE                       │
├─────────────────────────────────────────────────────────────┤
│  alias: "TS"          →  skill_id: (TypeScript)             │
│  alias: "Typescript"  →  skill_id: (TypeScript)             │
│  alias: "typescript"  →  skill_id: (TypeScript)             │
│  alias: "ReactJS"     →  skill_id: (React)                  │
│  alias: "React.js"    →  skill_id: (React)                  │
│  alias: "K8s"         →  skill_id: (Kubernetes)             │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 Initial Skills Seed

Based on the current dataset, here are the canonical skills to seed:

**Programming Languages:**
```
TypeScript, JavaScript, Rust, Python, Go, Solidity, Java, Kotlin,
Scala, C, C++, Ruby, Move, SQL
```

**Frameworks & Libraries:**
```
React, Next.js, Node.js, Express.js, Angular, Vue.js, SolidJS,
Svelte, Anchor, Hardhat, Foundry, Django, Flask, FastAPI,
Spring Boot, NestJS, TanStack Query, Zustand, Redux
```

**Databases:**
```
PostgreSQL, MongoDB, MySQL, Redis, RocksDB, Supabase, Firebase
```

**Infrastructure & DevOps:**
```
Docker, Kubernetes, AWS, GCP, Azure, Terraform, GitHub Actions,
CI/CD, Nginx, HAProxy
```

**Blockchain & Web3:**
```
Solana, Ethereum, Cosmos, Sui, Polkadot, Mina, DeFi, NFT,
Cross-chain, ZK Proofs, Smart Contracts
```

**Tools & Protocols:**
```
GraphQL, REST API, gRPC, Kafka, RabbitMQ, WebSocket,
TradingView, Jito, Jupiter, Meteora, Metaplex
```

**Domains & Skills:**
```
AI Agents, Machine Learning, Full Stack, Backend, Frontend,
DevRel, DevOps, Protocol Engineering, Trading Systems,
SDK Development, Mobile Development
```

---

## 3. Skill Normalization Pipeline

### 3.1 Flow Overview

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Raw Skill List  │ →  │  Normalize Each  │ →  │  Resolved Skills │
│  from Candidate  │    │  Against Aliases │    │  with IDs        │
└──────────────────┘    └──────────────────┘    └──────────────────┘
         │                      │                        │
         │                      ▼                        │
         │               ┌──────────────┐                │
         │               │ Unknown?     │                │
         │               │ Call LLM     │                │
         │               └──────────────┘                │
         │                      │                        │
         │         ┌────────────┴────────────┐           │
         │         ▼                         ▼           │
         │  ┌─────────────┐          ┌─────────────┐     │
         │  │ Map to      │          │ Create New  │     │
         │  │ Existing    │          │ Skill       │     │
         │  │ + Add Alias │          │             │     │
         │  └─────────────┘          └─────────────┘     │
         │                                               │
         └───────────────────────────────────────────────┘
```

### 3.2 Normalization Algorithm

```python
def normalize_skill(raw_skill: str) -> Skill:
    """
    Normalize a raw skill string to a canonical skill.
    """
    # Step 1: Clean the input
    cleaned = raw_skill.strip().lower()

    # Step 2: Check exact match in skills table
    skill = db.query(Skills).filter(func.lower(Skills.name) == cleaned).first()
    if skill:
        return skill

    # Step 3: Check aliases table
    alias = db.query(SkillAliases).filter(func.lower(SkillAliases.alias) == cleaned).first()
    if alias:
        return alias.skill

    # Step 4: LLM resolution for unknown skill
    return resolve_unknown_skill_with_llm(raw_skill)


def resolve_unknown_skill_with_llm(raw_skill: str) -> Skill:
    """
    Use LLM to decide: map to existing skill or create new.
    """
    existing_skills = db.query(Skills.name).all()

    response = llm.complete(
        prompt=SKILL_RESOLUTION_PROMPT.format(
            unknown_skill=raw_skill,
            existing_skills=existing_skills
        )
    )

    # Response format: {"action": "map", "target": "TypeScript"}
    # or: {"action": "create", "name": "Cairo", "description": "..."}

    if response["action"] == "map":
        skill = db.query(Skills).filter(Skills.name == response["target"]).first()
        # Add alias for future lookups
        db.add(SkillAliases(alias=raw_skill, skill_id=skill.id, added_by="llm"))
        return skill
    else:
        # Create new skill
        new_skill = Skills(
            name=response["name"],
            slug=slugify(response["name"]),
            description=response.get("description"),
            created_by="llm"
        )
        db.add(new_skill)
        return new_skill
```

### 3.3 Frontend Skill Selection

When a user selects skills in the frontend:
1. **Autocomplete** from canonical skills list
2. **Free text entry** allowed for new skills
3. On submit, **normalize through pipeline**
4. If LLM creates new skill, optionally **flag for human review**

---

## 4. Skill Rating System

### 4.1 Rating Scale

| Rating | Label | Description |
|--------|-------|-------------|
| 1 | Beginner | Basic understanding, limited practical experience |
| 2 | Elementary | Can complete simple tasks with guidance |
| 3 | Intermediate | Comfortable with common use cases, works independently |
| 4 | Advanced | Deep expertise, can architect solutions, mentor others |
| 5 | Expert | Industry-recognized expertise, significant contributions |

### 4.2 LLM Skill Rating Pipeline

For each candidate, after skill normalization:

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Candidate CV    │ →  │  LLM Rates Each  │ →  │  Store Ratings   │
│  + Normalized    │    │  Skill 1-5       │    │  in DB           │
│  Skills List     │    │  + Years Est.    │    │                  │
└──────────────────┘    └──────────────────┘    └──────────────────┘
```

### 4.3 What We Store Per Skill

| Field | Type | Description |
|-------|------|-------------|
| `skill_id` | UUID | Reference to canonical skill |
| `rating` | INTEGER (1-5) | LLM-assessed proficiency |
| `years_experience` | INTEGER | Years using this skill (from CV) |
| `notable_achievement` | TEXT | Best example of skill usage |
| `skill_vector` | VECTOR(1536) | Embedding of achievement (for search) |

---

## 5. Role Fitness Scoring

### 5.1 Desired Roles

Candidates specify desired roles (e.g., "Backend Developer", "Protocol Engineer").

For each desired role, we compute a **fitness score** based on their skills.

### 5.2 Role Requirements Mapping

Define expected skills per role:

```json
{
  "Backend Developer": {
    "core_skills": ["Node.js", "PostgreSQL", "REST API", "Docker"],
    "bonus_skills": ["Kubernetes", "Kafka", "Redis", "GraphQL"],
    "weight_core": 0.7,
    "weight_bonus": 0.3
  },
  "Protocol Engineer": {
    "core_skills": ["Rust", "Solana", "Smart Contracts", "Anchor"],
    "bonus_skills": ["ZK Proofs", "Cryptography", "DeFi"],
    "weight_core": 0.7,
    "weight_bonus": 0.3
  },
  "Full-Stack Developer": {
    "core_skills": ["React", "Node.js", "TypeScript", "PostgreSQL"],
    "bonus_skills": ["Next.js", "Docker", "GraphQL", "AWS"],
    "weight_core": 0.7,
    "weight_bonus": 0.3
  }
}
```

### 5.3 Fitness Score Calculation

Two approaches:

**Option A: Rule-Based (Fast, Deterministic)**
```python
def calculate_role_fitness(candidate_skills, role):
    role_req = ROLE_REQUIREMENTS[role]

    # Score core skills (weighted by rating)
    core_score = sum(
        candidate_skills.get(skill, {}).get('rating', 0) / 5
        for skill in role_req['core_skills']
    ) / len(role_req['core_skills'])

    # Score bonus skills
    bonus_score = sum(
        candidate_skills.get(skill, {}).get('rating', 0) / 5
        for skill in role_req['bonus_skills']
    ) / len(role_req['bonus_skills'])

    return (core_score * 0.7) + (bonus_score * 0.3)
```

**Option B: LLM-Based (Flexible, Contextual)**
```python
def calculate_role_fitness_llm(candidate_skills, desired_role, cv_text):
    prompt = f"""
    Given this candidate's skills and their CV, rate their fitness
    for the role "{desired_role}" on a scale of 1-10.

    Skills with ratings: {candidate_skills}

    CV Summary: {cv_text[:2000]}

    Return JSON: {{"fitness_score": <1-10>, "reasoning": "..."}}
    """
    return llm.complete(prompt)
```

**Recommendation:** Start with **Option A** for MVP (fast, no LLM cost), add LLM scoring in Phase 2 for nuanced roles.

### 5.4 Storing Role Fitness

```sql
CREATE TABLE candidate_role_fitness (
    id              UUID PRIMARY KEY,
    candidate_id    UUID REFERENCES normalized_candidates(id),
    role_name       TEXT NOT NULL,              -- "Backend Developer"
    fitness_score   FLOAT NOT NULL,             -- 0.0 - 1.0
    score_breakdown JSONB,                      -- {"core": 0.8, "bonus": 0.5, ...}
    computed_at     TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 6. Database Schema

### 6.1 Skills Taxonomy Tables

```sql
-- ═══════════════════════════════════════════════════════════════════
-- CANONICAL SKILLS TABLE
-- ═══════════════════════════════════════════════════════════════════
CREATE TABLE skills (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE,           -- "TypeScript"
    slug            TEXT NOT NULL UNIQUE,           -- "typescript"
    description     TEXT,                           -- "Typed superset of JavaScript"

    -- Metadata
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    created_by      VARCHAR(50) DEFAULT 'seed',     -- 'seed', 'llm', 'manual'
    is_active       BOOLEAN DEFAULT TRUE,
    review_status   VARCHAR(20) DEFAULT 'approved'  -- 'pending', 'approved', 'rejected'
);

CREATE INDEX idx_skills_name ON skills(name);
CREATE INDEX idx_skills_slug ON skills(slug);


-- ═══════════════════════════════════════════════════════════════════
-- SKILL ALIASES (VARIANTS MAPPING)
-- ═══════════════════════════════════════════════════════════════════
CREATE TABLE skill_aliases (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alias           TEXT NOT NULL,                  -- "TS", "Typescript", "typescript"
    skill_id        UUID NOT NULL REFERENCES skills(id) ON DELETE CASCADE,

    -- Metadata
    added_at        TIMESTAMPTZ DEFAULT NOW(),
    added_by        VARCHAR(50) DEFAULT 'manual',   -- 'seed', 'llm', 'manual'

    CONSTRAINT unique_alias UNIQUE (alias)
);

CREATE INDEX idx_skill_aliases_alias ON skill_aliases(lower(alias));
CREATE INDEX idx_skill_aliases_skill ON skill_aliases(skill_id);
```

### 6.2 Candidate Skills Table

```sql
-- ═══════════════════════════════════════════════════════════════════
-- CANDIDATE SKILLS (with rating and years)
-- ═══════════════════════════════════════════════════════════════════
CREATE TABLE candidate_skills (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,
    skill_id            UUID NOT NULL REFERENCES skills(id) ON DELETE CASCADE,

    -- Rating & Experience
    rating              INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    years_experience    INTEGER,                    -- NULL if not determinable

    -- Context for vectorization
    notable_achievement TEXT,                       -- Best example of skill usage
    skill_vector        VECTOR(1536),               -- Embedding of achievement

    -- Metadata
    rated_at            TIMESTAMPTZ DEFAULT NOW(),
    rating_model        VARCHAR(50),                -- LLM model used for rating
    rating_confidence   FLOAT,                      -- LLM confidence (0-1)

    CONSTRAINT unique_candidate_skill UNIQUE (candidate_id, skill_id)
);

CREATE INDEX idx_candidate_skills_candidate ON candidate_skills(candidate_id);
CREATE INDEX idx_candidate_skills_skill ON candidate_skills(skill_id);
CREATE INDEX idx_candidate_skills_rating ON candidate_skills(rating);
CREATE INDEX idx_candidate_skill_vec ON candidate_skills
    USING hnsw (skill_vector vector_cosine_ops);
```

### 6.3 Candidate Work Experience Table

```sql
-- ═══════════════════════════════════════════════════════════════════
-- CANDIDATE WORK EXPERIENCE
-- ═══════════════════════════════════════════════════════════════════
CREATE TABLE candidate_experiences (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- Position details
    company_name        TEXT NOT NULL,
    position_title      TEXT NOT NULL,

    -- Duration
    start_date          DATE,
    end_date            DATE,                       -- NULL = current position
    years_experience    FLOAT,                      -- Computed from dates (e.g., 2.5)
    is_current          BOOLEAN DEFAULT FALSE,

    -- Description (for vectorization)
    description         TEXT,                       -- Summary of role/responsibilities

    -- Vector for semantic matching
    position_vector     VECTOR(1536),               -- Embedding of description

    -- Ordering
    position_order      INTEGER NOT NULL,           -- 1 = most recent

    -- Metadata
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_experiences_candidate ON candidate_experiences(candidate_id);
CREATE INDEX idx_experiences_company ON candidate_experiences(company_name);
CREATE INDEX idx_position_vec ON candidate_experiences
    USING hnsw (position_vector vector_cosine_ops);
```

### 6.4 Simplified GitHub Metrics

```sql
-- ═══════════════════════════════════════════════════════════════════
-- GITHUB METRICS (SIMPLIFIED)
-- ═══════════════════════════════════════════════════════════════════
CREATE TABLE candidate_github_metrics (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id            UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- Profile info
    github_username         TEXT NOT NULL,
    github_url              TEXT,

    -- Basic metrics
    public_repos            INTEGER DEFAULT 0,
    total_stars             INTEGER DEFAULT 0,
    followers               INTEGER DEFAULT 0,

    -- Languages (from repos)
    languages               TEXT[],                 -- Top languages

    -- Fetch metadata
    fetched_at              TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_github_per_candidate UNIQUE (candidate_id)
);
```

### 6.5 Role Fitness Table

```sql
-- ═══════════════════════════════════════════════════════════════════
-- CANDIDATE ROLE FITNESS SCORES
-- ═══════════════════════════════════════════════════════════════════
CREATE TABLE candidate_role_fitness (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- Role
    role_name       TEXT NOT NULL,                  -- "Backend Developer"

    -- Score
    fitness_score   FLOAT NOT NULL,                 -- 0.0 - 1.0
    score_breakdown JSONB,                          -- Detailed breakdown

    -- Metadata
    computed_at     TIMESTAMPTZ DEFAULT NOW(),
    algorithm_version VARCHAR(50),

    CONSTRAINT unique_candidate_role UNIQUE (candidate_id, role_name)
);

CREATE INDEX idx_role_fitness_candidate ON candidate_role_fitness(candidate_id);
CREATE INDEX idx_role_fitness_score ON candidate_role_fitness(role_name, fitness_score DESC);
```

---

## 7. LLM Prompts

### 7.1 Skill Resolution Prompt

For unknown skills → decide map or create:

```
You are a technical skills taxonomy expert. Given an unknown skill, decide
whether it should be mapped to an existing skill or created as a new skill.

Unknown skill: "{unknown_skill}"

Existing skills in our taxonomy:
{existing_skills_list}

Rules:
1. If this is a variant/abbreviation/typo of an existing skill, MAP it
2. If this is a genuinely new skill not covered, CREATE it
3. When mapping, choose the most specific match (not general categories)

Respond with JSON:
- To map: {"action": "map", "target": "<existing_skill_name>", "reason": "..."}
- To create: {"action": "create", "name": "<canonical_name>", "description": "<1 sentence>"}

Examples:
- "TS" → {"action": "map", "target": "TypeScript", "reason": "Common abbreviation"}
- "React.js" → {"action": "map", "target": "React", "reason": "Same library"}
- "Cairo" → {"action": "create", "name": "Cairo", "description": "Smart contract language for StarkNet"}
```

### 7.2 Skill Rating Prompt

Rate candidate's skills from their CV:

```
You are evaluating a software developer's skill proficiency based on their CV.

Candidate CV:
---
{cv_text}
---

For each of the following skills, provide:
1. Rating (1-5): 1=Beginner, 2=Elementary, 3=Intermediate, 4=Advanced, 5=Expert
2. Years of experience with this skill (estimate from positions/projects)
3. Most notable achievement using this skill (1 sentence, or null)

Skills to rate:
{skills_list}

Scoring guidelines:
- 1 (Beginner): Mentioned once, no significant projects
- 2 (Elementary): Used in 1-2 projects, basic applications
- 3 (Intermediate): Primary skill in multiple roles, solid experience
- 4 (Advanced): Led projects, architected solutions, 4+ years deep use
- 5 (Expert): Industry recognition, core maintainer, exceptional depth

Respond with JSON:
{
  "skills": {
    "<skill_name>": {
      "rating": <1-5>,
      "years": <number or null>,
      "notable_achievement": "<string or null>"
    },
    ...
  }
}
```

### 7.3 Universal Soft Attributes Prompt

Score candidates on 5 universal attributes that apply across all job types:

```
You are evaluating a candidate's soft attributes based on their CV.

Candidate CV:
---
{cv_text}
---

Rate each attribute from 1-5 based on evidence in the CV:

1. **Leadership** (1=IC only, 3=led small projects, 5=led teams/orgs)
   Look for: "Led team of X", "Managed", "Architected", "Mentored",
   decision-making authority, hiring/building teams

2. **Autonomy** (1=needs guidance, 3=independent on tasks, 5=founder-type self-starter)
   Look for: Founder/co-founder roles, solo projects, remote work history,
   "independently", "self-directed", side projects, entrepreneurial

3. **Technical Depth** (1=surface-level, 3=solid practitioner, 5=architect/systems thinker)
   Look for: Infrastructure work, performance optimization, "designed system",
   "scalability", distributed systems, core protocol work, complexity handling

4. **Communication** (1=minimal evidence, 3=collaborates well, 5=excellent writer/speaker)
   Look for: Documentation, blog posts, conference talks, DevRel experience,
   "collaborated with", technical writing, open source maintainer

5. **Growth Trajectory** (1=static career, 3=steady growth, 5=rapid advancement)
   Look for: Promotions within company, expanding scope, title progression,
   quick learning of new domains, career acceleration signals

Respond with JSON:
{
  "leadership_score": <1-5>,
  "autonomy_score": <1-5>,
  "technical_depth_score": <1-5>,
  "communication_score": <1-5>,
  "growth_trajectory_score": <1-5>,
  "reasoning": {
    "leadership": "<1 sentence justification>",
    "autonomy": "<1 sentence justification>",
    "technical_depth": "<1 sentence justification>",
    "communication": "<1 sentence justification>",
    "growth_trajectory": "<1 sentence justification>"
  }
}
```

### 7.4 Job Soft Requirements Extraction Prompt

Infer minimum attribute requirements from job description:

```
Analyze this job description and infer the minimum soft attribute scores required.

Job Description:
---
{job_description}
---

For each attribute, decide if it's required (score 3-5) or not mentioned (null):

1. **Leadership**: Does this role require leading others, decision-making authority?
   Signals: "lead a team", "key decision maker", "mentor", "architect"

2. **Autonomy**: Does this role require working independently?
   Signals: "autonomous", "self-motivated", "work independently", "proactive"

3. **Technical Depth**: Does this role require deep systems/architecture expertise?
   Signals: "architect", "complex challenges", "scalability", "performance"

4. **Communication**: Does this role require strong collaboration/writing skills?
   Signals: "collaborate", "articulate", "documentation", "stakeholders"

5. **Growth Trajectory**: Does this role prefer candidates with rapid growth history?
   Signals: "fast learner", "growth mindset" (rarely explicit, usually null)

Respond with JSON:
{
  "min_leadership_score": <3-5 or null>,
  "min_autonomy_score": <3-5 or null>,
  "min_technical_depth_score": <3-5 or null>,
  "min_communication_score": <3-5 or null>,
  "min_growth_trajectory_score": <3-5 or null>,
  "reasoning": "<2-3 sentences explaining key inferences>"
}
```

### 7.5 Role Fitness Prompt (Phase 2)

For LLM-based role fitness scoring:

```
Evaluate this candidate's fitness for the role "{role_name}".

Candidate Skills (with ratings):
{skills_with_ratings_json}

Candidate CV Summary:
{cv_summary}

Role Requirements for {role_name}:
- Core skills: {core_skills}
- Bonus skills: {bonus_skills}
- Typical responsibilities: {role_description}

Provide a fitness score (1-10) and explanation:
- 1-3: Poor fit, missing critical skills
- 4-5: Partial fit, could grow into role
- 6-7: Good fit, meets most requirements
- 8-9: Strong fit, exceeds requirements
- 10: Exceptional fit, ideal candidate

Respond with JSON:
{
  "fitness_score": <1-10>,
  "strengths": ["...", "..."],
  "gaps": ["...", "..."],
  "reasoning": "<2-3 sentences>"
}
```

---

## 8. Implementation Plan

### Phase 1: Skills Taxonomy (Week 1)
- [ ] Create `skills` and `skill_aliases` tables
- [ ] Seed initial skills from existing dataset
- [ ] Implement normalization function
- [ ] Add LLM resolution for unknown skills

### Phase 2: Skill Rating (Week 1-2)
- [ ] Create `candidate_skills` table
- [ ] Implement skill rating LLM prompt
- [ ] Extract years from CV positions
- [ ] Store notable achievements

### Phase 3: Experience & Vectors (Week 2)
- [ ] Create `candidate_experiences` table
- [ ] Extract positions from CVs
- [ ] Generate position vectors
- [ ] Generate skill vectors (from achievements)

### Phase 4: Role Fitness (Week 2-3)
- [ ] Define role requirements mapping
- [ ] Implement rule-based fitness scoring
- [ ] Create `candidate_role_fitness` table
- [ ] (Optional) Add LLM-based scoring

---

## Appendix: Example Data

### Skill Rating Example

Input skills: `["TypeScript", "Rust", "Solana", "React", "PostgreSQL"]`

LLM Response:
```json
{
  "skills": {
    "TypeScript": {
      "rating": 5,
      "years": 5,
      "notable_achievement": "Built high-performance trading UI handling 50K daily users"
    },
    "Rust": {
      "rating": 3,
      "years": 2,
      "notable_achievement": "Developed cross-chain bridge wrapper contracts"
    },
    "Solana": {
      "rating": 4,
      "years": 3,
      "notable_achievement": "Architected DEX supporting 3K+ daily active users"
    },
    "React": {
      "rating": 5,
      "years": 5,
      "notable_achievement": "Led frontend architecture for multi-chain blockchain explorer"
    },
    "PostgreSQL": {
      "rating": 3,
      "years": 4,
      "notable_achievement": null
    }
  }
}
```

### Role Fitness Example

Candidate applying for "Protocol Engineer":

```json
{
  "role_name": "Protocol Engineer",
  "fitness_score": 0.72,
  "score_breakdown": {
    "core_skills": {
      "Rust": {"rating": 3, "coverage": 0.6},
      "Solana": {"rating": 4, "coverage": 0.8},
      "Smart Contracts": {"rating": 4, "coverage": 0.8},
      "Anchor": {"rating": 4, "coverage": 0.8}
    },
    "core_average": 0.75,
    "bonus_skills": {
      "ZK Proofs": {"rating": 0, "coverage": 0.0},
      "Cryptography": {"rating": 0, "coverage": 0.0},
      "DeFi": {"rating": 4, "coverage": 0.8}
    },
    "bonus_average": 0.27
  }
}
```
