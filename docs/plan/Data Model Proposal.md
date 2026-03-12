# Data Model Proposal: Talent Evaluation & Matchmaking MVP

> **Version:** 1.1
> **Date:** February 2026
> **Purpose:** Define the complete data structure for normalized candidates, jobs, and their vector representations

**Related Documents:**
- [Skills Taxonomy System Proposal](./Skills%20Taxonomy%20System%20Proposal.md) — Canonical skills, rating, and role fitness scoring

**Implementation (SQLAlchemy Models):**
| Model | File | Description |
|-------|------|-------------|
| `RawCandidate`, `RawJob` | [`talent_matching/models/raw.py`](../../talent_matching/models/raw.py) | Raw input data before normalization |
| `NormalizedCandidate`, `CandidateSkill`, `CandidateExperience`, `CandidateProject`, `CandidateAttribute` | [`talent_matching/models/candidates.py`](../../talent_matching/models/candidates.py) | Smart Profile models |
| `NormalizedJob`, `JobRequiredSkill` | [`talent_matching/models/jobs.py`](../../talent_matching/models/jobs.py) | Normalized job requirements |
| `Skill`, `SkillAlias` | [`talent_matching/models/skills.py`](../../talent_matching/models/skills.py) | Skills taxonomy |
| `Match` | [`talent_matching/models/matches.py`](../../talent_matching/models/matches.py) | Candidate-job match results |
| Enums | [`talent_matching/models/enums.py`](../../talent_matching/models/enums.py) | All enum types |

---

## Table of Contents

1. [Current Input Data Analysis](#1-current-input-data-analysis)
2. [Candidate Data Model](#2-candidate-data-model)
3. [Job Data Model](#3-job-data-model)
4. [Vector Storage Design](#4-vector-storage-design)
5. [Database Schema (PostgreSQL + pgvector)](#5-database-schema-postgresql--pgvector)
6. [Data Quality & Transformation Notes](#6-data-quality--transformation-notes)

### Design Note: Summary Fields vs Detail Tables

The normalized candidate profile contains **pre-computed summary fields** (e.g., `years_of_experience`, `companies[]`) alongside detailed tables (e.g., `candidate_experiences`, `candidate_skills`). This is intentional **denormalization for query performance**:

- **Summary fields** → Fast filtering/sorting on thousands of candidates without joins
- **Detail tables** → Full breakdown for display, vectorization, and auditing

Example: To find "5+ years experience" candidates, query `WHERE years_of_experience >= 5` directly, rather than aggregating from the experiences table.

---

## 1. Current Input Data Analysis

### 1.1 Source Data Structure (Airtable Export)

The current candidate dataset contains **13 columns** from the Airtable form/export:

| Column | Data Type | Example | Completeness | Notes |
|--------|-----------|---------|--------------|-------|
| `Full Name` | string | "Mayank Rawat" | ✅ Required | Primary identifier |
| `Location` | string | "Malaysia", "Portugal", "France" | ✅ High | Country-level, sometimes city |
| `Desired Job Category` | string (comma-separated) | "Backend Developer,Protocol Engineer" | ✅ High | Multiple roles possible |
| `Skills` | string (comma-separated) | "Rust,TypeScript,Anchor,Solana" | ✅ High | Raw skill tags, varied taxonomy |
| `CV` | URL (Airtable attachment) | PDF link to `v5.airtableusercontent.com` | ⚠️ Variable | PDF/DOCX; contains work history |
| `Professional summary` | text (multi-paragraph) | Free-form self-description | ✅ High | Key for personality/experience vectors |
| `Proof of Work` | text (URLs/descriptions) | Links to projects, apps, repos | ⚠️ Variable | Some have multiple, some empty |
| `Salary Range` | string | "$91,000 - $125,000", "100k~120k" | ⚠️ Variable | Inconsistent format |
| `X Profile Link` | URL | `https://x.com/username` | ⚠️ Medium | Some have placeholder "NA" |
| `LinkedIn Profile` | URL | `https://linkedin.com/in/...` | ⚠️ Medium | Not always present |
| `Earn Profile` | URL | `https://earn.superteam.fun/t/...` | ⚠️ Low | Superteam Earn profile |
| `Git Hub Profile` | URL | `https://github.com/username` | ✅ High | Primary technical signal |
| `Work Experience` | text (multi-paragraph) | Extracted CV text | ✅ High | Contains full work history, education, skills |

### 1.2 Data Quality Observations

**High-Quality Signals Available:**
- **Rich professional summaries:** Candidates provide detailed self-descriptions (2-5 paragraphs) explaining their experience, focus areas, and work style → excellent for personality and domain vectors
- **Comprehensive work experience text:** Full CV content is available inline, including company names, roles, durations, technologies, and achievements
- **Strong GitHub presence:** Most tech candidates have GitHub profiles with active repos
- **Hackathon achievements:** Many candidates mention hackathon wins with prize amounts ($4K - $10K+)
- **Skills are tagged:** Pre-tagged skills list (though taxonomy is inconsistent)

**Data Quality Issues to Address:**
- **Salary format inconsistency:** "91,000 - $125,000", "100k~120k", "$180000+", "Between 60000 USD and 110000 USD"
- **Location granularity varies:** Some have city+country ("Lisbon, Portugal"), others just country ("Malaysia")
- **Social links incomplete:** Some profiles have "NA", empty, or placeholder URLs
- **Skills taxonomy is loose:** "Rust" vs "rust", "JavaScript" vs "Javascript", "TypeScript" vs "Typescript"
- **Multi-role candidates:** Single candidates may have 2-4 desired job categories (need to handle array)
- **Work experience is unstructured:** Free-form text needs LLM parsing for structured extraction

### 1.3 MVP Data Coverage Assessment

| Signal | Available in Current Data? | Source | Extraction Method |
|--------|---------------------------|--------|-------------------|
| **Years of experience** | ✅ Yes (in CV text) | Work Experience column | LLM extraction from role dates |
| **Skills & tech stack** | ✅ Yes | Skills column + CV text | Direct + LLM validation |
| **Current/past roles** | ✅ Yes | Work Experience | LLM extraction |
| **Company names** | ✅ Yes | Work Experience | LLM extraction |
| **Seniority level** | ⚠️ Inferred | Role titles in CV | LLM classification |
| **Location** | ✅ Yes | Location column | Direct (needs normalization) |
| **Salary expectations** | ✅ Yes | Salary Range column | Regex + LLM parsing |
| **GitHub profile** | ✅ Yes | Git Hub Profile column | GitHub API calls |
| **Hackathon wins** | ⚠️ Partial | CV text + Proof of Work | LLM extraction |
| **Professional summary** | ✅ Yes | Professional summary column | Direct |
| **Social profiles** | ⚠️ Partial | X, LinkedIn, Earn columns | Direct (when present) |
| **Domain experience** | ✅ Yes (in text) | Professional summary + CV | LLM inference |
| **Education** | ✅ Yes | Work Experience | LLM extraction |

---

## 2. Candidate Data Model

### 2.1 Raw Candidate Record

Stores the original input data exactly as received:

```sql
CREATE TABLE raw_candidates (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source              VARCHAR(50) NOT NULL,           -- 'airtable', 'form', 'referral'
    source_id           VARCHAR(255),                   -- External ID from source system

    -- Original fields from input
    full_name           TEXT NOT NULL,
    location_raw        TEXT,
    desired_job_categories_raw TEXT,                    -- Comma-separated string
    skills_raw          TEXT,                           -- Comma-separated string
    cv_url              TEXT,
    cv_text             TEXT,                           -- Extracted/parsed CV content
    professional_summary TEXT,
    proof_of_work       TEXT,
    salary_range_raw    TEXT,
    x_profile_url       TEXT,
    linkedin_url        TEXT,
    earn_profile_url    TEXT,
    github_url          TEXT,
    work_experience_raw TEXT,

    -- Metadata
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    -- Processing status
    processing_status   VARCHAR(20) DEFAULT 'pending',  -- pending, processing, completed, failed
    processing_error    TEXT
);
```

### 2.2 Normalized Candidate Profile

The LLM-normalized structured representation (the "Smart Profile"):

```sql
CREATE TABLE normalized_candidates (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_candidate_id        UUID NOT NULL REFERENCES raw_candidates(id),

    -- ═══════════════════════════════════════════════════════════════════
    -- IDENTITY & BASIC INFO
    -- ═══════════════════════════════════════════════════════════════════
    full_name               TEXT NOT NULL,
    email                   TEXT,                           -- Extracted from CV if present
    phone                   TEXT,                           -- Extracted from CV if present

    -- ═══════════════════════════════════════════════════════════════════
    -- LOCATION (normalized)
    -- ═══════════════════════════════════════════════════════════════════
    location_city           TEXT,                           -- e.g., "Lisbon", "Kuala Lumpur"
    location_country        TEXT,                           -- e.g., "Portugal", "Malaysia"
    location_region         TEXT,                           -- e.g., "Europe", "Asia-Pacific", "North America"
    timezone                TEXT,                           -- e.g., "UTC+0", "UTC+8"

    -- ═══════════════════════════════════════════════════════════════════
    -- PROFESSIONAL SUMMARY
    -- ═══════════════════════════════════════════════════════════════════
    professional_summary    TEXT,                           -- LLM-refined 2-3 sentence summary
    current_role            TEXT,                           -- e.g., "Senior Full-Stack Developer"
    seniority_level         VARCHAR(20),                    -- 'junior', 'mid', 'senior', 'lead', 'principal', 'executive'
    years_of_experience     INTEGER,                        -- Total years (computed from work history)

    -- ═══════════════════════════════════════════════════════════════════
    -- SKILLS (summary for fast filtering - detail in candidate_skills table)
    -- See: Skills Taxonomy System Proposal for full design
    -- ═══════════════════════════════════════════════════════════════════
    skills_all              TEXT[],                         -- All normalized skill names for filtering

    -- ═══════════════════════════════════════════════════════════════════
    -- DESIRED ROLES
    -- ═══════════════════════════════════════════════════════════════════
    desired_job_categories  TEXT[],                         -- ['Backend Developer', 'Protocol Engineer']

    -- ═══════════════════════════════════════════════════════════════════
    -- COMPENSATION
    -- ═══════════════════════════════════════════════════════════════════
    compensation_min        INTEGER,                        -- In USD
    compensation_max        INTEGER,                        -- In USD
    compensation_currency   VARCHAR(10) DEFAULT 'USD',

    -- ═══════════════════════════════════════════════════════════════════
    -- WORK HISTORY METRICS
    -- Note: Domain experience (Web3, Solana, etc.) is matched via vectors,
    -- not boolean flags. See domain_context_vector in candidate_vectors.
    -- ═══════════════════════════════════════════════════════════════════
    companies               TEXT[],                         -- ['Router Protocol', 'AWS', 'SAP']
    job_count               INTEGER,                        -- Number of positions held
    job_switches_count      INTEGER,                        -- Number of company changes
    average_tenure_months   INTEGER,                        -- Average time at each company
    longest_tenure_months   INTEGER,                        -- Longest single tenure

    -- ═══════════════════════════════════════════════════════════════════
    -- EDUCATION
    -- ═══════════════════════════════════════════════════════════════════
    education_highest_degree    TEXT,                       -- e.g., "Master's", "Bachelor's", "PhD"
    education_field             TEXT,                       -- e.g., "Computer Science", "Software Engineering"
    education_institution       TEXT,                       -- e.g., "MIT", "Stanford"

    -- ═══════════════════════════════════════════════════════════════════
    -- ACHIEVEMENTS & RECOGNITION
    -- ═══════════════════════════════════════════════════════════════════
    hackathon_wins_count        INTEGER DEFAULT 0,
    hackathon_total_prize_usd   INTEGER DEFAULT 0,
    solana_hackathon_wins       INTEGER DEFAULT 0,
    notable_achievements        TEXT[],                     -- Array of achievement descriptions

    -- ═══════════════════════════════════════════════════════════════════
    -- SOCIAL PRESENCE (for BD/Growth roles)
    -- ═══════════════════════════════════════════════════════════════════
    x_handle                TEXT,
    linkedin_handle         TEXT,
    github_handle           TEXT,
    social_followers_total  INTEGER DEFAULT 0,              -- Combined social following
    verified_communities    TEXT[],                         -- ['Superteam', 'Solana Foundation']

    -- ═══════════════════════════════════════════════════════════════════
    -- VERIFICATION & REVIEW
    -- ═══════════════════════════════════════════════════════════════════
    verification_status     VARCHAR(20) DEFAULT 'unverified', -- unverified, verified, suspicious, fraudulent
    verification_notes      TEXT,
    verified_by             UUID,                           -- Reviewer user ID
    verified_at             TIMESTAMPTZ,

    -- ═══════════════════════════════════════════════════════════════════
    -- PROCESSING METADATA
    -- ═══════════════════════════════════════════════════════════════════
    prompt_version          VARCHAR(50) NOT NULL,           -- Version of normalization prompt
    model_version           VARCHAR(50) NOT NULL,           -- LLM model used (e.g., 'gpt-4-turbo')
    normalized_at           TIMESTAMPTZ DEFAULT NOW(),
    confidence_score        FLOAT,                          -- LLM confidence in extraction (0-1)

    -- Full normalized JSON (for debugging/auditing)
    normalized_json         JSONB NOT NULL,

    CONSTRAINT fk_raw_candidate FOREIGN KEY (raw_candidate_id)
        REFERENCES raw_candidates(id) ON DELETE CASCADE
);

CREATE INDEX idx_candidates_skills ON normalized_candidates USING GIN (skills_all);
CREATE INDEX idx_candidates_seniority ON normalized_candidates(seniority_level);
CREATE INDEX idx_candidates_years_exp ON normalized_candidates(years_of_experience);
CREATE INDEX idx_candidates_verification ON normalized_candidates(verification_status);
CREATE INDEX idx_candidates_location ON normalized_candidates(location_country, location_region);
```

### 2.3 Candidate Skills (1-to-Many)

Each skill with its rating and years of experience (see [Skills Taxonomy System Proposal](./Skills%20Taxonomy%20System%20Proposal.md) for full design):

```sql
CREATE TABLE candidate_skills (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,
    skill_id            UUID NOT NULL REFERENCES skills(id) ON DELETE CASCADE,

    -- Rating & Experience
    rating              INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),  -- 1=beginner, 5=expert
    years_experience    INTEGER,                        -- Years using this skill (from CV)

    -- Context for vectorization
    notable_achievement TEXT,                           -- Best example of skill usage (for skill_vector)
    skill_vector        VECTOR(1536),                   -- Embedding of achievement

    -- Metadata
    rated_at            TIMESTAMPTZ DEFAULT NOW(),
    rating_model        VARCHAR(50),                    -- LLM model used for rating

    CONSTRAINT unique_candidate_skill UNIQUE (candidate_id, skill_id)
);

CREATE INDEX idx_candidate_skills_candidate ON candidate_skills(candidate_id);
CREATE INDEX idx_candidate_skills_skill ON candidate_skills(skill_id);
CREATE INDEX idx_candidate_skills_rating ON candidate_skills(rating);
CREATE INDEX idx_candidate_skill_vec ON candidate_skills
    USING hnsw (skill_vector vector_cosine_ops);
```

### 2.4 Candidate Work Experience (1-to-Many)

Structured work history for each position (used for position vectors):

```sql
CREATE TABLE candidate_experiences (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- Position details
    company_name        TEXT NOT NULL,
    position_title      TEXT NOT NULL,

    -- Duration
    start_date          DATE,
    end_date            DATE,                           -- NULL = current position
    years_experience    FLOAT,                          -- Computed from dates (e.g., 2.5)
    is_current          BOOLEAN DEFAULT FALSE,

    -- Description (for vectorization)
    description         TEXT,                           -- Summary of role/responsibilities

    -- Skills used in this role (denormalized for context/reference)
    skills_used         TEXT[],                         -- e.g., ['TypeScript', 'React', 'PostgreSQL']

    -- Vector for semantic matching
    position_vector     VECTOR(1536),                   -- Embedding of description

    -- Ordering
    position_order      INTEGER NOT NULL,               -- 1 = most recent

    -- Metadata
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_experiences_candidate ON candidate_experiences(candidate_id);
CREATE INDEX idx_experiences_company ON candidate_experiences(company_name);
CREATE INDEX idx_position_vec ON candidate_experiences
    USING hnsw (position_vector vector_cosine_ops);
```

### 2.5 GitHub Metrics (Simplified)

Basic metrics from GitHub API (quality signals deferred to Phase 2 with LLM-based repo analysis):

```sql
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
    languages               TEXT[],                     -- Top languages

    -- Fetch metadata
    fetched_at              TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_github_per_candidate UNIQUE (candidate_id)
);

-- NOTE: Quality signals (consistency_score, code quality, etc.) deferred to Phase 2
-- Will add candidate_github_analysis table with LLM-based repository analysis
```

### 2.6 Skills Taxonomy Tables

Canonical skills with aliases for normalization (see [Skills Taxonomy System Proposal](./Skills%20Taxonomy%20System%20Proposal.md)):

```sql
-- Canonical skills table (all skills treated as equals)
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


-- Skill aliases (variants mapping)
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
```

### 2.7 Candidate Projects & Hackathons

Individual projects, hackathons, and side projects (separate from employment history). This is one of the three key artifacts of any CV: **Skills, Jobs, and Projects**.

```sql
CREATE TABLE candidate_projects (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- Project details
    project_name        TEXT NOT NULL,
    description         TEXT,                           -- Summary for AI matching
    url                 TEXT,                           -- Link to project/repo
    technologies        TEXT[],                         -- Technologies used

    -- Hackathon-specific fields
    is_hackathon        BOOLEAN DEFAULT FALSE,
    hackathon_name      TEXT,                           -- e.g., "Solana Grizzlython"
    prize_won           TEXT,                           -- e.g., "1st Place", "Best DeFi"
    prize_amount_usd    INTEGER,

    -- Timeline
    year                INTEGER,                        -- Year of project
    start_date          DATE,
    end_date            DATE,

    -- Ordering
    project_order       INTEGER DEFAULT 1,              -- 1 = most important/recent

    -- Vector for semantic matching (Phase 2)
    project_vector      VECTOR(1536),                   -- Embedding of description

    -- Metadata
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_projects_candidate ON candidate_projects(candidate_id);
CREATE INDEX idx_projects_hackathon ON candidate_projects(is_hackathon);
CREATE INDEX idx_project_vec ON candidate_projects
    USING hnsw (project_vector vector_cosine_ops);
```

**Why Projects Matter:**

| Candidate Type | What Projects Show |
|----------------|-------------------|
| **Tech** | Side projects, open source contributions, hackathon wins |
| **Non-Tech** | Marketing campaigns, growth experiments, content portfolios |
| **Mixed** | Personal brands, community initiatives, startup ventures |

Projects are especially valuable for:
- Hackathon achievements (with prizes and recognition)
- Self-directed work demonstrating autonomy
- Technical depth beyond day jobs
- Non-tech candidates' portfolios (campaigns, content, etc.)

### 2.8 Role Fitness Scores

Score each candidate's fitness for their desired roles:

```sql
CREATE TABLE candidate_role_fitness (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- Role
    role_name       TEXT NOT NULL,                  -- "Backend Developer", "Protocol Engineer"

    -- Score
    fitness_score   FLOAT NOT NULL,                 -- 0.0 - 1.0
    score_breakdown JSONB,                          -- {"core_skills": {...}, "bonus_skills": {...}}

    -- Metadata
    computed_at     TIMESTAMPTZ DEFAULT NOW(),
    algorithm_version VARCHAR(50),

    CONSTRAINT unique_candidate_role UNIQUE (candidate_id, role_name)
);

CREATE INDEX idx_role_fitness_candidate ON candidate_role_fitness(candidate_id);
CREATE INDEX idx_role_fitness_score ON candidate_role_fitness(role_name, fitness_score DESC);
```

### 2.9 Universal Soft Attributes

LLM-scored universal attributes that apply across all job types. These replace job-specific boolean flags with flexible scores:

```sql
CREATE TABLE candidate_attributes (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id            UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- ═══════════════════════════════════════════════════════════════════
    -- UNIVERSAL SOFT SCORES (1-5, LLM-rated from CV)
    -- ═══════════════════════════════════════════════════════════════════

    -- Leadership: Has led teams/projects, made key decisions
    -- 1=IC only, 3=led small projects, 5=led teams/orgs
    leadership_score        INTEGER CHECK (leadership_score BETWEEN 1 AND 5),

    -- Autonomy: Works independently, self-directed
    -- 1=needs guidance, 3=independent on tasks, 5=founder-type self-starter
    autonomy_score          INTEGER CHECK (autonomy_score BETWEEN 1 AND 5),

    -- Technical Depth: Systems thinking, architecture skills
    -- 1=surface-level, 3=solid practitioner, 5=architect/systems thinker
    technical_depth_score   INTEGER CHECK (technical_depth_score BETWEEN 1 AND 5),

    -- Communication: Collaboration, documentation, public presence
    -- 1=minimal evidence, 3=collaborates well, 5=excellent writer/speaker
    communication_score     INTEGER CHECK (communication_score BETWEEN 1 AND 5),

    -- Growth Trajectory: Career progression, learning velocity
    -- 1=static career, 3=steady growth, 5=rapid advancement
    growth_trajectory_score INTEGER CHECK (growth_trajectory_score BETWEEN 1 AND 5),

    -- ═══════════════════════════════════════════════════════════════════
    -- REASONING (for transparency/debugging)
    -- ═══════════════════════════════════════════════════════════════════
    reasoning               JSONB,                  -- LLM reasoning for each score

    -- ═══════════════════════════════════════════════════════════════════
    -- METADATA
    -- ═══════════════════════════════════════════════════════════════════
    rated_at                TIMESTAMPTZ DEFAULT NOW(),
    rating_model            VARCHAR(50),            -- LLM model used
    prompt_version          VARCHAR(50),

    CONSTRAINT unique_attributes_per_candidate UNIQUE (candidate_id)
);

CREATE INDEX idx_attributes_leadership ON candidate_attributes(leadership_score);
CREATE INDEX idx_attributes_autonomy ON candidate_attributes(autonomy_score);
CREATE INDEX idx_attributes_technical ON candidate_attributes(technical_depth_score);
```

**Why These 5 Scores?**

| Score | What It Measures | CV Signals |
|-------|-----------------|------------|
| **Leadership** | Team/project leadership, decision-making | "Led team of 10", "Architected", "Managed", "Mentored" |
| **Autonomy** | Independent work, self-direction | Founder roles, solo projects, remote success, proactive |
| **Technical Depth** | Systems thinking, complexity handling | Infrastructure, performance optimization, core systems |
| **Communication** | Collaboration, writing, speaking | Documentation, blog posts, talks, DevRel, cross-team work |
| **Growth Trajectory** | Career progression, learning speed | Promotions, expanding scope, new domains, rapid advancement |

These are **universal** — they apply to frontend, backend, DevRel, growth, and any other role type. Jobs specify minimum thresholds rather than requiring new flags for each job type.

---

## 3. Job Data Model

### 3.1 Raw Job Record

```sql
CREATE TABLE raw_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source              VARCHAR(50) NOT NULL,           -- 'manual', 'api', 'partner', 'paste'
    source_id           VARCHAR(255),
    source_url          TEXT,

    -- ═══════════════════════════════════════════════════════════════════
    -- CORE FIELDS
    -- For unstructured input: only job_description is required
    -- For structured input: fields come pre-parsed from Notion/Airtable
    -- ═══════════════════════════════════════════════════════════════════
    job_title           TEXT,                           -- Can be extracted by LLM if not provided
    company_name        TEXT,
    job_description     TEXT NOT NULL,                  -- Raw job posting text (always required)
    company_website_url TEXT,                           -- Company website for additional context

    -- ═══════════════════════════════════════════════════════════════════
    -- STRUCTURED FIELDS (from Notion Job Board, optional)
    -- ═══════════════════════════════════════════════════════════════════
    experience_level_raw TEXT,                          -- "Junior", "Mid", "Senior"
    location_raw        TEXT,                           -- "Europe", "Global", etc.
    work_setup_raw      TEXT,                           -- "Remote", "Hybrid", "On-Site"
    status_raw          TEXT,                           -- "Active", "Closed"
    job_category_raw    TEXT,                           -- "Frontend Engineer", etc.

    -- Metadata
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    processing_status   VARCHAR(20) DEFAULT 'pending',
    processing_error    TEXT
);
```

**Two Ingestion Modes:**

| Mode | Input | What LLM Extracts |
|------|-------|-------------------|
| **Unstructured** | `job_description` only (pasted job posting) | Title, company, skills, requirements, salary, location, everything |
| **Structured** | `job_description` + pre-parsed fields from Notion/Airtable | Normalizes and validates existing fields |

`job_description` is always required. This flexibility allows ingesting jobs from any source - whether it's a copy-pasted LinkedIn posting or a structured Notion database.

### 3.2 Normalized Job Profile

```sql
CREATE TABLE normalized_jobs (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_job_id              UUID NOT NULL REFERENCES raw_jobs(id),

    -- ═══════════════════════════════════════════════════════════════════
    -- BASIC INFO
    -- ═══════════════════════════════════════════════════════════════════
    title                   TEXT NOT NULL,
    company_name            TEXT,
    company_stage           VARCHAR(30),                -- 'startup', 'scaleup', 'enterprise', 'dao'

    -- ═══════════════════════════════════════════════════════════════════
    -- ROLE CLASSIFICATION
    -- ═══════════════════════════════════════════════════════════════════
    seniority_level         VARCHAR(20),                -- 'junior', 'mid', 'senior', 'lead', 'principal'
    employment_type         VARCHAR(20),                -- 'full-time', 'part-time', 'contract'
    role_type               VARCHAR(30),                -- 'engineering', 'devrel', 'growth', 'product', 'design'

    -- ═══════════════════════════════════════════════════════════════════
    -- REQUIREMENTS
    -- ═══════════════════════════════════════════════════════════════════
    must_have_skills        TEXT[],                     -- Hard requirements
    nice_to_have_skills     TEXT[],                     -- Preferred but optional
    years_experience_min    INTEGER,
    years_experience_max    INTEGER,                    -- NULL if no max
    education_required      TEXT,                       -- e.g., "Bachelor's in CS or equivalent"
    domain_experience       TEXT[],                     -- e.g., ['DeFi', 'Trading', 'Infrastructure']
    tech_stack              TEXT[],                     -- Specific technologies

    -- ═══════════════════════════════════════════════════════════════════
    -- ROLE DESCRIPTION
    -- ═══════════════════════════════════════════════════════════════════
    role_summary            TEXT,                       -- 2-3 sentence summary
    responsibilities        TEXT[],                     -- Key responsibilities
    team_context            TEXT,                       -- Team size, reporting structure

    -- ═══════════════════════════════════════════════════════════════════
    -- COMPENSATION
    -- ═══════════════════════════════════════════════════════════════════
    salary_min              INTEGER,                    -- In USD
    salary_max              INTEGER,
    salary_currency         VARCHAR(10) DEFAULT 'USD',
    has_equity              BOOLEAN DEFAULT FALSE,
    has_token_compensation  BOOLEAN DEFAULT FALSE,

    -- ═══════════════════════════════════════════════════════════════════
    -- LOCATION
    -- ═══════════════════════════════════════════════════════════════════
    location_type           VARCHAR(20),                -- 'remote', 'hybrid', 'onsite'
    location_countries      TEXT[],                     -- Allowed countries
    timezone_requirements   TEXT,                       -- e.g., "UTC-5 to UTC+3"

    -- ═══════════════════════════════════════════════════════════════════
    -- SOFT ATTRIBUTE REQUIREMENTS (minimum scores, NULL = not required)
    -- LLM-inferred from job description signals
    -- ═══════════════════════════════════════════════════════════════════
    min_leadership_score        INTEGER CHECK (min_leadership_score BETWEEN 1 AND 5),
    min_autonomy_score          INTEGER CHECK (min_autonomy_score BETWEEN 1 AND 5),
    min_technical_depth_score   INTEGER CHECK (min_technical_depth_score BETWEEN 1 AND 5),
    min_communication_score     INTEGER CHECK (min_communication_score BETWEEN 1 AND 5),
    min_growth_trajectory_score INTEGER CHECK (min_growth_trajectory_score BETWEEN 1 AND 5),

    -- ═══════════════════════════════════════════════════════════════════
    -- STATUS
    -- ═══════════════════════════════════════════════════════════════════
    status                  VARCHAR(20) DEFAULT 'active', -- 'active', 'paused', 'filled', 'closed'
    priority                INTEGER DEFAULT 3,          -- 1=urgent, 2=high, 3=normal, 4=low

    -- ═══════════════════════════════════════════════════════════════════
    -- PROCESSING METADATA
    -- ═══════════════════════════════════════════════════════════════════
    prompt_version          VARCHAR(50) NOT NULL,
    model_version           VARCHAR(50) NOT NULL,
    normalized_at           TIMESTAMPTZ DEFAULT NOW(),
    normalized_json         JSONB NOT NULL,

    CONSTRAINT fk_raw_job FOREIGN KEY (raw_job_id)
        REFERENCES raw_jobs(id) ON DELETE CASCADE
);

CREATE INDEX idx_jobs_skills ON normalized_jobs USING GIN (must_have_skills);
CREATE INDEX idx_jobs_seniority ON normalized_jobs(seniority_level);
CREATE INDEX idx_jobs_status ON normalized_jobs(status);
CREATE INDEX idx_jobs_location_type ON normalized_jobs(location_type);
```

---

## 4. Vector Storage Design

### 4.1 Candidate Vectors

Semantic embeddings for candidate matching:

```sql
CREATE TABLE candidate_vectors (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id            UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- ═══════════════════════════════════════════════════════════════════
    -- CORE VECTORS (Phase 1 MVP)
    -- ═══════════════════════════════════════════════════════════════════

    -- Overall experience vector (all roles concatenated)
    experience_vector       VECTOR(1536),               -- "Who has similar overall work history?"

    -- Domain/industry context
    domain_context_vector   VECTOR(1536),               -- "Who has worked in similar problem spaces?"

    -- Personality/work style
    personality_vector      VECTOR(1536),               -- "Who would be a good culture fit?"

    -- Impact/ownership signals
    project_impact_vector   VECTOR(1536),               -- "Who has owned significant scope?"

    -- ═══════════════════════════════════════════════════════════════════
    -- ROLE-SPECIFIC VECTORS (Phase 1 MVP)
    -- ═══════════════════════════════════════════════════════════════════

    -- Technical depth (for engineering roles)
    technical_depth_vector  VECTOR(1536),               -- "Who thinks in systems, not just features?"

    -- Social presence (for BD/Growth roles)
    social_presence_vector  VECTOR(1536),               -- "Who has relevant social proof?"

    -- Community involvement
    community_narrative_vector VECTOR(1536),            -- "Who is well-connected in DeFi communities?"

    -- ═══════════════════════════════════════════════════════════════════
    -- METADATA
    -- ═══════════════════════════════════════════════════════════════════
    embedding_model         VARCHAR(50) NOT NULL,       -- e.g., 'text-embedding-3-large'
    model_version           VARCHAR(50) NOT NULL,
    created_at              TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_vectors_per_candidate UNIQUE (candidate_id)
);

-- HNSW indexes for fast similarity search
CREATE INDEX idx_candidate_experience_vec ON candidate_vectors
    USING hnsw (experience_vector vector_cosine_ops);
CREATE INDEX idx_candidate_domain_vec ON candidate_vectors
    USING hnsw (domain_context_vector vector_cosine_ops);
CREATE INDEX idx_candidate_personality_vec ON candidate_vectors
    USING hnsw (personality_vector vector_cosine_ops);
```

### 4.2 Position Vectors

Position vectors are stored **directly in the `candidate_experiences` table** (see Section 2.4) rather than in a separate table. This simplifies the schema:

- Each `candidate_experiences` row has a `position_vector` column
- Vector is the embedding of the role description
- Enables "Who has done this exact job before?" queries

**Note:** Skill vectors are stored in `candidate_skills.skill_vector` (see Section 2.3).

### 4.3 Job Vectors

```sql
CREATE TABLE job_vectors (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                  UUID NOT NULL REFERENCES normalized_jobs(id) ON DELETE CASCADE,

    -- Core job vectors
    role_description_vector VECTOR(1536),               -- Match against candidate positions/experience
    domain_context_vector   VECTOR(1536),               -- Match against candidate domain expertise
    culture_vector          VECTOR(1536),               -- Match against candidate personality

    -- Metadata
    embedding_model         VARCHAR(50) NOT NULL,
    model_version           VARCHAR(50) NOT NULL,
    created_at              TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_vectors_per_job UNIQUE (job_id)
);

CREATE INDEX idx_job_role_vec ON job_vectors
    USING hnsw (role_description_vector vector_cosine_ops);
CREATE INDEX idx_job_domain_vec ON job_vectors
    USING hnsw (domain_context_vector vector_cosine_ops);
CREATE INDEX idx_job_culture_vec ON job_vectors
    USING hnsw (culture_vector vector_cosine_ops);
```

---

## 5. Database Schema (PostgreSQL + pgvector)

### 5.1 Matches Table

Stores computed match results:

```sql
CREATE TABLE matches (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                  UUID NOT NULL REFERENCES normalized_jobs(id) ON DELETE CASCADE,
    candidate_id            UUID NOT NULL REFERENCES normalized_candidates(id) ON DELETE CASCADE,

    -- ═══════════════════════════════════════════════════════════════════
    -- SCORES
    -- ═══════════════════════════════════════════════════════════════════

    -- Final combined score (0-1)
    match_score             FLOAT NOT NULL,

    -- Score components
    keyword_score           FLOAT,                      -- Keyword matching component (0-1)
    vector_score            FLOAT,                      -- Vector similarity component (0-1)

    -- Detailed breakdown
    score_breakdown         JSONB,                      -- Full breakdown for explainability
    /*
    Example:
    {
        "keyword": {
            "must_have_coverage": 0.85,
            "nice_to_have_coverage": 0.60,
            "matched_skills": ["Rust", "TypeScript", "Solana"],
            "missing_skills": ["Go"]
        },
        "vector": {
            "best_position_similarity": 0.89,
            "experience_similarity": 0.78,
            "domain_similarity": 0.92,
            "culture_similarity": 0.71
        }
    }
    */

    -- ═══════════════════════════════════════════════════════════════════
    -- STATUS
    -- ═══════════════════════════════════════════════════════════════════
    rank                    INTEGER,                    -- Position in shortlist (1 = best)
    status                  VARCHAR(20) DEFAULT 'matched', -- matched, reviewed, contacted, rejected, hired

    -- Review tracking
    reviewed_by             UUID,
    reviewed_at             TIMESTAMPTZ,
    review_notes            TEXT,

    -- ═══════════════════════════════════════════════════════════════════
    -- METADATA
    -- ═══════════════════════════════════════════════════════════════════
    algorithm_version       VARCHAR(50) NOT NULL,       -- Version of matching algorithm
    created_at              TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_job_candidate_match UNIQUE (job_id, candidate_id)
);

CREATE INDEX idx_matches_job ON matches(job_id);
CREATE INDEX idx_matches_candidate ON matches(candidate_id);
CREATE INDEX idx_matches_score ON matches(job_id, match_score DESC);
CREATE INDEX idx_matches_status ON matches(status);
```

### 5.2 Processing Audit Log

Track all transformations for debugging and reprocessing:

```sql
CREATE TABLE processing_log (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- What was processed
    entity_type         VARCHAR(20) NOT NULL,           -- 'candidate', 'job', 'match'
    entity_id           UUID NOT NULL,

    -- Processing step
    step_name           VARCHAR(50) NOT NULL,           -- 'ingest', 'normalize', 'vectorize', 'match'
    step_status         VARCHAR(20) NOT NULL,           -- 'started', 'completed', 'failed'

    -- Versioning
    prompt_version      VARCHAR(50),
    model_version       VARCHAR(50),
    algorithm_version   VARCHAR(50),

    -- Timing
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    duration_ms         INTEGER,

    -- Cost tracking
    tokens_input        INTEGER,
    tokens_output       INTEGER,
    estimated_cost_usd  FLOAT,

    -- Errors
    error_message       TEXT,
    error_details       JSONB,

    -- Output reference
    output_data         JSONB                           -- Optional: store intermediate output
);

CREATE INDEX idx_processing_log_entity ON processing_log(entity_type, entity_id);
CREATE INDEX idx_processing_log_step ON processing_log(step_name, step_status);
CREATE INDEX idx_processing_log_time ON processing_log(started_at DESC);
```

---

## 6. Data Quality & Transformation Notes

### 6.1 LLM Prompts Required

| Step | Input | Output | Key Extractions |
|------|-------|--------|-----------------|
| **Candidate Normalization** | Raw CV text + Professional Summary | Structured JSON profile | years_of_experience, seniority_level, companies, education |
| **Experience Extraction** | Work Experience text | Array of structured positions | company, position, years, description |
| **Skill Resolution** | Unknown skill + existing skills list | Map to existing OR create new | See [Skills Taxonomy](./Skills%20Taxonomy%20System%20Proposal.md) |
| **Skill Rating** | CV text + normalized skills list | JSON with rating (1-5), years, achievement per skill | `{"TypeScript": {"rating": 5, "years": 5, "notable": "..."}}` |
| **Universal Attributes** | CV text | 5 soft scores (1-5) with reasoning | leadership, autonomy, technical_depth, communication, growth_trajectory |
| **Salary Parsing** | Raw salary string | min/max integers in USD | Handle formats: "$91K-$125K", "100k~120k" |
| **Location Normalization** | Raw location string | city, country, region, timezone | "Malaysia" → ("Kuala Lumpur", "Malaysia", "Asia-Pacific") |
| **Job Normalization** | Raw job posting text OR structured fields | Structured job JSON | title, company, must_have/nice_to_have skills, seniority, min attribute scores |

See [Skills Taxonomy System Proposal](./Skills%20Taxonomy%20System%20Proposal.md) for full prompt templates.

### 6.2 Vector Generation Strategy

**Candidate Vectors:**

| Vector | Location | Text to Embed | Purpose |
|--------|----------|---------------|---------|
| `experience_vector` | `candidate_vectors` | Concatenated descriptions of all past roles | Overall career similarity |
| `domain_context_vector` | `candidate_vectors` | LLM-generated narrative of industries/domains | "DeFi" vs "NFT" differentiation |
| `personality_vector` | `candidate_vectors` | Professional summary + work style signals | Culture/team fit |
| `position_vector` | `candidate_experiences` | Individual role description | "Has done this exact job before?" |
| `skill_vector` | `candidate_skills` | Notable achievement for skill | "Who has deep TypeScript expertise?" |

**Job Vectors:**

| Vector | Location | Text to Embed | Purpose |
|--------|----------|---------------|---------|
| `role_description_vector` | `job_vectors` | Full job responsibilities | Match against candidate positions |
| `domain_context_vector` | `job_vectors` | Product/industry context | Match against candidate domains |
| `culture_vector` | `job_vectors` | LLM-inferred culture from JD | Match against personality |

### 6.3 Skills Taxonomy Normalization

Common mappings needed:

| Raw Variant | Normalized Value |
|-------------|------------------|
| "TypeScript", "Typescript", "TS" | "TypeScript" |
| "JavaScript", "Javascript", "JS" | "JavaScript" |
| "Rust", "rust" | "Rust" |
| "React.js", "ReactJS", "React" | "React" |
| "Next.js", "NextJS", "Nextjs" | "Next.js" |
| "Postgres", "PostgreSQL", "psql" | "PostgreSQL" |
| "K8s", "Kubernetes" | "Kubernetes" |

### 6.4 Seniority Classification Rules

| Signal | Junior | Mid | Senior | Lead | Principal |
|--------|--------|-----|--------|------|-----------|
| Years of experience | 0-2 | 2-5 | 5-8 | 8-12 | 12+ |
| Title keywords | "Junior", "Associate", "Intern" | "Developer", "Engineer" | "Senior", "Staff" | "Lead", "Tech Lead", "Manager" | "Principal", "Distinguished", "CTO" |
| Team responsibility | Individual contributor | IC | IC + mentoring | Leads small team | Org-level impact |

---

## Appendix: Example Candidate Data

### A.1 Normalized Candidate Profile

```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "full_name": "Mayank Rawat",
  "professional_summary": "Full-stack engineer with 5+ years building production Web3 applications. Expert in blockchain explorers, trading terminals, and DeFi interfaces with focus on performance and UX.",
  "current_role": "Senior Software Developer",
  "seniority_level": "senior",
  "years_of_experience": 5,

  "location": {
    "city": "Kuala Lumpur",
    "country": "Malaysia",
    "region": "Asia-Pacific",
    "timezone": "UTC+8"
  },

  "skills_all": ["TypeScript", "JavaScript", "Rust", "React", "Next.js", "Node.js", "Solana", "DeFi"],

  "desired_job_categories": ["Full-Stack Developer", "Frontend Developer"],

  "compensation": {
    "min": 91000,
    "max": 125000,
    "currency": "USD"
  },

  "companies": ["Blinq", "Router Protocol"]
}
```

### A.2 Candidate Skills (with Ratings)

```json
{
  "skills": [
    {
      "skill_name": "TypeScript",
      "rating": 5,
      "years_experience": 5,
      "notable_achievement": "Built high-performance trading UI handling 50K daily users"
    },
    {
      "skill_name": "React",
      "rating": 5,
      "years_experience": 5,
      "notable_achievement": "Led frontend architecture for multi-chain blockchain explorer"
    },
    {
      "skill_name": "Rust",
      "rating": 3,
      "years_experience": 2,
      "notable_achievement": "Developed cross-chain bridge wrapper contracts"
    },
    {
      "skill_name": "Solana",
      "rating": 4,
      "years_experience": 3,
      "notable_achievement": "Architected DEX supporting 3K+ daily active users"
    },
    {
      "skill_name": "Node.js",
      "rating": 4,
      "years_experience": 5,
      "notable_achievement": null
    }
  ]
}
```

### A.3 Candidate Work Experience

```json
{
  "experiences": [
    {
      "company_name": "Blinq",
      "position_title": "Senior Software Developer",
      "years_experience": 0.5,
      "is_current": true,
      "description": "High-performance trading UI with Zustand state management, React Query for caching, WebSocket streams for real-time data. Reduced redundant network calls by 40-60%."
    },
    {
      "company_name": "Router Protocol",
      "position_title": "Senior Software Developer",
      "years_experience": 4.25,
      "is_current": false,
      "description": "Cross-chain bridge and DeFi infrastructure. Built explorer indexing 50+ chains, DEX UI supporting 3K+ daily users, staking platform with $5M+ TVL."
    }
  ]
}
```

### A.4 Universal Soft Attributes

```json
{
  "attributes": {
    "leadership_score": 4,
    "autonomy_score": 4,
    "technical_depth_score": 5,
    "communication_score": 3,
    "growth_trajectory_score": 4,
    "reasoning": {
      "leadership": "Led frontend architecture decisions, mentored developers, drove technical strategy at Router Protocol",
      "autonomy": "Built complex multi-chain systems independently, self-directed in choosing technical approaches",
      "technical_depth": "Deep expertise in performance optimization, real-time systems, cross-chain architecture",
      "communication": "Documented SDKs with 1K+ weekly downloads, but limited public speaking/writing evidence",
      "growth_trajectory": "Progressed to Senior quickly, expanded from frontend to full-stack to multi-chain"
    }
  }
}
```

### A.5 Role Fitness Scores

```json
{
  "role_fitness": [
    {
      "role_name": "Full-Stack Developer",
      "fitness_score": 0.89,
      "score_breakdown": {
        "core_skills_avg": 0.92,
        "bonus_skills_avg": 0.80
      }
    },
    {
      "role_name": "Frontend Developer",
      "fitness_score": 0.94,
      "score_breakdown": {
        "core_skills_avg": 0.96,
        "bonus_skills_avg": 0.88
      }
    }
  ]
}
```

### A.6 Candidate Projects & Hackathons

```json
{
  "projects": [
    {
      "project_name": "SolanaSwap DEX",
      "description": "Built a decentralized exchange on Solana with AMM, limit orders, and real-time price charts. Handled 5K+ transactions during hackathon demo.",
      "url": "https://github.com/candidate/solana-swap",
      "technologies": ["Rust", "Anchor", "React", "TypeScript"],
      "is_hackathon": true,
      "hackathon_name": "Solana Grizzlython 2023",
      "prize_won": "1st Place DeFi Track",
      "prize_amount_usd": 10000,
      "year": 2023
    },
    {
      "project_name": "Multi-chain Portfolio Tracker",
      "description": "Personal project tracking assets across 10+ chains with real-time prices, PnL calculations, and tax reporting.",
      "url": "https://portfolio.example.com",
      "technologies": ["Next.js", "TypeScript", "GraphQL"],
      "is_hackathon": false,
      "year": 2024
    },
    {
      "project_name": "Open Source SDK Contribution",
      "description": "Major contributor to Router Protocol SDK, added support for 15 new chains and improved documentation.",
      "url": "https://github.com/router-protocol/sdk",
      "technologies": ["TypeScript", "Node.js"],
      "is_hackathon": false
    }
  ]
}
```

### A.7 Example Job (Normalized with Soft Requirements)

```json
{
  "title": "Senior Frontend Engineer",
  "company_name": "Trojan Trading",
  "seniority_level": "senior",

  "must_have_skills": ["React", "Next.js", "TypeScript", "WebSocket"],
  "nice_to_have_skills": ["Solana", "DeFi", "CI/CD"],
  "years_experience_min": 5,
  "domain_experience": ["Trading Platforms", "DeFi", "Crypto"],

  "min_leadership_score": 4,
  "min_autonomy_score": 4,
  "min_technical_depth_score": 4,
  "min_communication_score": 3,
  "min_growth_trajectory_score": null,

  "salary_min": 100000,
  "salary_max": 350000,
  "location_type": "remote"
}
```

**Note:** After discovery calls with clients, the normalized job fields above can be directly edited to incorporate additional insights. For example:
- Add companies to target in `domain_experience`
- Add specific skills mentioned in `must_have_skills` or `nice_to_have_skills`
- Adjust `min_*_score` thresholds based on role requirements

---

## Next Steps

1. **Seed skills taxonomy** - Initialize canonical skills from dataset (see [Skills Taxonomy System Proposal](./Skills%20Taxonomy%20System%20Proposal.md))
2. **Implement skill rating pipeline** - LLM prompt to rate skills 1-5 with years extraction
3. **Implement universal attributes scoring** - LLM prompt to score 5 soft attributes per candidate
4. **Implement schema** - Create PostgreSQL migration scripts with pgvector
5. **Build experience extraction** - Parse work history, generate position vectors
6. **Implement role fitness scoring** - Rule-based scoring from skill ratings
7. **GitHub API integration** - Basic metrics (repos, stars, languages)
8. **Test with sample data** - Process 10-20 candidates to validate the pipeline
