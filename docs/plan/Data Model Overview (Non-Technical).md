# Data Model Overview: Talent Matching MVP

> **For:** Product, Recruiting, and Business Team Members
> **Date:** February 2026

---

## What This Document Covers

This document explains **what information we store** about candidates and jobs, and **how we use it to find matches**. No coding knowledge required!

---

## Table of Contents

1. [The Big Picture](#1-the-big-picture)
2. [Candidate Profile](#2-candidate-profile)
3. [Skills & Ratings](#3-skills--ratings)
4. [Work Experience](#4-work-experience)
5. [Projects & Hackathons](#5-projects--hackathons)
6. [Soft Attributes](#6-soft-attributes)
7. [Job Requirements](#7-job-requirements)
8. [Semantic Vectors (AI Matching)](#8-semantic-vectors-ai-matching)
9. [How Matching Works](#9-how-matching-works)

---

## 1. The Big Picture

We store information about candidates and jobs, then use a combination of **exact filters** and **AI-powered similarity** to find the best matches.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CANDIDATE DATA                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Profile    │  │    Skills    │  │  Experience  │              │
│  │  (basics)    │  │  (rated 1-5) │  │  (history)   │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Projects   │  │   Soft       │  │   GitHub     │              │
│  │ (hackathons) │  │  Attributes  │  │   Metrics    │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
                    [ MATCHING ENGINE ]
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                           JOB DATA                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ Requirements │  │    Skills    │  │    Soft      │              │
│  │   (basics)   │  │   (needed)   │  │  Minimums    │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Candidate Profile

### Basic Information

What we know about each candidate at a glance:

| Field | Description | Example |
|-------|-------------|---------|
| **Name** | Full name | "Mayank Rawat" |
| **Email** | Contact email | "mayank@example.com" |
| **Location** | Where they're based | City: "Kuala Lumpur", Country: "Malaysia", Region: "Asia-Pacific" |
| **Current Role** | Their current job title | "Senior Software Developer" |
| **Seniority Level** | Career level | Junior / Mid / Senior / Lead / Principal |
| **Years of Experience** | Total years in the field | 5 years |
| **Desired Roles** | What roles they're looking for | ["Full-Stack Developer", "Frontend Developer"] |
| **Salary Expectations** | Compensation range | Min: $91,000 / Max: $125,000 |

### Verification Status

| Status | Meaning |
|--------|---------|
| **Unverified** | Not yet reviewed by a human |
| **Verified** | Human confirmed their claims look valid |
| **Suspicious** | Something seems off, needs investigation |
| **Fraudulent** | Confirmed fake information |

---

## 3. Skills & Ratings

For each skill a candidate has, we store:

| Field | Description | Example |
|-------|-------------|---------|
| **Skill Name** | The normalized skill name | "TypeScript" |
| **Rating** | Proficiency level (1-5) | 5 (Expert) |
| **Years Experience** | How long they've used it | 5 years |
| **Notable Achievement** | Best example of using this skill | "Built trading UI handling 50K daily users" |

### Rating Scale

| Rating | Level | What It Means |
|--------|-------|---------------|
| 1 | Beginner | Basic understanding, limited practice |
| 2 | Elementary | Can do simple tasks with guidance |
| 3 | Intermediate | Comfortable with common use cases |
| 4 | Advanced | Deep expertise, can architect solutions |
| 5 | Expert | Industry-recognized, exceptional depth |

### Example: Candidate's Skills

| Skill | Rating | Years | Notable Achievement |
|-------|--------|-------|---------------------|
| TypeScript | ⭐⭐⭐⭐⭐ (5) | 5 | Built high-performance trading UI |
| React | ⭐⭐⭐⭐⭐ (5) | 5 | Led frontend architecture for explorer |
| Rust | ⭐⭐⭐ (3) | 2 | Cross-chain bridge wrapper contracts |
| Solana | ⭐⭐⭐⭐ (4) | 3 | Architected DEX with 3K+ daily users |
| Node.js | ⭐⭐⭐⭐ (4) | 5 | — |

---

## 4. Work Experience

For each position in their career:

| Field | Description | Example |
|-------|-------------|---------|
| **Company Name** | Where they worked | "Router Protocol" |
| **Position Title** | Their job title | "Senior Software Developer" |
| **Years** | Time in this role | 4.25 years |
| **Is Current** | Still working there? | No |
| **Description** | What they did (for AI matching) | "Cross-chain bridge and DeFi infrastructure. Built explorer indexing 50+ chains..." |
| **Skills Used** | Technologies used in this role | ["TypeScript", "React", "PostgreSQL"] |

### Example: Candidate's Work History

| # | Company | Position | Years | Skills Used |
|---|---------|----------|-------|-------------|
| 1 | Blinq | Senior Software Developer | 0.5 | TypeScript, React, Solana |
| 2 | Router Protocol | Senior Software Developer | 4.25 | TypeScript, Node.js, PostgreSQL |

---

## 5. Projects & Hackathons

Beyond jobs, we also track individual **projects** and **hackathon wins**. This is one of the three key artifacts of any CV: **Skills, Jobs, and Projects**.

### Why Projects Matter

| Candidate Type | What Projects Show |
|----------------|-------------------|
| **Tech** | Side projects, open source contributions, hackathon wins |
| **Non-Tech** | Marketing campaigns, growth experiments, content portfolios |
| **All** | Self-directed work, autonomy, passion outside day job |

### What We Store Per Project

| Field | Description | Example |
|-------|-------------|---------|
| **Project Name** | Name of the project | "SolanaSwap DEX" |
| **Description** | What they built | "Decentralized exchange with AMM..." |
| **URL** | Link to project/repo | github.com/user/project |
| **Technologies** | Tech stack used | ["Rust", "Anchor", "React"] |
| **Is Hackathon?** | Was this a hackathon project? | Yes |
| **Hackathon Name** | Name of the hackathon | "Solana Grizzlython 2023" |
| **Prize Won** | What prize they received | "1st Place DeFi Track" |
| **Prize Amount** | Prize money in USD | $10,000 |
| **Year** | When it was built | 2023 |

### Example: Candidate's Projects

| Project | Type | Prize | Technologies |
|---------|------|-------|--------------|
| SolanaSwap DEX | 🏆 Hackathon | $10K - 1st Place | Rust, Anchor, React |
| Multi-chain Portfolio Tracker | Side Project | — | Next.js, GraphQL |
| Router SDK Contribution | Open Source | — | TypeScript, Node.js |

---

## 6. Soft Attributes

These are **universal qualities** that matter for any job. The AI scores each candidate 1-5:

| Attribute | What It Measures | Signals in CV |
|-----------|-----------------|---------------|
| **Leadership** | Has led teams or projects | "Led team of 10", "Architected", "Managed" |
| **Autonomy** | Works independently | Founder roles, solo projects, remote success |
| **Technical Depth** | Systems thinking, handles complexity | Infrastructure, performance optimization |
| **Communication** | Collaboration, writing, speaking | Documentation, blog posts, talks |
| **Growth Trajectory** | Career progression speed | Promotions, expanding scope |

### Score Meanings

| Score | Level | Description |
|-------|-------|-------------|
| 1 | Minimal | Little evidence of this quality |
| 2 | Basic | Some indicators present |
| 3 | Solid | Clear evidence, meets expectations |
| 4 | Strong | Above average, stands out |
| 5 | Exceptional | Top-tier, very impressive |

### Example: Candidate's Soft Attributes

| Attribute | Score | Why |
|-----------|-------|-----|
| Leadership | ⭐⭐⭐⭐ (4) | Led frontend architecture decisions, mentored developers |
| Autonomy | ⭐⭐⭐⭐ (4) | Built complex multi-chain systems independently |
| Technical Depth | ⭐⭐⭐⭐⭐ (5) | Deep expertise in real-time systems, cross-chain architecture |
| Communication | ⭐⭐⭐ (3) | Good documentation, but limited public speaking |
| Growth Trajectory | ⭐⭐⭐⭐ (4) | Quick progression to Senior, expanded into new domains |

---

## 7. Job Requirements

### Basic Job Info

| Field | Description | Example |
|-------|-------------|---------|
| **Title** | Job title | "Senior Frontend Engineer" |
| **Company** | Hiring company | "Trojan Trading" |
| **Seniority Level** | Required level | Senior |
| **Employment Type** | Full-time, part-time, contract | Full-time |
| **Location** | Remote, hybrid, onsite | Remote |
| **Salary Range** | Compensation offered | $100,000 - $350,000 |

### Skill Requirements

| Category | Skills | How We Use It |
|----------|--------|---------------|
| **Must-Have** | React, Next.js, TypeScript, WebSocket | Candidate must have these (filter) |
| **Nice-to-Have** | Solana, DeFi, CI/CD | Bonus points if they have these |
| **Domain Experience** | Trading Platforms, DeFi, Crypto | Match against candidate's past work |

### Soft Attribute Minimums

Jobs can specify minimum scores for soft attributes:

| Attribute | Minimum Required | Why (from job description) |
|-----------|-----------------|---------------------------|
| Leadership | 4+ | "key decision maker on frontend team" |
| Autonomy | 4+ | "highly autonomous and driven" |
| Technical Depth | 4+ | "complex challenges", "optimize for speed" |
| Communication | 3+ | "articulate ideas and collaborate" |
| Growth | — | Not specified |

### Enriching Jobs After Discovery Calls

After a discovery call with the client, the team can **directly edit** the normalized job fields to incorporate additional insights:

| What to Update | Example Change |
|----------------|----------------|
| **Must-Have Skills** | Add "HFT experience" if client emphasized it |
| **Nice-to-Have Skills** | Add specific technologies mentioned |
| **Domain Experience** | Add "Trading Platforms" or specific company backgrounds |
| **Soft Attribute Minimums** | Increase leadership score if they need someone senior |

This keeps the data model simple while allowing full flexibility to refine requirements.

---

## 8. Semantic Vectors (AI Matching)

Beyond exact filters and scores, we use **AI-generated vectors** to find candidates who are *semantically similar* to what the job needs — even if the exact words don't match.

### What Are Vectors?

A vector is a way to represent text as numbers that capture its **meaning**. Two pieces of text with similar meaning will have similar vectors, even if they use different words.

**Example:**
- "Built a DEX on Solana" and "Developed decentralized exchange for Solana blockchain" → **Very similar vectors**
- "Built a DEX on Solana" and "Managed marketing campaigns" → **Very different vectors**

### Candidate Vectors

We generate these vectors for each candidate:

| Vector | What It Captures | Question It Answers |
|--------|------------------|---------------------|
| **Experience Vector** | Overall career story (all roles combined) | "Who has a similar overall background?" |
| **Domain Context Vector** | Industries & problem spaces worked in | "Who has worked in DeFi vs NFTs vs Infrastructure?" |
| **Personality Vector** | Work style, values, culture fit | "Who would thrive in a fast-paced startup?" |
| **Position Vectors** | Each individual past role (one per job) | "Who has done *this exact job* before?" |
| **Skill Vectors** | Notable achievements per skill | "Who has deep TypeScript expertise in trading?" |

### Job Vectors

We generate these vectors for each job:

| Vector | What It Captures | Matches Against |
|--------|------------------|-----------------|
| **Role Description Vector** | What the job involves | Candidate's position vectors |
| **Domain Context Vector** | Product/industry context | Candidate's domain vector |
| **Culture Vector** | Team style, work environment | Candidate's personality vector |

### How Vector Matching Works

```
Job: "Senior Frontend Engineer at crypto trading platform"
                    ↓
        [Generate job vectors]
                    ↓
    Compare against all candidate vectors
                    ↓
    "Who has worked on something similar?"
                    ↓
    Candidates ranked by similarity (0-100%)
```

### Why This Matters

| Scenario | Exact Matching | Vector Matching |
|----------|----------------|-----------------|
| Job says "DEX experience" | Only finds "DEX" in CV | Also finds "decentralized exchange", "AMM", "liquidity pool" |
| Job says "trading platform" | Only finds "trading platform" | Also finds "exchange", "order book", "market maker" |
| Job is at a "lean startup" | Can't filter for this | Finds candidates who worked at similar-stage companies |

Vectors let us find the **right** candidates, not just the ones who used the exact same words.

---

## 9. How Matching Works

When a job comes in, we find the best candidates in **three steps**:

### Step 1: Hard Filters (Eliminate Non-Starters)

These are deal-breakers. Candidates who don't meet these are excluded:

| Filter | Example |
|--------|---------|
| Years of experience | Must have 5+ years |
| Required skills | Must have React, TypeScript |
| Seniority level | Must be Senior or above |
| Verification | Not marked as fraudulent |

**Result:** Narrows thousands → hundreds of candidates

### Step 2: Score-Based Ranking

For remaining candidates, we calculate scores:

| Score Type | Weight | What It Measures |
|------------|--------|------------------|
| **Skill Match** | 40% | Do they have the required skills at the right level? |
| **Similarity Match** | 60% | How similar is their experience to what the job needs? |

**Skill Match Example:**
- Job needs: React (must-have), Solana (nice-to-have)
- Candidate has: React ⭐⭐⭐⭐⭐, Solana ⭐⭐⭐⭐
- Score: Very high!

**Similarity Match:**
- We use AI to compare the job description against the candidate's past roles
- "Has this person done similar work before?"

### Step 3: Soft Attribute Filtering

If the job requires minimum soft scores, we filter:

| Job Requires | Candidate Has | Pass? |
|--------------|---------------|-------|
| Leadership ≥ 4 | Leadership = 4 | ✅ Yes |
| Autonomy ≥ 4 | Autonomy = 4 | ✅ Yes |
| Technical ≥ 4 | Technical = 5 | ✅ Yes |
| Communication ≥ 3 | Communication = 3 | ✅ Yes |

### Final Result

Top 20-50 candidates ranked by match score, with breakdown showing:
- ✅ Which skills they have
- ✅ How their experience matches
- ✅ Their soft attribute scores
- ⚠️ Any gaps to be aware of

---

## Quick Reference: What We Store

### Per Candidate

| Category | What We Store |
|----------|---------------|
| **Profile** | Name, location, current role, seniority, years experience, salary expectations |
| **Skills** | Each skill with rating (1-5), years, and best example |
| **Experience** | Each job with company, title, years, description, and skills used |
| **Projects** | Hackathons, side projects, open source with technologies and prizes |
| **Soft Attributes** | 5 universal scores (leadership, autonomy, depth, communication, growth) |
| **Vectors** | Experience, domain context, personality, per-position, per-skill |
| **GitHub** | Username, repos, stars, languages |
| **Verification** | Status (unverified/verified/suspicious/fraudulent) |

### Per Job

| Category | What We Store |
|----------|---------------|
| **Basics** | Title, company, seniority, employment type, location, salary |
| **Skills** | Must-have list, nice-to-have list, domain experience (editable after discovery calls) |
| **Soft Minimums** | Minimum scores for leadership, autonomy, etc. (optional, adjustable) |
| **Vectors** | Role description, domain context, culture |

---

## Questions?

For technical implementation details, see:
- [Data Model Proposal.md](./Data%20Model%20Proposal.md) — Full database schema
- [Skills Taxonomy System Proposal.md](./Skills%20Taxonomy%20System%20Proposal.md) — How we normalize and rate skills

**Code Implementation:**
- [`talent_matching/models/`](../../talent_matching/models/) — SQLAlchemy models (Python)
- [`talent_matching/models/candidates.py`](../../talent_matching/models/candidates.py) — Smart Profile schema
- [`talent_matching/models/jobs.py`](../../talent_matching/models/jobs.py) — Job requirements schema
