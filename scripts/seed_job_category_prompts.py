"""Seed job_category_prompts from the Notion-derived proposal.

Run against remote DB: poetry run with-remote-db python scripts/seed_job_category_prompts.py
Run against local DB:  poetry run with-local-db python scripts/seed_job_category_prompts.py
On server: poetry run python scripts/seed_job_category_prompts.py --local

Upserts by job_category (insert or update). Adds alias Compliance = Legal.
"""

from datetime import UTC, datetime

from sqlalchemy import select

from talent_matching.db import get_session
from talent_matching.models.job_category_prompts import JobCategoryPromptsRecord
from talent_matching.script_env import apply_local_db

# From docs/job-category-prompts-proposal.md (Notion: Data Points per Job Category).
# CV extraction: emphasize category-relevant skills and evidence; extract generously — more skills with evidence is better.
CATEGORY_PROMPTS = [
    (
        "Account Executive",
        "Extract all sales-relevant skills with evidence. Include: quota ownership and attainment, deal closing, average deal size, revenue closed, strategic/enterprise account management, outbound vs inbound sales; evidence of closing (not just supporting); buyer types (founders, protocols, exchanges, enterprises). Include any other sales or commercial skills with concrete evidence. More skills with evidence is better.",
        "Prioritize: actual revenue closed and quota ownership (not just relationship management). Weight size of accounts managed, relevance of network, seniority of previous employers, and evidence of closing. Distinguish enterprise/mid-market from small transactional sales.",
    ),
    (
        "Business Development",
        "Extract all partnership and BD-relevant skills with evidence. Include: partnership development, deal value, strategic account/ecosystem names, go-to-market partnerships, cross-functional work with product/marketing; outcomes (integrations, revenue, ecosystem growth). Include any related BD or partnership skills. More skills with evidence is better.",
        "Prioritize: real partnerships that led to integrations, revenue, or ecosystem growth. Weight partnership type and value, strategic account names, and access to decision-makers. Consider cross-functional work with product and marketing.",
    ),
    (
        "Community & Marketing",
        "Extract all community and marketing skills with evidence. Include: follower/engagement growth, campaign metrics, community growth, retention or activation impact, content strategy, launches and campaigns; organic/social growth, brand, narrative, ecosystem campaigns. Include any related marketing or community skills. More skills with evidence is better.",
        "Prioritize: ability to build and maintain attention, create community trust, and turn awareness into growth. Score organic growth, social growth, brand, content, narrative, partnerships, and ecosystem campaigns. Weight measurable campaign and community impact.",
    ),
    (
        "Customer Support",
        "Extract all support-relevant skills with evidence. Include: support volume, CSAT/NPS impact, ticket backlog reduction, escalation handling, tooling, multilingual support; response/resolution time and retention improvements; crypto/wallet/DeFi/onboarding experience. Include operations maturity, communication, product familiarity. More skills with evidence is better.",
        "Prioritize: support at scale; improvements in response time, resolution time, retention, satisfaction. Weight operations maturity, communication clarity, product familiarity, and scale of user support. Public social metrics matter less here.",
    ),
    (
        "Designer",
        "Extract all design-relevant skills with evidence. Include: portfolio quality, shipped designs, design systems, UX/conversion impact, brand consistency; what was designed (brand, product, motion, growth, systems) and whether work shipped to real users. Include any design or creative skills with evidence. More skills with evidence is better.",
        "Prioritize: portfolio quality, relevance to crypto/web3 aesthetics and product patterns, shipped work, and visible craft level. Followers alone are a weak signal.",
    ),
    (
        "Growth",
        "Extract all growth-relevant skills with evidence. Include: user/activation/retention metrics, CAC, funnel optimization, growth loops and channels, experimentation; acquisition, retention, referrals, monetization, token or community-led growth. Include any related growth or growth-marketing skills. More skills with evidence is better.",
        "Prioritize: measurable growth impact, experimentation mindset, channel sophistication, and crypto-native distribution knowledge.",
    ),
    (
        "Legal",
        "Extract all legal-relevant skills with evidence. Include: jurisdictions, transaction types, licensing/compliance, token/DAO/foundation work, contract ownership, counsel coordination, scale of matters; legal domain (regulatory, employment, corporate, funds, compliance). Include any related legal or compliance skills. More skills with evidence is better.",
        "Prioritize: specialization relevance, complexity of legal work, jurisdiction fit, and employer quality. Public social signals are secondary.",
    ),
    (
        "Operations",
        "Extract all operations-relevant skills with evidence. Include: functions owned, process improvements, operational scale, tooling, hiring/finance/compliance/people ops, cross-functional coordination, international/remote ops; systems building and execution in ambiguity. Include any related ops or execution skills. More skills with evidence is better.",
        "Prioritize: scope, ownership, complexity, and execution reliability. Public audience metrics are low priority.",
    ),
    (
        "Product Designer",
        "Extract all product design skills with evidence. Include: user problems solved, shipped interfaces, design systems, user research, product/UX impact; onboarding, dashboard, wallet/DeFi/trading UX, mobile flows. Include any related design or UX skills. More skills with evidence is better.",
        "Prioritize: portfolio depth, shipped work, product thinking, and crypto UX familiarity.",
    ),
    (
        "Product Manager",
        "Extract all product management skills with evidence. Include: product scale shipped, MAU/DAU, adoption, roadmap ownership, feature launches, user/retention impact; stage (0→1, 1→10, scale) and technical/ecosystem constraints. Include true product ownership and shipped adoption; include project coordination where present. More skills with evidence is better.",
        "Distinguish: project coordinators with PM titles vs true product owners; strategy-heavy vs execution-heavy PMs. Weight shipped product scale and adoption.",
    ),
    (
        "Product Marketer",
        "Extract all product marketing skills with evidence. Include: launches owned, positioning, GTM strategy, activation/adoption metrics, messaging frameworks, content strategy; cross-functional work with product/growth. Include any related marketing or GTM skills. More skills with evidence is better.",
        "Score both communication quality and actual launch impact.",
    ),
    (
        "Project Manager",
        "Extract all project delivery skills with evidence. Include: project size, timeline ownership, cross-functional coordination, budget, delivery success, process improvement, stakeholder management; outcomes (cost, speed, revenue). Include any related delivery or coordination skills. More skills with evidence is better.",
        "Prioritize: execution discipline, complexity handled, and delivery record.",
    ),
    (
        "Talent Sourcing / Human Resources",
        "Extract all recruiting/HR skills with evidence. Include: jobs managed, hires completed, time-to-fill, offer acceptance, seniority of hires, functions and geography, pipeline ownership; talent market understanding (crypto), passive sourcing, full-cycle vs sourcing. Include any related hiring or HR skills. More skills with evidence is better.",
        "Score: hiring relevance, quality of hiring network, pace of execution, and web3 talent access.",
    ),
    (
        "Public Relations",
        "Extract skills generously; include all abilities that support job matching. For PR, capture relevant metrics and campaign outcomes where present. For each skill include clear evidence. More skills with evidence is better.",
        "Use default refinement: evaluate against must-haves and overall fit; consider skills match, experience level, and domain relevance.",
    ),
    (
        "Research",
        "Extract skills generously; include all abilities that support job matching. For research, capture publications, methods, and domain expertise where present. For each skill include clear evidence. More skills with evidence is better.",
        "Use default refinement: evaluate against must-haves and overall fit; consider skills match, experience level, and domain relevance.",
    ),
    # Technical
    (
        "General (technical)",
        "Extract all technical skills with evidence. Include: languages, frameworks, infra; tenure per role, ownership of services/features/modules; measurable impact (latency, cost, throughput). For GitHub/code: languages, frameworks, recent activity, README/tests/CI/CD, PR quality, OSS contributions. Include substantial repos and any other technical evidence; note red flags (e.g. many short stints) where relevant. More skills with evidence is better.",
        "Evaluate technical fit from JD: must-haves vs nice-to-haves, level (Senior/Staff/Lead), success metrics, hard blockers. Weight CV evidence (tenure, ownership, impact, tech stack) and GitHub/code quality where relevant.",
    ),
    (
        "AI Engineer",
        "Extract all AI/ML engineering skills with evidence. Include: data hygiene, reproducible pipelines, experiment tracking (MLflow, W&B), model versioning, inference monitoring, cost/latency trade-offs, preprocessing and feature engineering. Include any related ML or data science skills. More skills with evidence is better.",
        "Prioritize: data hygiene, reproducible pipelines, experiment tracking, model versioning, inference monitoring, and clear preprocessing and feature engineering.",
    ),
    (
        "Backend Developer",
        "Extract all backend-relevant skills with evidence. Include: error handling, idempotency, transactional integrity, concurrency, database choice, message queues/eventing, caching, retry logic; plus general technical (tenure, ownership, impact, tech stack, GitHub). Include any related backend or systems skills. More skills with evidence is better.",
        "Prioritize: error handling, idempotency, transactional integrity, concurrency model, DB and eventing choices, caching, retry logic.",
    ),
    (
        "Data Engineer",
        "Extract all data engineering skills with evidence. Include: idempotent ETL/ELT, partitioning/retention, schema versioning, data validation tests, DAGs with CI, data SLAs and freshness. Include any related data or pipeline skills. More skills with evidence is better.",
        "Prioritize: idempotent pipelines, partitioning/retention, schema versioning, validation tests, DAG/CI, and data SLAs.",
    ),
    (
        "DevOps",
        "Extract all DevOps/SRE skills with evidence. Include: IaC and state management, safe rollouts (canary, blue-green), autoscaling, runbooks, cost ops, backup/restore. Include any related infra or reliability skills. More skills with evidence is better.",
        "Prioritize: IaC and state management, safe rollouts, autoscaling, runbooks, cost ops and backup/restore.",
    ),
    (
        "DevRel",
        "Extract all developer relations skills with evidence. Include: SDKs, example apps, quickstarts, docs quality, adoption metrics (downloads, issues), public talks and tutorials. Include any related dev advocacy or technical content skills. More skills with evidence is better.",
        "Prioritize: SDK/docs quality, quickstarts, adoption metrics, and public presence (talks, tutorials).",
    ),
    (
        "Frontend Developer",
        "Extract all frontend skills with evidence. Include: component structure, state/render performance, accessibility (a11y), bundle size/code-splitting, e2e tests, SSR/CSR. Include any related frontend or UI skills. More skills with evidence is better.",
        "Prioritize: component structure, performance, a11y, bundle/code-splitting, e2e tests, SSR/CSR trade-offs.",
    ),
    (
        "Full-Stack Developer",
        "Extract all full-stack skills with evidence. Include: end-to-end ownership (frontend, API, deployment), e2e tests, API design for frontend, security basics. Include both frontend and backend skills where present. More skills with evidence is better.",
        "Prioritize: end-to-end ownership, e2e coverage, API design for frontend, security basics.",
    ),
    (
        "Infrastructure Engineer",
        "Extract all infrastructure skills with evidence. Include: cluster/region scale, stateful services ops, disaster recovery, capacity planning, scaling automation. Include any related infra or platform skills. More skills with evidence is better.",
        "Prioritize: scale expertise, stateful ops, DR, capacity planning, scaling automation.",
    ),
    (
        "Mobile Engineer",
        "Extract all mobile engineering skills with evidence. Include: offline-first (local DB, background sync), crash/memory handling, CI for builds, release/rollout strategy, ANR/crash metrics. Include any related mobile or app skills. More skills with evidence is better.",
        "Prioritize: offline-first, crash/memory handling, CI, release/rollout strategy, ANR/crash metrics.",
    ),
    (
        "Protocol Engineer",
        "Extract all protocol/on-chain skills with evidence. Include: on-chain programs, account validation, compute-budget optimization, Anchor/IDL, deploy/test scripts, audit or fuzz/property tests. Include any related blockchain or smart-contract skills. More skills with evidence is better.",
        "Prioritize: tested on-chain programs, account validation, compute-budget optimization, Anchor/IDL, deploy/test scripts, audit or fuzz/property tests.",
    ),
    (
        "QA Engineer",
        "Extract all QA and test engineering skills with evidence. Include: automation suites, flaky-test handling, CI gating, test-data management, regression/perf strategy. Include any related testing or quality skills. More skills with evidence is better.",
        "Prioritize: automation quality, flaky-test handling, CI gating, test-data management, regression/perf strategy.",
    ),
    (
        "Security",
        "Extract all security skills with evidence. Include: threat modeling, SAST/DAST and dependency scanning, secret management, incident/pentest history, remediation. Include any related security or compliance skills. More skills with evidence is better.",
        "Prioritize: threat modeling, SAST/DAST and dependency scanning, secret management, incident/pentest history, remediation.",
    ),
]


def _legal_prompts():
    """Return (cv, refinement) for Legal so we can reuse for Compliance alias."""
    for cat, cv, ref in CATEGORY_PROMPTS:
        if cat == "Legal":
            return (cv, ref)
    return ("", "")


def main():
    apply_local_db()
    session = get_session()
    now = datetime.now(UTC)
    legal_cv, legal_ref = _legal_prompts()

    for job_category, cv_extraction_prompt, refinement_prompt in CATEGORY_PROMPTS:
        row = session.execute(
            select(JobCategoryPromptsRecord).where(
                JobCategoryPromptsRecord.job_category == job_category
            )
        ).scalar_one_or_none()
        if row is not None:
            row.cv_extraction_prompt = cv_extraction_prompt
            row.refinement_prompt = refinement_prompt
            row.updated_at = now
        else:
            session.add(
                JobCategoryPromptsRecord(
                    job_category=job_category,
                    cv_extraction_prompt=cv_extraction_prompt,
                    refinement_prompt=refinement_prompt,
                    updated_at=now,
                )
            )

    # Alias: Compliance uses same prompts as Legal
    row = session.execute(
        select(JobCategoryPromptsRecord).where(
            JobCategoryPromptsRecord.job_category == "Compliance"
        )
    ).scalar_one_or_none()
    if row is not None:
        row.cv_extraction_prompt = legal_cv
        row.refinement_prompt = legal_ref
        row.updated_at = now
    else:
        session.add(
            JobCategoryPromptsRecord(
                job_category="Compliance",
                cv_extraction_prompt=legal_cv,
                refinement_prompt=legal_ref,
                updated_at=now,
            )
        )

    session.commit()
    session.close()
    print(
        f"Upserted {len(CATEGORY_PROMPTS) + 1} rows into job_category_prompts (including Compliance alias)."
    )


if __name__ == "__main__":
    main()
