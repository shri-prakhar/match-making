# Principle: LLM Cost Tracking & Estimation

## Problem

LLM API calls are the primary cost driver. You need to:
1. **Track actual costs** per asset, per run, per record
2. **Estimate costs** before running a pipeline (especially after code changes)
3. **Alert** when costs exceed thresholds

## Solution

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    LLM Cost Tracking Flow                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Asset Run                                                      │
│     │                                                           │
│     ▼                                                           │
│  ┌─────────────────┐    ┌─────────────────┐                    │
│  │ LLM Resource    │───►│ OpenRouter API  │                    │
│  │ (wraps calls)   │    └────────┬────────┘                    │
│  └────────┬────────┘             │                              │
│           │                      │ Response includes:           │
│           │                      │ • tokens_used                │
│           │                      │ • model                      │
│           │                      │ • cost                       │
│           ▼                      ▼                              │
│  ┌─────────────────────────────────────────┐                   │
│  │           Cost Tracking Table           │                   │
│  │  • run_id                               │                   │
│  │  • asset_key                            │                   │
│  │  • partition_key                        │                   │
│  │  • operation (normalize_cv, score, etc) │                   │
│  │  • model                                │                   │
│  │  • input_tokens                         │                   │
│  │  • output_tokens                        │                   │
│  │  • cost_usd                             │                   │
│  │  • timestamp                            │                   │
│  └─────────────────────────────────────────┘                   │
│                                                                 │
│  Cost Estimation (before run)                                   │
│     │                                                           │
│     ▼                                                           │
│  ┌─────────────────────────────────────────┐                   │
│  │ 1. Get stale assets (from code change)  │                   │
│  │ 2. Count affected partitions            │                   │
│  │ 3. Look up avg cost per partition       │                   │
│  │ 4. Multiply: partitions × avg_cost      │                   │
│  └─────────────────────────────────────────┘                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation

### 1. Database Schema for Cost Tracking

```sql
-- Add to docker/init.sql
CREATE TABLE IF NOT EXISTS llm_costs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id VARCHAR(100) NOT NULL,
    asset_key VARCHAR(100) NOT NULL,
    partition_key VARCHAR(100),
    operation VARCHAR(50) NOT NULL,  -- 'normalize_cv', 'normalize_job', 'score', 'embed'
    model VARCHAR(100) NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    total_tokens INTEGER GENERATED ALWAYS AS (input_tokens + output_tokens) STORED,
    cost_usd DECIMAL(10, 6) NOT NULL,
    code_version VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for cost analysis
CREATE INDEX idx_llm_costs_asset ON llm_costs(asset_key);
CREATE INDEX idx_llm_costs_operation ON llm_costs(operation);
CREATE INDEX idx_llm_costs_created ON llm_costs(created_at);
CREATE INDEX idx_llm_costs_code_version ON llm_costs(code_version);

-- View for cost aggregation
CREATE VIEW llm_cost_summary AS
SELECT
    asset_key,
    operation,
    code_version,
    COUNT(*) as call_count,
    AVG(cost_usd) as avg_cost_per_call,
    SUM(cost_usd) as total_cost,
    AVG(input_tokens) as avg_input_tokens,
    AVG(output_tokens) as avg_output_tokens
FROM llm_costs
GROUP BY asset_key, operation, code_version;
```

### 2. OpenRouter LLM Resource with Cost Tracking

```python
# talent_matching/resources/openrouter.py

import os
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field


# OpenRouter pricing per 1M tokens (as of 2024)
MODEL_PRICING = {
    "openai/gpt-4o": {"input": 2.50, "output": 10.00},
    "openai/gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "anthropic/claude-3.5-sonnet": {"input": 3.00, "output": 15.00},
    "anthropic/claude-3-haiku": {"input": 0.25, "output": 1.25},
}


class OpenRouterResource(ConfigurableResource):
    """OpenRouter LLM resource with built-in cost tracking."""

    api_key: str = Field(
        default_factory=lambda: os.getenv("OPENROUTER_API_KEY", ""),
    )
    default_model: str = Field(default="openai/gpt-4o-mini")

    # Cost tracking
    _db_connection: Any = None
    _current_run_id: str = ""
    _current_asset_key: str = ""
    _current_partition_key: str = ""
    _code_version: str = ""

    def set_context(
        self,
        run_id: str,
        asset_key: str,
        partition_key: str = "",
        code_version: str = "",
    ):
        """Set context for cost tracking. Call this at start of asset."""
        self._current_run_id = run_id
        self._current_asset_key = asset_key
        self._current_partition_key = partition_key
        self._code_version = code_version

    def _calculate_cost(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
    ) -> Decimal:
        """Calculate cost in USD based on model pricing."""
        pricing = MODEL_PRICING.get(model, {"input": 5.0, "output": 15.0})

        input_cost = Decimal(str(input_tokens)) / 1_000_000 * Decimal(str(pricing["input"]))
        output_cost = Decimal(str(output_tokens)) / 1_000_000 * Decimal(str(pricing["output"]))

        return input_cost + output_cost

    def _log_cost(
        self,
        operation: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: Decimal,
    ):
        """Log cost to database and Dagster."""
        logger = get_dagster_logger()

        # Log to Dagster
        logger.info(
            f"LLM Cost: {operation} | {model} | "
            f"{input_tokens}+{output_tokens} tokens | ${cost_usd:.6f}"
        )

        # Store in database
        self._store_cost_record(
            operation=operation,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
        )

    def _store_cost_record(
        self,
        operation: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: Decimal,
    ):
        """Store cost record in PostgreSQL."""
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "talent"),
            password=os.getenv("POSTGRES_PASSWORD", "talent_dev"),
            dbname=os.getenv("POSTGRES_DB", "talent_matching"),
        )
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO llm_costs
            (run_id, asset_key, partition_key, operation, model,
             input_tokens, output_tokens, cost_usd, code_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                self._current_run_id,
                self._current_asset_key,
                self._current_partition_key or None,
                operation,
                model,
                input_tokens,
                output_tokens,
                float(cost_usd),
                self._code_version or None,
            ),
        )

        conn.commit()
        cursor.close()
        conn.close()

    def complete(
        self,
        messages: list[dict],
        model: str | None = None,
        operation: str = "completion",
        **kwargs,
    ) -> dict[str, Any]:
        """Make a completion request and track costs."""
        model = model or self.default_model

        response = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": messages,
                **kwargs,
            },
            timeout=60.0,
        )
        response.raise_for_status()
        data = response.json()

        # Extract usage
        usage = data.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)

        # Calculate and log cost
        cost_usd = self._calculate_cost(model, input_tokens, output_tokens)
        self._log_cost(operation, model, input_tokens, output_tokens, cost_usd)

        return data

    def normalize_cv(self, raw_cv_text: str) -> dict[str, Any]:
        """Normalize CV with cost tracking."""
        response = self.complete(
            messages=[
                {"role": "system", "content": "You are a CV parser..."},
                {"role": "user", "content": f"Parse this CV:\n\n{raw_cv_text}"},
            ],
            model="openai/gpt-4o-mini",  # Cheaper model for extraction
            operation="normalize_cv",
            response_format={"type": "json_object"},
        )

        import json
        return json.loads(response["choices"][0]["message"]["content"])

    def normalize_job(self, raw_job_text: str) -> dict[str, Any]:
        """Normalize job description with cost tracking."""
        response = self.complete(
            messages=[
                {"role": "system", "content": "You are a job description parser..."},
                {"role": "user", "content": f"Parse this job:\n\n{raw_job_text}"},
            ],
            model="openai/gpt-4o-mini",
            operation="normalize_job",
            response_format={"type": "json_object"},
        )

        import json
        return json.loads(response["choices"][0]["message"]["content"])

    def score_candidate(self, profile: dict, job: dict) -> dict[str, Any]:
        """Score candidate fit with cost tracking."""
        response = self.complete(
            messages=[
                {"role": "system", "content": "You are a recruiting expert..."},
                {"role": "user", "content": f"Score this candidate:\n{profile}\n\nFor job:\n{job}"},
            ],
            model="openai/gpt-4o",  # Better model for reasoning
            operation="score_candidate",
        )

        import json
        return json.loads(response["choices"][0]["message"]["content"])
```

### 3. Using the Resource in Assets

```python
# talent_matching/assets/candidates.py

@asset(
    code_version="1.0.0",
    partitions_def=candidate_partitions,
)
def normalized_candidates(
    context: AssetExecutionContext,
    llm: OpenRouterResource,  # Injected resource
    raw_candidates: dict,
) -> dict:
    """Normalize candidate with cost tracking."""

    # Set context for cost tracking
    llm.set_context(
        run_id=context.run_id,
        asset_key="normalized_candidates",
        partition_key=context.partition_key,
        code_version="1.0.0",
    )

    # This call is now automatically tracked
    return llm.normalize_cv(raw_candidates["raw_text"])
```

### 4. Cost Estimation Before Running

```python
# talent_matching/utils/cost_estimator.py

from dataclasses import dataclass
from decimal import Decimal

import psycopg2


@dataclass
class CostEstimate:
    """Estimated cost for a pipeline run."""
    asset_key: str
    partition_count: int
    avg_cost_per_partition: Decimal
    estimated_total: Decimal
    confidence: str  # 'high', 'medium', 'low'


def get_avg_cost_per_partition(
    asset_key: str,
    code_version: str | None = None,
) -> tuple[Decimal, int]:
    """Get average cost per partition from historical data."""
    conn = psycopg2.connect(...)
    cursor = conn.cursor()

    if code_version:
        # Get cost for specific code version
        cursor.execute(
            """
            SELECT AVG(cost_usd), COUNT(*)
            FROM llm_costs
            WHERE asset_key = %s AND code_version = %s
            """,
            (asset_key, code_version),
        )
    else:
        # Get cost for latest code version
        cursor.execute(
            """
            SELECT AVG(cost_usd), COUNT(*)
            FROM llm_costs
            WHERE asset_key = %s
            AND code_version = (
                SELECT code_version FROM llm_costs
                WHERE asset_key = %s
                ORDER BY created_at DESC LIMIT 1
            )
            """,
            (asset_key, asset_key),
        )

    result = cursor.fetchone()
    cursor.close()
    conn.close()

    avg_cost = Decimal(str(result[0])) if result[0] else Decimal("0")
    sample_count = result[1] or 0

    return avg_cost, sample_count


def estimate_run_cost(
    stale_assets: list[str],
    partition_counts: dict[str, int],
) -> list[CostEstimate]:
    """Estimate cost for running stale assets."""
    estimates = []

    for asset_key in stale_assets:
        partition_count = partition_counts.get(asset_key, 1)
        avg_cost, sample_count = get_avg_cost_per_partition(asset_key)

        # Determine confidence based on sample size
        if sample_count >= 100:
            confidence = "high"
        elif sample_count >= 10:
            confidence = "medium"
        else:
            confidence = "low"

        estimates.append(CostEstimate(
            asset_key=asset_key,
            partition_count=partition_count,
            avg_cost_per_partition=avg_cost,
            estimated_total=avg_cost * partition_count,
            confidence=confidence,
        ))

    return estimates


def estimate_code_change_cost(
    changed_asset: str,
    new_code_version: str,
) -> dict:
    """Estimate cost of changing an asset's code version.

    When code version changes, ALL partitions become stale.
    """
    conn = psycopg2.connect(...)
    cursor = conn.cursor()

    # Count total partitions for this asset
    cursor.execute(
        """
        SELECT COUNT(DISTINCT partition_key)
        FROM llm_costs
        WHERE asset_key = %s
        """,
        (changed_asset,),
    )
    partition_count = cursor.fetchone()[0] or 0

    # Get average cost per partition
    avg_cost, _ = get_avg_cost_per_partition(changed_asset)

    # Get downstream assets that would also re-run
    downstream = get_downstream_assets(changed_asset)

    total_estimate = avg_cost * partition_count

    # Add downstream costs
    downstream_estimates = []
    for downstream_asset in downstream:
        ds_avg, _ = get_avg_cost_per_partition(downstream_asset)
        ds_cost = ds_avg * partition_count
        total_estimate += ds_cost
        downstream_estimates.append({
            "asset": downstream_asset,
            "estimated_cost": float(ds_cost),
        })

    cursor.close()
    conn.close()

    return {
        "changed_asset": changed_asset,
        "partition_count": partition_count,
        "direct_cost": float(avg_cost * partition_count),
        "downstream_assets": downstream_estimates,
        "total_estimated_cost": float(total_estimate),
        "warning": "This will re-process ALL existing partitions" if partition_count > 100 else None,
    }
```

### 5. CLI Tool for Cost Estimation

```python
# talent_matching/cli/estimate_costs.py

import click
from talent_matching.utils.cost_estimator import estimate_code_change_cost


@click.command()
@click.option("--asset", required=True, help="Asset that changed")
@click.option("--version", required=True, help="New code version")
def estimate(asset: str, version: str):
    """Estimate cost of a code change before deploying."""
    result = estimate_code_change_cost(asset, version)

    click.echo(f"\n{'='*50}")
    click.echo(f"COST ESTIMATE: {asset} → v{version}")
    click.echo(f"{'='*50}\n")

    click.echo(f"Partitions affected: {result['partition_count']}")
    click.echo(f"Direct cost: ${result['direct_cost']:.2f}")

    if result['downstream_assets']:
        click.echo(f"\nDownstream cascade:")
        for ds in result['downstream_assets']:
            click.echo(f"  • {ds['asset']}: ${ds['estimated_cost']:.2f}")

    click.echo(f"\n{'─'*50}")
    click.echo(f"TOTAL ESTIMATED COST: ${result['total_estimated_cost']:.2f}")
    click.echo(f"{'─'*50}\n")

    if result.get('warning'):
        click.echo(f"⚠️  {result['warning']}\n")


if __name__ == "__main__":
    estimate()
```

Usage:
```bash
python -m talent_matching.cli.estimate_costs \
    --asset normalized_candidates \
    --version 1.1.0

# Output:
# ==================================================
# COST ESTIMATE: normalized_candidates → v1.1.0
# ==================================================
#
# Partitions affected: 5,234
# Direct cost: $52.34
#
# Downstream cascade:
#   • candidate_vectors: $10.47
#   • candidate_matches: $26.17
#
# ──────────────────────────────────────────────────
# TOTAL ESTIMATED COST: $88.98
# ──────────────────────────────────────────────────
#
# ⚠️  This will re-process ALL existing partitions
```

## Cost Dashboards

### SQL Queries for Analysis

```sql
-- Cost by asset (last 30 days)
SELECT
    asset_key,
    SUM(cost_usd) as total_cost,
    COUNT(*) as call_count,
    AVG(cost_usd) as avg_cost
FROM llm_costs
WHERE created_at > NOW() - INTERVAL '30 days'
GROUP BY asset_key
ORDER BY total_cost DESC;

-- Cost by code version (track impact of changes)
SELECT
    asset_key,
    code_version,
    SUM(cost_usd) as total_cost,
    AVG(cost_usd) as avg_per_call,
    MIN(created_at) as first_used,
    MAX(created_at) as last_used
FROM llm_costs
GROUP BY asset_key, code_version
ORDER BY asset_key, first_used DESC;

-- Daily cost trend
SELECT
    DATE(created_at) as date,
    SUM(cost_usd) as daily_cost,
    COUNT(*) as calls
FROM llm_costs
GROUP BY DATE(created_at)
ORDER BY date DESC
LIMIT 30;
```

## Trade-offs

| Approach | Pros | Cons |
|----------|------|------|
| Track per-call | Granular data | Storage overhead |
| Track per-run | Less storage | Less granular |
| Estimate from history | Fast, cheap | Less accurate for new patterns |

## Related Concepts

- [Code Versioning](01-code-versioning.md)
- [Deterministic Staleness](03-freshness-policies.md)
- [Dynamic Partitions](05-dynamic-partitions.md)
