# Principle: Deterministic Staleness (NOT Time-Based)

## Core Principle

**An asset is stale if and only if:**
1. Its input data changed (upstream assets were re-materialized)
2. Its processing code version changed

**An asset is NOT stale just because time passed.**

## Rationale

```
┌─────────────────────────────────────────────────────────────────┐
│                     Why NOT Time-Based Staleness                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ❌ TTL-based: "Re-run every 60 minutes"                        │
│     • Wastes LLM API credits on unchanged data                  │
│     • Non-deterministic behavior                                │
│     • Data doesn't "decay" over time                            │
│                                                                 │
│  ✅ Event-based: "Re-run when inputs or code change"            │
│     • Only processes when there's actual change                 │
│     • Deterministic and auditable                               │
│     • Cost-efficient (LLM calls only when needed)               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation

### 1. Track Code Versions Explicitly

```python
@asset(
    code_version="1.0.0",  # Bump when processing logic changes
)
def normalized_candidates(raw_candidates):
    """Only re-runs when raw_candidates changes OR code_version bumps."""
    ...
```

### 2. Use Eager Auto-Materialize (Cascading Changes)

```python
from dagster import AutoMaterializePolicy

@asset(
    code_version="1.0.0",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def candidate_vectors(normalized_candidates):
    """Automatically re-runs when normalized_candidates is updated."""
    ...
```

### 3. Staleness Propagation

```
raw_candidates[X] changes
        │
        ▼ (triggers)
normalized_candidates[X] re-runs
        │
        ▼ (triggers)
candidate_vectors[X] re-runs
        │
        ▼ (triggers)
candidate_matches[X] re-runs
```

Each downstream asset re-runs because its **input changed**, not because of a timer.

## When Code Version Changes

```python
# Before: v1.0.0
@asset(code_version="1.0.0")
def normalized_candidates(raw_candidates):
    return llm.normalize(raw_candidates, prompt="Extract skills...")

# After: v1.1.0 (prompt changed)
@asset(code_version="1.1.0")  # ◄── Bump version
def normalized_candidates(raw_candidates):
    return llm.normalize(raw_candidates, prompt="Extract skills and years...")
```

When you bump `code_version`:
- Dagster marks ALL existing materializations as stale
- Next run will reprocess with new logic
- Downstream assets cascade

## What Triggers Re-Materialization

| Trigger | Re-runs? | Example |
|---------|----------|---------|
| Upstream asset re-materialized | ✅ Yes | `raw_candidates` updated |
| Code version bumped | ✅ Yes | Changed LLM prompt |
| Configuration changed | ✅ Yes | Different model parameters |
| Time passed | ❌ No | 24 hours since last run |
| Manual trigger | ✅ Yes | User clicks "Materialize" |

## Anti-Pattern: FreshnessPolicy

Dagster offers `FreshnessPolicy(maximum_lag_minutes=60)` but we explicitly **do not use it** because:

```python
# ❌ ANTI-PATTERN - Do not use
@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60),
)
def normalized_candidates(raw_candidates):
    # This would re-run every hour even if nothing changed
    # Wastes LLM credits, non-deterministic
    ...
```

## Observable Source Assets (External Data)

For data that enters the system from outside (files, APIs), use observable source assets to track actual changes:

```python
from dagster import observable_source_asset, DataVersion
import hashlib

@observable_source_asset
def cv_files(context):
    """Track when actual CV files change, not just time."""
    files = list(Path("cvs/").glob("*.pdf"))

    # Hash based on file contents/modification times
    content_hash = hashlib.md5(
        str([(f.name, f.stat().st_mtime) for f in files]).encode()
    ).hexdigest()

    # Only triggers downstream when hash changes
    return DataVersion(content_hash)
```

## Summary

```
┌─────────────────────────────────────────────────────────────────┐
│                  Deterministic Staleness Model                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Asset is FRESH when:                                           │
│  • All upstream assets unchanged since last materialization     │
│  • Code version unchanged                                       │
│  • Configuration unchanged                                      │
│                                                                 │
│  Asset is STALE when:                                           │
│  • Any upstream asset was re-materialized                       │
│  • Code version was bumped                                      │
│  • Configuration was modified                                   │
│                                                                 │
│  Asset is NEVER stale just because time passed.                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Related Concepts

- [Code Versioning](01-code-versioning.md)
- [Auto-Materialization Policies](02-auto-materialize-policies.md)
- [Sensors](04-sensors.md)
