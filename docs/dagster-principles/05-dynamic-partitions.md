# Principle: Dynamic Partitions for Incremental Processing

## Problem

You have a growing dataset (candidates, jobs) and want to:
1. Process only NEW records, not the entire dataset
2. Track which records have been processed
3. Re-run processing for specific records if needed

## Solution

Dynamic partitions allow you to add partition keys at runtime as new records arrive. Each record gets its own partition, and Dagster tracks materialization status per partition.

## Implementation

### 1. Define Dynamic Partitions

```python
from dagster import DynamicPartitionsDefinition

# Define partition namespaces
candidate_partitions = DynamicPartitionsDefinition(name="candidates")
job_partitions = DynamicPartitionsDefinition(name="jobs")
```

### 2. Create Partitioned Assets

```python
@asset(
    partitions_def=candidate_partitions,
    group_name="candidates",
)
def raw_candidates(context: AssetExecutionContext) -> dict:
    """Process a single candidate."""
    candidate_id = context.partition_key

    # Fetch only this candidate
    return fetch_candidate_by_id(candidate_id)


@asset(
    partitions_def=candidate_partitions,
    group_name="candidates",
)
def normalized_candidates(
    context: AssetExecutionContext,
    raw_candidates: dict,
) -> dict:
    """Normalize a single candidate."""
    return llm.normalize_cv(raw_candidates)


@asset(
    partitions_def=candidate_partitions,
    group_name="candidates",
)
def candidate_vectors(
    context: AssetExecutionContext,
    normalized_candidates: dict,
) -> dict:
    """Generate vectors for a single candidate."""
    return embeddings.embed(normalized_candidates)
```

### 3. Add Partitions at Runtime

```python
from dagster import sensor, RunRequest

@sensor(job=candidate_pipeline_job)
def new_candidate_sensor(context: SensorEvaluationContext):
    new_candidates = fetch_unprocessed_candidates()

    for candidate in new_candidates:
        candidate_id = candidate["id"]

        # Add new partition key (idempotent operation)
        context.instance.add_dynamic_partitions(
            partitions_def_name="candidates",
            partition_keys=[candidate_id],
        )

        # Trigger run for this specific partition
        yield RunRequest(
            run_key=f"candidate-{candidate_id}",
            partition_key=candidate_id,
        )
```

## Visual Representation

```
┌──────────────────────────────────────────────────────────────┐
│                   Partition Status View                      │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  raw_candidates          normalized_candidates               │
│  ┌────────────────┐      ┌────────────────┐                 │
│  │ candidate-001 ✓│ ───► │ candidate-001 ✓│                 │
│  │ candidate-002 ✓│ ───► │ candidate-002 ✓│                 │
│  │ candidate-003 ✓│ ───► │ candidate-003 ○│  ◄── In progress│
│  │ candidate-004 ○│      │ candidate-004 -│  ◄── Pending    │
│  └────────────────┘      └────────────────┘                 │
│                                                              │
│  Legend: ✓ = Materialized, ○ = Running, - = Not started     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## Key Operations

### Add Partitions Programmatically

```python
from dagster import DagsterInstance

instance = DagsterInstance.get()
instance.add_dynamic_partitions(
    partitions_def_name="candidates",
    partition_keys=["candidate-005", "candidate-006"],
)
```

### List Existing Partitions

```python
partitions = instance.get_dynamic_partitions("candidates")
# ['candidate-001', 'candidate-002', ...]
```

### Delete Partitions

```python
instance.delete_dynamic_partition(
    partitions_def_name="candidates",
    partition_key="candidate-deleted",
)
```

### Materialize Specific Partition

```bash
# CLI
dagster asset materialize \
    --select raw_candidates \
    --partition candidate-005
```

## Comparison: Static vs Dynamic Partitions

| Type | Use Case | Example |
|------|----------|---------|
| `DailyPartitionsDefinition` | Time-based batches | Process all candidates from 2024-01-15 |
| `StaticPartitionsDefinition` | Fixed categories | Process by region: US, EU, APAC |
| `DynamicPartitionsDefinition` | Growing entities | Process candidate-xyz |

## Trade-offs

| Approach | Pros | Cons |
|----------|------|------|
| Dynamic partitions | Fine-grained control, per-record tracking | More partition keys to manage |
| Time-based batches | Simpler, natural grouping | Must wait for batch window |
| No partitions | Simplest | Reprocess everything each run |

## Related Concepts

- [Sensors](04-sensors.md)
- [Cross-Join Matching Pattern](06-cross-join-matching.md)
