# Principle: Sensors for Event-Driven Pipelines

## Problem

You want to trigger pipeline runs when external events happen:
- A new candidate is added to the database
- A new CV file appears in a directory
- An API webhook is received

Schedules run at fixed times, but sensors react to events.

## Solution

Sensors are functions that periodically check for changes and yield run requests when conditions are met.

## Implementation

### Basic Sensor

```python
from dagster import sensor, RunRequest, SensorEvaluationContext

@sensor(job=candidate_processing_job)
def new_candidate_sensor(context: SensorEvaluationContext):
    """Poll database for new candidates every 30 seconds."""

    # Get cursor (last processed timestamp)
    last_processed = context.cursor or "1970-01-01T00:00:00"

    # Query for new records
    new_candidates = fetch_new_candidates_since(last_processed)

    for candidate in new_candidates:
        yield RunRequest(
            run_key=f"candidate-{candidate['id']}",
            run_config={
                "ops": {
                    "process_candidate": {
                        "config": {"candidate_id": candidate["id"]}
                    }
                }
            },
        )

    # Update cursor for next evaluation
    if new_candidates:
        context.update_cursor(new_candidates[-1]["created_at"])
```

### Sensor with Dynamic Partitions

```python
@sensor(job=partitioned_candidate_job)
def new_candidate_partition_sensor(context: SensorEvaluationContext):
    """Add new partition keys and trigger runs."""

    new_candidates = fetch_unprocessed_candidates()

    for candidate in new_candidates:
        candidate_id = candidate["id"]

        # Register new partition key
        context.instance.add_dynamic_partitions(
            partitions_def_name="candidates",
            partition_keys=[candidate_id],
        )

        # Trigger run for this partition
        yield RunRequest(
            run_key=f"candidate-{candidate_id}",
            partition_key=candidate_id,
        )
```

### File-Based Sensor

```python
from pathlib import Path

@sensor(job=cv_processing_job)
def new_cv_file_sensor(context: SensorEvaluationContext):
    """Watch directory for new CV files."""

    cv_dir = Path("/data/incoming_cvs")
    processed = set((context.cursor or "").split(","))

    for cv_file in cv_dir.glob("*.pdf"):
        if cv_file.name not in processed:
            yield RunRequest(
                run_key=cv_file.name,
                run_config={"ops": {"parse_cv": {"config": {"path": str(cv_file)}}}},
            )
            processed.add(cv_file.name)

    context.update_cursor(",".join(processed))
```

## Sensor Lifecycle

```
┌─────────────────────────────────────────────────────┐
│                 Sensor Lifecycle                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│   dagster-daemon running                            │
│          │                                          │
│          ▼                                          │
│   ┌──────────────┐                                  │
│   │ Evaluate     │◄─── Every 30 seconds (default)  │
│   │ Sensor       │                                  │
│   └──────┬───────┘                                  │
│          │                                          │
│          ▼                                          │
│   ┌──────────────┐     ┌──────────────┐            │
│   │ New records? │────►│ Yield        │            │
│   │              │ Yes │ RunRequest   │            │
│   └──────────────┘     └──────┬───────┘            │
│          │ No                 │                     │
│          ▼                    ▼                     │
│   ┌──────────────┐     ┌──────────────┐            │
│   │ Sleep        │     │ Start Run    │            │
│   │ 30 seconds   │     │              │            │
│   └──────────────┘     └──────────────┘            │
│                                                     │
└─────────────────────────────────────────────────────┘
```

## Configuration

### Minimum Interval

```python
@sensor(
    job=candidate_job,
    minimum_interval_seconds=60,  # Check every 60 seconds
)
def new_candidate_sensor(context):
    ...
```

### Default Status

```python
from dagster import DefaultSensorStatus

@sensor(
    job=candidate_job,
    default_status=DefaultSensorStatus.RUNNING,  # Start automatically
)
def new_candidate_sensor(context):
    ...
```

## Trade-offs

| Approach | Latency | Complexity | Cost |
|----------|---------|------------|------|
| Sensors (polling) | 30-60 seconds | Medium | DB queries |
| Webhooks + API | < 1 second | High | Infrastructure |
| Schedules | Minutes to hours | Low | Fixed |

## Related Concepts

- [Dynamic Partitions](05-dynamic-partitions.md)
- [Auto-Materialization Policies](02-auto-materialize-policies.md)
