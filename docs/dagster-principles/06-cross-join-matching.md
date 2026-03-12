# Principle: Cross-Join Matching Pattern

## Problem

In a two-sided marketplace (candidates and jobs), when a new entity arrives on one side, you need to match it against ALL entities on the other side:

- New candidate → Match against ALL jobs
- New job → Match against ALL candidates

## Solution

Use partitioned assets for incremental processing, but at the matching step, load all entities from the opposite side from the database.

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                    Cross-Join Matching Pattern                     │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  CANDIDATE SIDE (Partitioned)          JOB SIDE (Partitioned)     │
│                                                                    │
│  raw_candidates[X]                     raw_jobs[Y]                │
│        │                                     │                     │
│        ▼                                     ▼                     │
│  normalized_candidates[X]              normalized_jobs[Y]         │
│        │                                     │                     │
│        ▼                                     ▼                     │
│  candidate_vectors[X]                  job_vectors[Y]             │
│        │                                     │                     │
│        ▼                                     ▼                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                      MATCHING LAYER                         │  │
│  ├─────────────────────────────────────────────────────────────┤  │
│  │                                                             │  │
│  │  candidate_matches[X]          job_matches[Y]               │  │
│  │  ┌─────────────────────┐       ┌─────────────────────┐     │  │
│  │  │ Load vectors[X]     │       │ Load vectors[Y]     │     │  │
│  │  │ Load ALL job_vectors│       │ Load ALL cand_vecs  │     │  │
│  │  │ Compute similarities│       │ Compute similarities│     │  │
│  │  │ Return top 50 jobs  │       │ Return top 50 cands │     │  │
│  │  └─────────────────────┘       └─────────────────────┘     │  │
│  │                                                             │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

## Implementation

### Candidate-Side Matching

```python
from dagster import asset, AssetExecutionContext

@asset(
    partitions_def=candidate_partitions,
    group_name="matching",
)
def candidate_matches(
    context: AssetExecutionContext,
    candidate_vectors: dict,
) -> list[dict]:
    """Match ONE candidate against ALL jobs."""
    candidate_id = context.partition_key

    # Load this candidate's vectors (from upstream asset)
    cand_vecs = candidate_vectors

    # Load ALL job vectors from database (not from Dagster assets)
    all_job_vectors = load_all_job_vectors_from_db()

    # Compute match scores
    matches = []
    for job in all_job_vectors:
        score = compute_match_score(
            candidate_vectors=cand_vecs,
            job_vectors=job,
        )
        matches.append({
            "candidate_id": candidate_id,
            "job_id": job["job_id"],
            "match_score": score["final"],
            "keyword_score": score["keyword"],
            "vector_score": score["vector"],
        })

    # Return top matches
    return sorted(matches, key=lambda x: x["match_score"], reverse=True)[:50]
```

### Job-Side Matching

```python
@asset(
    partitions_def=job_partitions,
    group_name="matching",
)
def job_matches(
    context: AssetExecutionContext,
    job_vectors: dict,
) -> list[dict]:
    """Match ONE job against ALL candidates."""
    job_id = context.partition_key

    # Load this job's vectors
    job_vecs = job_vectors

    # Load ALL candidate vectors from database
    all_candidate_vectors = load_all_candidate_vectors_from_db()

    # Compute match scores
    matches = []
    for candidate in all_candidate_vectors:
        score = compute_match_score(
            candidate_vectors=candidate,
            job_vectors=job_vecs,
        )
        matches.append({
            "job_id": job_id,
            "candidate_id": candidate["candidate_id"],
            "match_score": score["final"],
            "keyword_score": score["keyword"],
            "vector_score": score["vector"],
        })

    return sorted(matches, key=lambda x: x["match_score"], reverse=True)[:50]
```

### Database Helper Functions

```python
def load_all_job_vectors_from_db() -> list[dict]:
    """Load all job vectors directly from PostgreSQL."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT job_id, vector_type, vector
        FROM job_vectors
        WHERE vector_type = 'role_description'
    """)

    results = []
    for row in cursor.fetchall():
        results.append({
            "job_id": row[0],
            "vector_type": row[1],
            "vector": parse_vector(row[2]),
        })

    cursor.close()
    conn.close()
    return results
```

## Why Not Use Dagster Dependencies?

You might ask: why load from DB instead of depending on the job_vectors asset?

**Cross-partition dependencies are complex:**
- Dagster assets depend on specific partitions
- To match candidate X against ALL jobs, you'd need to depend on ALL job partitions
- This creates O(n×m) dependencies, which doesn't scale

**Database access is simpler:**
- Vectors are already stored in pgvector
- Loading from DB bypasses partition complexity
- pgvector provides efficient similarity search

## Optimized Matching with pgvector

Instead of loading all vectors and computing in Python, use pgvector's built-in similarity search:

```python
def find_top_job_matches(candidate_vector: list[float], limit: int = 50):
    """Use pgvector for efficient similarity search."""
    conn = get_db_connection()
    cursor = conn.cursor()

    vector_str = "[" + ",".join(str(v) for v in candidate_vector) + "]"

    cursor.execute("""
        SELECT job_id,
               1 - (vector <=> %s::vector) as similarity
        FROM job_vectors
        WHERE vector_type = 'role_description'
        ORDER BY vector <=> %s::vector
        LIMIT %s
    """, (vector_str, vector_str, limit))

    return cursor.fetchall()
```

This is much faster than loading all vectors into Python.

## Full Flow Example

```
1. New candidate "candidate-123" arrives
   └── Sensor detects, adds partition, triggers run

2. Pipeline processes candidate-123:
   └── raw_candidates[123] → normalized[123] → vectors[123]

3. Matching step:
   └── candidate_matches[123]:
       ├── Receives vectors[123] from upstream
       ├── Queries pgvector: "Find 50 jobs most similar to this candidate"
       └── Stores results in matches table

4. Result:
   └── candidate-123 now has ranked list of best-fit jobs
```

## Trade-offs

| Approach | Pros | Cons |
|----------|------|------|
| Cross-join in Python | Full control | Slow for large datasets |
| pgvector similarity | Fast, scalable | Limited to vector similarity |
| Hybrid (filter + rank) | Best of both | More complex |

## Related Concepts

- [Dynamic Partitions](05-dynamic-partitions.md)
- [Sensors](04-sensors.md)
