# Dagster Scheduling, Partitions & Cancellation

How runs are scheduled, partitions are tracked, and what happens when the Dagster daemon shuts down.

---

## Storage Architecture

All Dagster metadata is persisted in **PostgreSQL** (the same database as application data). Nothing lives in memory only.

```mermaid
graph LR
    subgraph PostgreSQL
        RS["runs table\n(run metadata, status, config)"]
        EL["event_logs table\n(step events, asset materializations)"]
        SS["schedule_storage tables\n(sensor ticks, cursors, schedule state)"]
        DP["dynamic_partitions table\n(partition keys per namespace)"]
    end

    subgraph Dagster Processes
        WS["Webserver\n(dagster-webserver)"]
        DM["Daemon\n(dagster-daemon)"]
        RL["Run Launcher\n(child processes)"]
    end

    WS -->|read/write| RS
    WS -->|read| EL
    DM -->|read/write| SS
    DM -->|create runs| RS
    RL -->|write events| EL
    RL -->|read/write| DP
```

### What each table stores

| PostgreSQL Table | Purpose | Survives daemon restart? |
|---|---|---|
| `runs` | Run metadata: status, job name, partition key, run config, timestamps, tags | Yes |
| `run_tags` | Key-value tags per run (for filtering in UI) | Yes |
| `event_logs` | Step-level events: start, success, failure, retries, asset materializations, metadata | Yes |
| `secondary_indexes` | Indexes on event logs for fast queries | Yes |
| `schedule_ticks` / `job_ticks` | Sensor/schedule evaluation history, tick status, error messages | Yes |
| `instigators` | Sensor/schedule definitions, their ON/OFF state, **cursors** | Yes |
| `dynamic_partitions` | Registered partition keys per namespace (`candidates`, `jobs`) | Yes |
| `asset_keys` | Known asset keys and their metadata | Yes |
| `bulk_actions` | Backfill metadata (which partitions, progress, status) | Yes |

Because everything is in PostgreSQL, **no state is lost when the daemon restarts**.

---

## How Sensor-Driven Scheduling Works

This project uses no cron schedules. All automation is event-driven via sensors.

### Sensor Lifecycle

```mermaid
sequenceDiagram
    participant DB as PostgreSQL
    participant DM as Dagster Daemon
    participant S as Sensor Function
    participant RL as Run Launcher

    loop Every evaluation interval
        DM->>DB: Read sensor state + cursor
        DM->>S: Evaluate sensor(cursor)
        S->>S: Poll external source\n(Airtable API)

        alt New data found
            S->>DM: Yield RunRequest(s)\n+ update_cursor(new_cursor)
            DM->>DB: Write new cursor
            DM->>DB: Create run record\n(status = QUEUED)
            DM->>RL: Launch run
            RL->>DB: Write step events\nas run executes
        else No new data
            S->>DM: Yield nothing\n+ update_cursor(same)
            DM->>DB: Write tick\n(status = SKIPPED)
        end
    end
```

### Sensor Cursor Persistence

Each sensor stores a **cursor** — a string (typically JSON) that tracks its progress. The cursor is written to PostgreSQL after every evaluation.

**`airtable_candidate_sensor`** (every 15 min):

```json
{
  "initialized": true,
  "last_sync": "2026-02-23T12:00:00.000Z"
}
```

On first run (`initialized: false`), it does a full sync of all Airtable record IDs. On subsequent runs, it uses `last_sync` to fetch only records modified since that timestamp via Airtable's `LAST_MODIFIED_TIME()` formula filter.

**`airtable_job_matchmaking_sensor`** (every 5 min):

No meaningful cursor — it polls for jobs with `Start Matchmaking = true` on every tick. Idempotent because it unchecks the flag after triggering.

### What happens when the daemon shuts down?

```mermaid
flowchart TD
    A["Daemon shuts down\n(kill, restart, crash)"]

    A --> B["Sensor cursors\nSafe in PostgreSQL"]
    A --> C["Pending RunRequests\nnot yet created as runs"]
    A --> D["Runs already launched\n(in child processes)"]
    A --> E["Queued runs\n(created but not launched)"]

    B --> B1["On restart: daemon reads\nlast cursor from DB,\nresumes exactly where\nit left off"]

    C --> C1["Lost — sensor will\nre-evaluate on restart\nand re-discover them"]

    D --> D1["DefaultRunLauncher:\nchild processes may\ncontinue or be orphaned"]
    D1 --> D2["On restart: daemon\ndetects runs stuck in\nSTARTED status and\nmarks them FAILED"]

    E --> E1["Safe in DB — daemon\npicks them up and\nlaunches on restart"]

    style C1 fill:#fff3e0,stroke:#f9ab00
    style D1 fill:#fce8e6,stroke:#ea4335
    style B1 fill:#e6f4ea,stroke:#34a853
    style E1 fill:#e6f4ea,stroke:#34a853
```

**Key guarantees:**

1. **Cursors are safe.** The sensor cursor is written to PostgreSQL after each tick. On restart, the daemon reads the last saved cursor and resumes. At worst, the sensor re-processes a few records that were fetched in the last tick but whose RunRequests weren't persisted yet. The pipeline handles duplicates gracefully (upserts).

2. **Queued runs are safe.** Runs that were created in the `runs` table (status `QUEUED` or `NOT_STARTED`) survive the restart. The daemon picks them up and launches them.

3. **In-flight runs may be lost.** With `DefaultRunLauncher` (local process), child processes are tied to the daemon's process tree. If the daemon is killed (`SIGKILL`), child processes may become orphaned. On restart, the daemon detects runs stuck in `STARTED` or `CANCELING` status and marks them `FAILURE` after a timeout.

4. **Backfills are safe.** Backfill progress (which partitions completed, which are pending) is tracked in the `bulk_actions` table. On restart, the daemon resumes submitting the remaining partitions.

---

## Dynamic Partitions

### How partitions are registered

Two partition namespaces exist:

| Namespace | Partition Key | Registered By |
|---|---|---|
| `candidates` | Airtable record ID (e.g., `recABC123`) | `airtable_candidate_sensor` or `sync_airtable_candidates_job` |
| `jobs` | Airtable job record ID (e.g., `recXYZ789`) | `sync_airtable_jobs_job` (manual) |

Registration happens via:

```python
context.instance.add_dynamic_partitions(
    partitions_def_name="candidates",
    partition_keys=["recABC123", "recDEF456"]
)
```

This inserts rows into the `dynamic_partitions` PostgreSQL table. Partition keys are permanent — they persist across daemon restarts and are never auto-deleted.

### How partitioned runs work

```mermaid
flowchart TD
    subgraph Registration
        R1["Sensor detects new\nAirtable record recABC123"]
        R2["Calls add_dynamic_partitions\n→ stored in PostgreSQL"]
        R3["Yields RunRequest\nwith partition_key='recABC123'"]
        R1 --> R2 --> R3
    end

    subgraph Execution
        E1["Run created in 'runs' table\nwith partition_key tag"]
        E2["Each asset materializes\nfor partition recABC123"]
        E3["IO Manager reads/writes\nusing partition_key as\nthe record identifier"]
        R3 --> E1 --> E2 --> E3
    end

    subgraph Tracking
        T1["Asset materialization\nevent stored per\n(asset_key, partition_key)"]
        T2["Dagster UI shows\nper-partition status:\n✅ materialized\n⏳ stale\n❌ failed"]
        E3 --> T1 --> T2
    end
```

### Backfills

To re-process all candidates (e.g., after a prompt change), use **backfills** in the Dagster UI:

1. Go to the asset or job in the UI
2. Click "Materialize" or "Launch Backfill"
3. Select partition range (all, specific partitions, or a filter)
4. Dagster creates one run per partition (or batches them)

Backfill state is stored in `bulk_actions`:

| Field | Description |
|---|---|
| `backfill_id` | Unique identifier |
| `partition_names` | List of partitions to process |
| `status` | `REQUESTED` → `COMPLETED` / `FAILED` / `CANCELED` |
| `num_partitions` | Total count |
| `num_cancelable` | Remaining partitions that can be canceled |

The daemon submits partitions in batches and tracks progress. If the daemon restarts mid-backfill, it reads the `bulk_actions` table and resumes with the remaining partitions.

---

## Run Lifecycle & Cancellation

### Run Status State Machine

```mermaid
stateDiagram-v2
    [*] --> QUEUED: RunRequest created
    QUEUED --> NOT_STARTED: Daemon picks up
    NOT_STARTED --> STARTING: Run launcher begins
    STARTING --> STARTED: Child process running

    STARTED --> SUCCESS: All steps complete
    STARTED --> FAILURE: Step fails (after retries)

    STARTED --> CANCELING: User cancels
    CANCELING --> CANCELED: Processes terminated

    QUEUED --> CANCELED: User cancels before launch

    NOT_STARTED --> FAILURE: Launch error

    STARTING --> FAILURE: Process spawn fails
    CANCELING --> FAILURE: Termination timeout

    SUCCESS --> [*]
    FAILURE --> [*]
    CANCELED --> [*]
```

All status transitions are written to the `runs` table in PostgreSQL.

### How cancellation works

```mermaid
sequenceDiagram
    participant U as User (UI/CLI)
    participant DB as PostgreSQL
    participant DM as Daemon
    participant RL as Run Process

    U->>DB: Set run status = CANCELING
    DM->>DB: Poll detects CANCELING run
    DM->>RL: Send termination signal\n(via termination thread)
    RL->>RL: Interrupt current step
    RL->>DB: Write step failure events
    RL->>DB: Set run status = CANCELED
    Note over RL: Process exits

    alt Process doesn't respond
        DM->>DM: Wait for timeout
        DM->>RL: SIGKILL
        DM->>DB: Set run status = CANCELED
    end
```

The `DefaultRunLauncher` uses local subprocesses, so cancellation sends OS signals (`SIGINT` → `SIGKILL`). Each subprocess has a **termination thread** that monitors a shared event and triggers cleanup.

**Long-running async steps (e.g. OpenRouter LLM):** If a step blocks inside `asyncio.run(openrouter_call)` for a long time, the main thread may not process the termination signal until the call returns, so the run can stay in CANCELING. Assets that do long LLM/API work use `run_with_interrupt_check(context, coro)` from `talent_matching.utils.dagster_async`: it runs the coroutine alongside a task that polls run status and raises when the run is CANCELING, so the step exits promptly and the run transitions to CANCELED.

#### Where run time is spent (normalized_candidates)

For a run that was cancelled while in `normalized_candidates`, the logs look like:

| Time (log)       | Event | Code path |
|------------------|-------|-----------|
| 11:12:08         | Started process for run | Daemon / run launcher |
| 11:12:08         | Started execution for "__ASSET_JOB" | Executor |
| 11:12:08–09      | Executing steps: normalized_candidates, candidate_vectors | `talent_matching/jobs` (asset job) |
| 11:12:09         | Launching subprocess for "normalized_candidates" | Multiprocess executor |
| 11:12:10–11      | Executing step in subprocess; init resources [openrouter, postgres_io] | `talent_matching/assets/candidates.py` → resource init |
| 11:12:11         | Loaded input "raw_candidates" using input manager "postgres_io" | `postgres_io.load_input` |
| 11:12:11         | **Normalizing candidate rec… via OpenRouter API (sources: airtable)** | `normalized_candidates()` just before `asyncio.run(run_with_interrupt_check(..., normalize_cv(...)))` |
| 11:12:11 → 11:17 | *No further step logs until cancel* | **Blocked here** |
| 11:17:13         | Sending run termination request | Daemon (run monitor or user/API) |

So the run was blocked for **~5 minutes** inside the OpenRouter call. The exact call chain:

1. **Asset:** `normalized_candidates()` in `talent_matching/assets/candidates.py` builds `cv_text_airtable` / `cv_text_pdf`, then calls `asyncio.run(run_with_interrupt_check(context, normalize_cv(...)))`.
2. **LLM op:** `normalize_cv()` in `talent_matching/llm/operations/normalize_cv.py` builds `user_content` via `_build_cv_content()`, then `await openrouter.complete(messages=[system, user], model=..., response_format={"type": "json_object"}, temperature=0.0)`.
3. **HTTP:** `OpenRouterResource.complete()` in `talent_matching/resources/openrouter.py` does a single `await client.post("https://openrouter.ai/api/v1/chat/completions", json=request_body, timeout=120.0)`.

**Where the time went:** Almost all of the ~5 minutes was spent in that **single `client.post()`** to OpenRouter (chat completions). The request body is large (long system prompt in `normalize_cv.SYSTEM_PROMPT` plus full CV text), and the response is a large JSON (skills, experience, narratives, etc.). So the bottleneck is **OpenRouter’s LLM latency** (model + network + output size). The step does not proceed until the full response is received; then it does `response.json()`, `_log_cost`, `await _store_cost_record(...)`, and returns. Cost logging and DB insert are fast; they do not explain the 5‑minute gap.

**Note:** `timeout=120.0` is set on the POST. If the API regularly takes >2 minutes, the request would normally raise a timeout; a 5‑minute wait before cancel can occur if the run was terminated by run monitoring (e.g. `max_runtime_seconds`) before the HTTP timeout fired, or if the server/OpenRouter behavior differs (e.g. slow streaming or keep-alive).

### Retry policies

API-heavy jobs use exponential backoff retries:

```
Max retries: 3
Delay: 1s base
Backoff: EXPONENTIAL (1s → 2s → 4s)
Jitter: PLUS_MINUS (randomized to avoid thundering herd)
```

Applied to: `candidate_pipeline_job`, `job_pipeline_job`, `matchmaking_with_feedback_job`

Retries happen **within the same run** — no new run is created. Each retry attempt is logged as a separate event in the event log. After exhausting retries, the step fails and the run status depends on whether other steps can continue.

---

## Concurrency Control

### How concurrency pools work

```yaml
# dagster.yaml
concurrency:
  pools:
    default_limit: 50
```

Assets are tagged with concurrency keys:

| Concurrency Key | Assets | Purpose |
|---|---|---|
| `airtable_api` | `airtable_candidates`, `airtable_jobs`, `airtable_candidate_sync`, `airtable_job_sync` | Prevent Airtable rate limiting |
| `openrouter_api` | `normalized_candidates`, `normalized_jobs`, `candidate_vectors`, `job_vectors`, `candidate_role_fitness` | Prevent OpenRouter rate limiting |

When multiple partitions run concurrently (e.g., during a backfill), Dagster ensures no more than 50 ops with the same concurrency key execute simultaneously. Excess ops are **queued** (not rejected) and execute as slots free up.

```mermaid
flowchart LR
    subgraph "Backfill: 200 candidates"
        P1["Partition 1\nnormalize_cv"]
        P2["Partition 2\nnormalize_cv"]
        P3["Partition 3\nnormalize_cv"]
        PN["... Partition N"]
    end

    subgraph "Concurrency Pool: openrouter_api (limit 50)"
        S1["Slot 1"]
        S2["Slot 2"]
        S50["Slot 50"]
        Q["Queue\n(remaining partitions wait)"]
    end

    P1 --> S1
    P2 --> S2
    P3 --> S50
    PN --> Q
```

---

## Summary: What survives a daemon restart?

| State | Persisted in PostgreSQL? | Behavior on restart |
|---|---|---|
| Sensor cursors | Yes (`instigators` table) | Resume from last cursor |
| Sensor ON/OFF state | Yes (`instigators` table) | Sensors stay in same state |
| Registered partitions | Yes (`dynamic_partitions` table) | All partitions preserved |
| Queued runs | Yes (`runs` table) | Launched by daemon |
| In-flight runs | Yes (status in `runs`) | Detected as stuck → marked `FAILURE` |
| Backfill progress | Yes (`bulk_actions` table) | Remaining partitions submitted |
| Run history & logs | Yes (`runs` + `event_logs`) | Fully queryable |
| Concurrency slot usage | Ephemeral | Recomputed from active runs |
| Pending RunRequests (from sensor yield, not yet persisted) | No | Sensor re-evaluates and re-discovers |
