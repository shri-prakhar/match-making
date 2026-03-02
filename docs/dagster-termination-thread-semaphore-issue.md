# Dagster termination thread and macOS semaphore issue

This document summarizes findings for an issue that can cause runs to fail on macOS with errors related to semaphores (e.g. “no space left on device” or resource exhaustion) when using the multiprocess executor. It is intended to help others investigate, work around, or report the problem upstream.

## Summary

- **What:** A single line in Dagster’s `_utils` creates an extra `multiprocessing.Event()` (and thus an extra semaphore) in child processes every time the termination thread is started.
- **Where:** `dagster/_utils/__init__.py`, in `start_termination_thread()`.
- **Impact:** Under load (many gRPC run worker processes, multiprocess execution), this can contribute to hitting the system semaphore limit on macOS and cause run failures.
- **Status:** Present in Dagster 1.12.14 and 1.12.15; also still present in upstream `dagster-io/dagster` master. Not a “wrong version” issue—no released version fixes it yet.

## The bug

In `start_termination_thread()`:

```python
def start_termination_thread(
    should_stop_event: threading.Event, is_done_event: threading.Event
) -> None:
    check.inst_param(should_stop_event, "should_stop_event", ttype=type(multiprocessing.Event()))
    # ...
```

- **Problem:** `ttype=type(multiprocessing.Event())` evaluates `multiprocessing.Event()` to get its type. That call **creates a new `Event` instance** (and its underlying semaphore) in the current process. Each `multiprocessing.Event()` allocates a `Lock`, a `Condition` (which itself creates a `Lock` + `Semaphore`), totalling **3 POSIX named semaphores** per spurious call.
- **Context:** This function runs in **child processes** (gRPC run workers). Dagster uses `multiprocessing.get_context("spawn")` for those, so each child imports this module and can execute this path. Creating an extra `Event()` in each child adds unnecessary semaphores and can contribute to semaphore exhaustion on macOS (default limit: 10,000 via `kern.posix.sem.max`).

**Correct approach for the check:** Reference the class directly as `ttype=multiprocessing.synchronize.Event`. This requires adding `import multiprocessing.synchronize` to the file's imports (plain `import multiprocessing` does **not** auto-import the `synchronize` submodule).

> **Warning:** `multiprocessing.Event` is a **bound method** on the default context object, not a class. Using `ttype=multiprocessing.Event` would cause `isinstance()` to raise `TypeError`. The actual class lives at `multiprocessing.synchronize.Event`.

## Why the termination thread exists

The comment above `start_termination_thread` explains that Dagster uses an **event-based** approach (a daemon thread that waits on `should_stop_event` and then calls `send_interrupt()`) so that runs can be cancelled cleanly on **Windows**, where POSIX signals are not available. The alternative would be to create processes with `CREATE_NEW_PROCESS_GROUP` and send `CTRL_BREAK_EVENT`; the current design avoids that.

References cited in the code (for the curious):

- [How to handle SIGINT on Windows (Stack Overflow)](https://stackoverflow.com/questions/35772001/how-to-handle-the-signal-in-python-on-windows-machine)
- [Handling sub-process hierarchies (Linux/OS X vs Windows) – Stefan Scherfke](https://stefan.sofa-rockers.org/2013/08/15/handling-sub-process-hierarchies-python-linux-os-x/)

These describe the alternative (process groups + CTRL_BREAK); they do not fix this bug.

## What we checked

| Check | Result |
|-------|--------|
| Project Dagster version | `pyproject.toml`: `dagster>=1.6.0`; lockfile had 1.12.14, upgraded to 1.12.15. |
| Upstream master | Same `ttype=type(multiprocessing.Event())` line present in `dagster-io/dagster` master. |
| Fix in 1.12.15? | No; the line is unchanged in 1.12.15. |
| Related GitHub issue | [#18728](https://github.com/dagster-io/dagster/issues/18728) – “Ctrl+C can cause hang” (closed as stale, not fixed). |

Conclusion: this is an upstream bug, not a wrong or outdated Dagster version in this project.

## Workarounds and next steps

1. **Local venv patch (two changes)**
   In the installed package
   `site-packages/dagster/_utils/__init__.py`:

   a. Add the import (after `import multiprocessing`):
   ```python
   import multiprocessing.synchronize
   ```

   b. Change the `check.inst_param` call:
   - `ttype=type(multiprocessing.Event())`
   to:
   - `ttype=multiprocessing.synchronize.Event`

   When implemented, a script at `scripts/patch_dagster_semaphore.py` can automate this; run it after any `poetry install` or dagster upgrade.

   > **Do NOT use** `ttype=multiprocessing.Event` — that is a bound method, not a class, and will raise `TypeError` at runtime.

2. **Reduce concurrency**
   Lower the number of gRPC run workers or overall parallelism (e.g. in `dagster.yaml` or run config) so that fewer child processes and thus fewer semaphores are created. This can avoid hitting the limit without patching.

3. **Report upstream**
   Open an issue or PR at [dagster-io/dagster](https://github.com/dagster-io/dagster) describing:
   - The unnecessary `Event()` creation in `check.inst_param(..., ttype=type(multiprocessing.Event()))` in `start_termination_thread`,
   - The impact on macOS (and possibly other systems) when many child processes are used,
   - The suggested fix: add `import multiprocessing.synchronize` and use `ttype=multiprocessing.synchronize.Event` (the actual class) instead of `type(multiprocessing.Event())`.

## File and function reference

- **Repository:** [dagster-io/dagster](https://github.com/dagster-io/dagster)
- **File:** `python_modules/dagster/dagster/_utils/__init__.py`
- **Function:** `start_termination_thread(should_stop_event, is_done_event)`
- **Line (approximate):** ~374 (varies by version)

Call site (for context): `MultiprocessExecutorChildProcessCommand.execute()` in `dagster/_core/executor/multiprocess.py` calls `start_termination_thread(self.term_event, done_event)` where `term_event` is a `multiprocessing.Event` passed into the child process.
