"""Helpers for running async work in Dagster assets with run cancellation support.

Long-running async calls (e.g. OpenRouter LLM) can block the step from responding
to "run termination request", leaving runs stuck in CANCELING. Running the
coroutine alongside a task that polls run status and raises on cancel ensures
the step exits promptly so the run can transition to CANCELED.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

from dagster import (
    AssetExecutionContext,
    DagsterExecutionInterruptedError,
    DagsterRunStatus,
)

T = TypeVar("T")


async def run_with_interrupt_check(
    context: AssetExecutionContext,
    main_coro: Coroutine[Any, Any, T],
    poll_interval_seconds: float = 1.0,
) -> T:
    """Run an async coroutine but poll run status and raise on cancel.

    Use when the coroutine may block for a long time (e.g. LLM API call) so
    that cancelling the run in the UI causes the step to exit instead of
    staying stuck in CANCELING.
    """
    main_task = asyncio.create_task(main_coro)
    loop = asyncio.get_running_loop()

    def check_run_canceled() -> None:
        run = context.instance.get_run_by_id(context.run_id)
        if run and run.status == DagsterRunStatus.CANCELING:
            raise DagsterExecutionInterruptedError()

    async def cancel_checker() -> None:
        while True:
            await asyncio.sleep(poll_interval_seconds)
            await loop.run_in_executor(None, check_run_canceled)

    check_task = asyncio.create_task(cancel_checker())
    done: set[asyncio.Task]
    pending: set[asyncio.Task]
    done, pending = await asyncio.wait([main_task, check_task], return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    if main_task in done:
        return main_task.result()
    await check_task
    raise AssertionError("unreachable")
