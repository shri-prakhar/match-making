"""OpenRouter LLM resource with automatic cost tracking.

This resource is a thin HTTP client for OpenRouter API. It handles:
- Authentication
- Request formatting
- Cost tracking (stored in PostgreSQL llm_costs table)
- Timeout and retry (configurable) for slow or transient failures

LLM operations (prompts and parsing) are in talent_matching.llm.operations.
"""

import asyncio
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field, PrivateAttr

from talent_matching.db import get_session
from talent_matching.models.llm_costs import LLMCost

# HTTP status codes that trigger a retry (rate limit, gateway/upstream errors)
_OPENROUTER_RETRYABLE_STATUS_CODES = (429, 502, 503, 504)


@dataclass
class LLMContext:
    """Context for tracking LLM costs per asset/run."""

    run_id: str = ""
    asset_key: str = ""
    partition_key: str = ""
    code_version: str = ""


@dataclass
class RunCostAccumulator:
    """Accumulates LLM costs across all assets in a run."""

    total_cost_usd: Decimal = Decimal("0")
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    api_calls: int = 0
    costs_by_asset: dict = None  # asset_key -> cost

    def __post_init__(self):
        if self.costs_by_asset is None:
            self.costs_by_asset = {}

    def add(self, asset_key: str, cost_usd: Decimal, input_tokens: int, output_tokens: int):
        """Record a cost."""
        self.total_cost_usd += cost_usd
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        self.api_calls += 1
        self.costs_by_asset[asset_key] = self.costs_by_asset.get(asset_key, Decimal("0")) + cost_usd

    @property
    def total_tokens(self) -> int:
        return self.total_input_tokens + self.total_output_tokens

    def to_metadata(self) -> dict:
        """Return as Dagster metadata dict."""
        return {
            "llm/total_cost_usd": float(self.total_cost_usd),
            "llm/total_tokens": self.total_tokens,
            "llm/api_calls": self.api_calls,
            "llm/costs_by_asset": {k: float(v) for k, v in self.costs_by_asset.items()},
        }


class OpenRouterResource(ConfigurableResource):
    """OpenRouter LLM resource with built-in cost tracking.

    This is a thin HTTP client. Use operations from talent_matching.llm for
    specific tasks like CV normalization.

    Example usage in an asset:
        from talent_matching.llm import normalize_cv, CV_PROMPT_VERSION

        openrouter.set_context(
            run_id=context.run_id,
            asset_key="normalized_candidates",
            partition_key=context.partition_key,
            code_version=CV_PROMPT_VERSION,
        )
        result = asyncio.run(normalize_cv(openrouter, cv_text))
    """

    api_key: str = Field(
        default_factory=lambda: os.getenv("OPENROUTER_API_KEY", ""),
        description="OpenRouter API key",
    )
    default_model: str = Field(
        default="openai/gpt-4o-mini",
        description="Default model to use for completions",
    )
    site_url: str = Field(
        default="https://superteam.fun",
        description="Site URL for OpenRouter analytics",
    )
    app_name: str = Field(
        default="Talent Matching Pipeline",
        description="Application name for OpenRouter analytics",
    )
    request_timeout_seconds: float = Field(
        default=120.0,
        description="HTTP request timeout per attempt (seconds). Long CVs can need 60–120s.",
    )
    max_retries: int = Field(
        default=3,
        description="Max retries on timeout or transient errors (429, 5xx). Total attempts = 1 + max_retries.",
    )
    retry_base_delay_seconds: float = Field(
        default=2.0,
        description="Base delay in seconds for exponential backoff between retries (2, 4, 8, ...).",
    )
    # Internal state (not configurable, uses Pydantic PrivateAttr)
    _context: LLMContext = PrivateAttr(default_factory=LLMContext)
    _run_costs: RunCostAccumulator = PrivateAttr(default_factory=RunCostAccumulator)

    def set_context(
        self,
        run_id: str,
        asset_key: str,
        partition_key: str = "",
        code_version: str = "",
    ) -> None:
        """Set context for cost tracking. Call this at start of each asset.

        Args:
            run_id: Dagster run ID
            asset_key: Name of the asset being processed
            partition_key: Partition key if applicable
            code_version: Prompt version from the operation module
        """
        self._context = LLMContext(
            run_id=run_id,
            asset_key=asset_key,
            partition_key=partition_key,
            code_version=code_version,
        )

    async def _post_with_retry(
        self,
        url: str,
        headers: dict[str, str],
        json_body: dict[str, Any],
    ) -> httpx.Response:
        """POST with timeout and exponential-backoff retry on timeout or 429/5xx."""
        last_error: BaseException | None = None
        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                delay = self.retry_base_delay_seconds * (2 ** (attempt - 1))
                get_dagster_logger().warning(
                    f"OpenRouter retry {attempt}/{self.max_retries} after {delay:.1f}s: {last_error}"
                )
                await asyncio.sleep(delay)
            async with httpx.AsyncClient() as client:
                try:
                    response = await client.post(
                        url,
                        headers=headers,
                        json=json_body,
                        timeout=self.request_timeout_seconds,
                    )
                    if response.is_success:
                        return response
                    if response.status_code in _OPENROUTER_RETRYABLE_STATUS_CODES:
                        last_error = httpx.HTTPStatusError(
                            f"OpenRouter {response.status_code}",
                            request=response.request,
                            response=response,
                        )
                        continue
                    response.raise_for_status()
                    return response
                except httpx.TimeoutException as e:
                    last_error = e
                    continue
        if last_error is not None:
            raise last_error
        raise RuntimeError("retry loop ended without response or error")

    async def _store_cost_record(
        self,
        operation: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: Decimal,
    ) -> None:
        """Store cost record in PostgreSQL and accumulate for run totals.

        Uses a sync database operation wrapped in asyncio.to_thread to avoid
        blocking the event loop.
        """
        self._run_costs.add(
            asset_key=self._context.asset_key or "unknown",
            cost_usd=cost_usd,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

        def _insert_record() -> None:
            session = get_session()
            cost_record = LLMCost(
                run_id=self._context.run_id or "unknown",
                asset_key=self._context.asset_key or "unknown",
                partition_key=self._context.partition_key or None,
                operation=operation,
                model=model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost_usd=cost_usd,
                code_version=self._context.code_version or None,
            )
            session.add(cost_record)
            session.commit()
            session.close()

        await asyncio.to_thread(_insert_record)

    def get_run_costs(self) -> RunCostAccumulator:
        """Get accumulated costs for the current run.

        Call this at the end of a job to get totals across all assets.
        Useful for outputting run-level metadata.
        """
        return self._run_costs

    def reset_run_costs(self) -> None:
        """Reset the run cost accumulator. Call at start of a new run."""
        self._run_costs = RunCostAccumulator()

    def _log_cost(
        self,
        operation: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: Decimal,
    ) -> None:
        """Log cost to Dagster logger."""
        logger = get_dagster_logger()
        logger.info(
            f"LLM Cost: {operation} | {model} | "
            f"{input_tokens}+{output_tokens} tokens | ${cost_usd:.6f}"
        )

    async def complete(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        operation: str = "completion",
        response_format: dict[str, str] | None = None,
        temperature: float = 0.0,
        max_tokens: int | None = None,
        plugins: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Make an async completion request and track costs.

        Args:
            messages: List of message dicts with 'role' and 'content'
            model: Model to use (defaults to default_model)
            operation: Operation type for cost tracking
            response_format: Response format (e.g., {"type": "json_object"})
            temperature: Sampling temperature (0.0 = deterministic)
            max_tokens: Maximum tokens in response
            plugins: OpenRouter plugins config (e.g., for PDF processing engine)

        Returns:
            Full API response dict including usage information
        """
        model = model or self.default_model

        request_body: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }

        if response_format:
            request_body["response_format"] = response_format

        if max_tokens:
            request_body["max_tokens"] = max_tokens

        if plugins:
            request_body["plugins"] = plugins

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": self.site_url,
            "X-Title": self.app_name,
        }
        response = await self._post_with_retry(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json_body=request_body,
        )
        data = response.json()

        # Extract usage from response (always included by OpenRouter)
        usage = data.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        # OpenRouter returns cost directly - no need to calculate
        cost_usd = Decimal(str(usage.get("cost", 0)))

        # Log to Dagster
        self._log_cost(operation, model, input_tokens, output_tokens, cost_usd)

        # Store in database
        await self._store_cost_record(
            operation=operation,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
        )

        return data

    async def embed(
        self,
        input: str | list[str],
        model: str = "openai/text-embedding-3-small",
        operation: str = "embed",
    ) -> dict[str, Any]:
        """Generate embeddings using OpenRouter's embeddings API.

        Reference: https://openrouter.ai/docs/api/reference/embeddings

        Args:
            input: Text or list of texts to embed
            model: Embedding model to use
            operation: Operation type for cost tracking

        Returns:
            Full API response dict including embeddings and usage
        """
        # Ensure input is a list for consistent handling
        if isinstance(input, str):
            input = [input]

        request_body: dict[str, Any] = {
            "model": model,
            "input": input,
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": self.site_url,
            "X-Title": self.app_name,
        }
        response = await self._post_with_retry(
            "https://openrouter.ai/api/v1/embeddings",
            headers=headers,
            json_body=request_body,
        )
        data = response.json()

        # Extract usage from response
        usage = data.get("usage", {})
        input_tokens = usage.get("prompt_tokens", usage.get("total_tokens", 0))
        output_tokens = 0  # Embeddings don't have output tokens
        cost_usd = Decimal(str(usage.get("cost", 0)))

        # Log to Dagster
        self._log_cost(operation, model, input_tokens, output_tokens, cost_usd)

        # Store in database
        await self._store_cost_record(
            operation=operation,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
        )

        return data
