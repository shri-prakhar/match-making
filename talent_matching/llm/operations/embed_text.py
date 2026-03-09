"""Text embedding operation using OpenRouter's embeddings API.

Generates vector embeddings for semantic search and matching.

Reference: https://openrouter.ai/docs/api/reference/embeddings
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from talent_matching.resources.openrouter import OpenRouterResource

from talent_matching.utils.llm_text_validation import require_meaningful_text_fields

# Version tracking for embedding changes
PROMPT_VERSION = "1.0.0"

# Default model for embeddings (cost-effective, 1536 dimensions)
DEFAULT_MODEL = "openai/text-embedding-3-small"


class EmbedTextResult:
    """Result of text embedding with usage stats for Dagster metadata."""

    def __init__(
        self,
        embeddings: list[list[float]],
        usage: dict[str, Any],
        model: str,
    ):
        self.embeddings = embeddings
        self.usage = usage
        self.model = model

    @property
    def input_tokens(self) -> int:
        return self.usage.get("prompt_tokens", 0)

    @property
    def total_tokens(self) -> int:
        return self.usage.get("total_tokens", self.input_tokens)

    @property
    def cost_usd(self) -> float:
        return float(self.usage.get("cost", 0))

    @property
    def dimensions(self) -> int:
        """Return the dimensionality of the embeddings."""
        if self.embeddings and len(self.embeddings) > 0:
            return len(self.embeddings[0])
        return 0


async def embed_text(
    openrouter: "OpenRouterResource",
    texts: list[str],
    model: str | None = None,
) -> EmbedTextResult:
    """Generate embeddings for one or more texts using OpenRouter.

    Args:
        openrouter: OpenRouterResource instance for API calls
        texts: List of texts to embed (batch processing supported)
        model: Embedding model to use (defaults to text-embedding-3-small)

    Returns:
        EmbedTextResult with embeddings, usage stats, and model for metadata
    """
    model = model or DEFAULT_MODEL
    validated_inputs = require_meaningful_text_fields(
        {f"embed_text[{idx}]": text for idx, text in enumerate(texts)},
        context="embed_text input validation",
    )
    texts = [validated_inputs[f"embed_text[{idx}]"] for idx in range(len(texts))]

    response = await openrouter.embed(
        input=texts,
        model=model,
        operation="embed_text",
    )

    # Extract embeddings from response
    embeddings = [item["embedding"] for item in response["data"]]
    usage = response.get("usage", {})

    return EmbedTextResult(embeddings=embeddings, usage=usage, model=model)


async def embed_single(
    openrouter: "OpenRouterResource",
    text: str,
    model: str | None = None,
) -> tuple[list[float], dict[str, Any]]:
    """Convenience function to embed a single text.

    Args:
        openrouter: OpenRouterResource instance for API calls
        text: Single text to embed
        model: Embedding model to use

    Returns:
        Tuple of (embedding vector, usage stats)
    """
    result = await embed_text(openrouter, [text], model)
    return result.embeddings[0], {"cost_usd": result.cost_usd, "model": result.model}
