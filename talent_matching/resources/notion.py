"""Notion API resource for fetching page content.

Used by the job pipeline to resolve job description text from Notion page URLs
(e.g. when Airtable row contains only a link to a Notion job description).
"""

import logging
import re
from typing import Any

import httpx
from dagster import ConfigurableResource
from pydantic import Field

logger = logging.getLogger(__name__)


def extract_notion_page_id(url: str) -> str | None:
    """Extract Notion page ID from a notion.so or notion.site URL.

    Examples:
        https://www.notion.so/Page-Title-22b4d743fe738009af15d28c56d15aaf -> 22b4d743fe738009af15d28c56d15aaf
        https://cliff-indigo-30c3.notion.site/Senior-Frontend-Engineer-22b4d743fe738009af15d28c56d15aaf -> 22b4d743fe738009af15d28c56d15aaf
    """
    if not url or "notion" not in url.lower():
        return None
    # Match 32-char hex (with optional dashes) at end of path or before query/fragment
    match = re.search(
        r"([a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12})", url, re.I
    )
    if match:
        return match.group(1).replace("-", "")
    return None


def _rich_text_to_plain(block: dict[str, Any]) -> str:
    """Extract plain text from a block's rich_text array."""
    text_parts = []
    for item in block.get("rich_text", []):
        if isinstance(item, dict) and "plain_text" in item:
            text_parts.append(item["plain_text"])
    return "".join(text_parts)


def _block_to_text(block: dict[str, Any]) -> str:
    """Convert a single block to a line of text. Handles common block types."""
    block_type = block.get("type")
    if block_type not in block:
        return ""
    data = block[block_type]
    plain = _rich_text_to_plain(data)
    if not plain:
        return ""
    if block_type == "heading_1":
        return f"# {plain}\n"
    if block_type == "heading_2":
        return f"## {plain}\n"
    if block_type == "heading_3":
        return f"### {plain}\n"
    if block_type in ("bulleted_list_item", "numbered_list_item", "to_do"):
        return f"- {plain}\n"
    return plain + "\n"


def _extract_site_origin(url: str) -> str | None:
    """Extract the origin (scheme + host) for the public Notion internal API.

    - notion.site: returns that origin (e.g. https://cliff-indigo-30c3.notion.site).
    - notion.so: returns https://www.notion.so so we can try loadPageChunk there too.
    Returns None only if the URL is not a known Notion host.
    """
    if not url or "notion" not in url.lower():
        return None
    site_match = re.match(r"(https?://[^/]*notion\.site)", url, re.I)
    if site_match:
        return site_match.group(1)
    if "notion.so" in url.lower():
        return "https://www.notion.so"
    return None


def _format_page_id_with_dashes(raw_id: str) -> str:
    """Convert a 32-char hex page ID to the dashed UUID format Notion expects."""
    h = raw_id.replace("-", "")
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def _fetch_public_page_content(page_url: str, page_id: str) -> str | None:
    """Fetch content from a publicly-published Notion page via the internal API.

    Works for pages published to the web: ``*.notion.site`` URLs use that origin;
    ``notion.so`` URLs use https://www.notion.so so the same page (when shared
    publicly) can be fetched without a Notion integration.
    """
    origin = _extract_site_origin(page_url)
    if not origin:
        return None

    dashed_id = _format_page_id_with_dashes(page_id)
    endpoint = f"{origin}/api/v3/loadPageChunk"

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                endpoint,
                json={
                    "page": {"id": dashed_id},
                    "limit": 100,
                    "cursor": {"stack": []},
                    "chunkNumber": 0,
                    "verticalColumns": False,
                },
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            data = response.json()
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Public Notion fetch failed for %s: %s", page_id, exc)
        return None

    blocks = data.get("recordMap", {}).get("block", {})
    texts: list[str] = []
    for block_data in blocks.values():
        value = block_data.get("value", {})
        title_parts = value.get("properties", {}).get("title", [])
        for part in title_parts:
            if isinstance(part, list) and part and isinstance(part[0], str):
                texts.append(part[0])

    return "\n".join(texts).strip() or None


class NotionResource(ConfigurableResource):
    """Notion API resource for fetching page content as text.

    Configuration:
        api_key: Notion integration token (starts with 'secret_'). Optional;
            if not set, requests are sent without authentication (public pages may still work).
    """

    api_key: str = Field(
        default="", description="Notion API token (optional; try without key if unset)"
    )

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }
        if self.api_key and self.api_key.strip():
            headers["Authorization"] = f"Bearer {self.api_key.strip()}"
        return headers

    def fetch_page_content(self, page_url: str) -> str | None:
        """Fetch a Notion page's content and return it as plain text.

        Tries the official Notion API first.  If the page is not shared with
        the integration (404), falls back to the public ``notion.site``
        internal API for pages that are published to the web.

        Args:
            page_url: Full Notion page URL (notion.so or notion.site).

        Returns:
            Concatenated text from all blocks, or None if the page could not be fetched.
        """
        page_id = extract_notion_page_id(page_url)
        if not page_id:
            return None

        all_text: list[str] = []
        next_cursor: str | None = None

        try:
            with httpx.Client(timeout=30.0) as client:
                while True:
                    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
                    params: dict[str, Any] = {"page_size": 100}
                    if next_cursor:
                        params["start_cursor"] = next_cursor
                    response = client.get(url, headers=self._headers(), params=params)
                    response.raise_for_status()
                    data = response.json()
                    for block in data.get("results", []):
                        line = _block_to_text(block)
                        if line:
                            all_text.append(line)
                    next_cursor = data.get("next_cursor")
                    if not next_cursor:
                        break
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "Notion API error for page %s (%s), trying public fallback",
                page_id,
                exc.response.status_code,
            )
            return _fetch_public_page_content(page_url, page_id)

        return "".join(all_text).strip() or None
