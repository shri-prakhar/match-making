"""Tests for Notion URL handling and public page fetch (notion.so and notion.site)."""

from talent_matching.resources.notion import (
    NotionResource,
    _extract_site_origin,
    extract_notion_page_id,
)

# Real public page used in Airtable (Social Media Manager job); same page ID for both URL forms.
NOTION_SO_LINK = (
    "https://www.notion.so/Social-Media-Manager-2cb4d743fe73803d89d9f7f3fc4b42e9?pvs=74"
)
NOTION_SITE_LINK = (
    "https://cliff-indigo-30c3.notion.site/Social-Media-Manager-2cb4d743fe73803d89d9f7f3fc4b42e9"
)
EXPECTED_PAGE_ID = "2cb4d743fe73803d89d9f7f3fc4b42e9"


def test_extract_notion_page_id_from_notion_so_link():
    """The notion.so link (as stored in Airtable) yields the correct page ID."""
    assert extract_notion_page_id(NOTION_SO_LINK) == EXPECTED_PAGE_ID


def test_extract_notion_page_id_from_notion_site_link():
    """The public notion.site link yields the same page ID."""
    assert extract_notion_page_id(NOTION_SITE_LINK) == EXPECTED_PAGE_ID


def test_extract_site_origin_notion_so_returns_www_notion_so():
    """notion.so URLs must use https://www.notion.so for the public loadPageChunk fallback."""
    assert _extract_site_origin(NOTION_SO_LINK) == "https://www.notion.so"


def test_extract_site_origin_notion_site_returns_origin():
    """notion.site URLs use their own origin for loadPageChunk."""
    assert _extract_site_origin(NOTION_SITE_LINK) == "https://cliff-indigo-30c3.notion.site"


def test_fetch_page_content_social_media_manager_link_real_request():
    """Fetch the real Social Media Manager Notion page via the notion.so link (no mock)."""
    notion = NotionResource(api_key="")
    result = notion.fetch_page_content(NOTION_SO_LINK)
    assert result is not None
    assert len(result) >= 100
    assert "Social Media Manager" in result
    assert "About the job" in result
