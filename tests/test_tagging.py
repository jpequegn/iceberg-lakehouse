"""Tests for table bookmarks, tags, and descriptions."""

import json
import pytest
from pathlib import Path

from lakehouse.tagging import (
    tag_table,
    untag_table,
    get_tags,
    search_by_tag,
    set_table_description,
    get_table_description,
    bookmark_table,
    unbookmark_table,
    list_bookmarks,
    search_tables,
)


@pytest.fixture
def meta_path(tmp_path):
    """Return a temporary metadata store path."""
    return tmp_path / "table_metadata.json"


# --- Tags ---


class TestTagTable:
    def test_add_tags(self, meta_path):
        """Add tags to a table."""
        result = tag_table("expenses", ["finance", "pii"], meta_path)
        assert result["table"] == "default.expenses"
        assert "finance" in result["tags"]
        assert "pii" in result["tags"]

    def test_tags_normalized_lowercase(self, meta_path):
        """Tags are lowercased."""
        result = tag_table("expenses", ["Finance", "PII"], meta_path)
        assert "finance" in result["tags"]
        assert "pii" in result["tags"]

    def test_tags_accumulate(self, meta_path):
        """Multiple tag calls accumulate tags."""
        tag_table("expenses", ["finance"], meta_path)
        result = tag_table("expenses", ["pii"], meta_path)
        assert "finance" in result["tags"]
        assert "pii" in result["tags"]

    def test_duplicate_tags_deduplicated(self, meta_path):
        """Adding the same tag twice doesn't duplicate."""
        tag_table("expenses", ["finance"], meta_path)
        result = tag_table("expenses", ["finance", "pii"], meta_path)
        assert result["tags"].count("finance") == 1

    def test_tags_sorted(self, meta_path):
        """Tags are returned sorted."""
        result = tag_table("expenses", ["zebra", "alpha", "middle"], meta_path)
        assert result["tags"] == ["alpha", "middle", "zebra"]

    def test_empty_tags_ignored(self, meta_path):
        """Empty/whitespace tags are ignored."""
        result = tag_table("expenses", ["finance", "", "  "], meta_path)
        assert result["tags"] == ["finance"]

    def test_bare_name_normalized(self, meta_path):
        """Bare table name is auto-prefixed."""
        tag_table("expenses", ["test"], meta_path)
        tags = get_tags("expenses", meta_path)
        assert "test" in tags


class TestUntagTable:
    def test_remove_tags(self, meta_path):
        """Remove tags from a table."""
        tag_table("expenses", ["finance", "pii", "old"], meta_path)
        result = untag_table("expenses", ["pii", "old"], meta_path)
        assert result["tags"] == ["finance"]

    def test_remove_nonexistent_tag(self, meta_path):
        """Removing a tag that doesn't exist is a no-op."""
        tag_table("expenses", ["finance"], meta_path)
        result = untag_table("expenses", ["nonexistent"], meta_path)
        assert result["tags"] == ["finance"]


class TestGetTags:
    def test_existing(self, meta_path):
        """Get tags for tagged table."""
        tag_table("expenses", ["finance"], meta_path)
        assert get_tags("expenses", meta_path) == ["finance"]

    def test_no_tags(self, meta_path):
        """Get tags for untagged table returns empty."""
        assert get_tags("no_table", meta_path) == []


class TestSearchByTag:
    def test_finds_tables(self, meta_path):
        """Find tables by tag."""
        tag_table("expenses", ["finance"], meta_path)
        tag_table("revenue", ["finance"], meta_path)
        tag_table("users", ["pii"], meta_path)

        result = search_by_tag("finance", meta_path)
        assert "default.expenses" in result
        assert "default.revenue" in result
        assert "default.users" not in result

    def test_no_matches(self, meta_path):
        """Search with no matching tag returns empty."""
        assert search_by_tag("nonexistent", meta_path) == []

    def test_returns_sorted(self, meta_path):
        """Results are sorted."""
        tag_table("zebra", ["test"], meta_path)
        tag_table("alpha", ["test"], meta_path)
        result = search_by_tag("test", meta_path)
        assert result == ["default.alpha", "default.zebra"]


# --- Descriptions ---


class TestDescriptions:
    def test_set_and_get(self, meta_path):
        """Set and get a description."""
        set_table_description("expenses", "Monthly expense reports", meta_path)
        desc = get_table_description("expenses", meta_path)
        assert desc == "Monthly expense reports"

    def test_overwrite(self, meta_path):
        """Setting description overwrites previous."""
        set_table_description("expenses", "Old", meta_path)
        set_table_description("expenses", "New", meta_path)
        assert get_table_description("expenses", meta_path) == "New"

    def test_no_description(self, meta_path):
        """Get description for undescribed table returns empty string."""
        assert get_table_description("no_table", meta_path) == ""

    def test_description_preserved_with_tags(self, meta_path):
        """Description persists when tags are added."""
        set_table_description("expenses", "My table", meta_path)
        tag_table("expenses", ["finance"], meta_path)
        assert get_table_description("expenses", meta_path) == "My table"


# --- Bookmarks ---


class TestBookmarks:
    def test_bookmark(self, meta_path):
        """Bookmark a table."""
        result = bookmark_table("expenses", meta_path)
        assert "bookmarked" in result["message"].lower()

    def test_unbookmark(self, meta_path):
        """Unbookmark a table."""
        bookmark_table("expenses", meta_path)
        result = unbookmark_table("expenses", meta_path)
        assert "unbookmarked" in result["message"].lower()

    def test_list_bookmarks(self, meta_path):
        """List bookmarked tables."""
        bookmark_table("expenses", meta_path)
        bookmark_table("revenue", meta_path)
        bookmarks = list_bookmarks(meta_path)
        assert "default.expenses" in bookmarks
        assert "default.revenue" in bookmarks

    def test_list_empty(self, meta_path):
        """List bookmarks when none exist."""
        assert list_bookmarks(meta_path) == []

    def test_unbookmark_removes_from_list(self, meta_path):
        """Unbookmarked table is removed from list."""
        bookmark_table("expenses", meta_path)
        unbookmark_table("expenses", meta_path)
        assert list_bookmarks(meta_path) == []

    def test_bookmark_preserved_with_tags(self, meta_path):
        """Bookmark persists when tags are added."""
        bookmark_table("expenses", meta_path)
        tag_table("expenses", ["finance"], meta_path)
        assert "default.expenses" in list_bookmarks(meta_path)


# --- Search ---


class TestSearchTables:
    def test_search_by_name(self, meta_path):
        """Search finds tables by name."""
        tag_table("expenses", ["finance"], meta_path)
        results = search_tables("expense", store_path=meta_path)
        assert len(results) == 1
        assert results[0]["table"] == "default.expenses"
        assert "name" in results[0]["match_type"]

    def test_search_by_tag(self, meta_path):
        """Search finds tables by tag."""
        tag_table("revenue", ["finance"], meta_path)
        results = search_tables("finance", store_path=meta_path)
        assert len(results) == 1
        assert "tag" in results[0]["match_type"]

    def test_search_by_description(self, meta_path):
        """Search finds tables by description."""
        set_table_description("users", "User accounts and profiles", meta_path)
        results = search_tables("profiles", store_path=meta_path)
        assert len(results) == 1
        assert "description" in results[0]["match_type"]

    def test_search_multiple_matches(self, meta_path):
        """Search can match multiple tables."""
        tag_table("expenses", ["finance"], meta_path)
        tag_table("revenue", ["finance"], meta_path)
        results = search_tables("finance", store_path=meta_path)
        assert len(results) == 2

    def test_search_no_matches(self, meta_path):
        """Search with no matches returns empty list."""
        results = search_tables("nonexistent", store_path=meta_path)
        assert results == []

    def test_search_with_catalog(self, test_catalog, meta_path):
        """Search includes catalog tables not in metadata store."""
        results = search_tables("expense", catalog=test_catalog, store_path=meta_path)
        # Should find sample tables from catalog matching "expense"
        table_names = [r["table"] for r in results]
        assert any("expense" in t.lower() for t in table_names)

    def test_search_includes_bookmark_status(self, meta_path):
        """Search results include bookmark status."""
        tag_table("expenses", ["finance"], meta_path)
        bookmark_table("expenses", meta_path)
        results = search_tables("expense", store_path=meta_path)
        assert results[0]["bookmarked"] is True

    def test_search_case_insensitive(self, meta_path):
        """Search is case-insensitive."""
        tag_table("Expenses", ["FINANCE"], meta_path)
        results = search_tables("finance", store_path=meta_path)
        assert len(results) == 1


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, meta_path):
        """Metadata store is valid JSON with expected structure."""
        tag_table("expenses", ["finance", "pii"], meta_path)
        set_table_description("expenses", "My table", meta_path)
        bookmark_table("expenses", meta_path)

        data = json.loads(meta_path.read_text())
        assert "default.expenses" in data
        entry = data["default.expenses"]
        assert "tags" in entry
        assert "description" in entry
        assert "bookmarked" in entry
        assert "updated_at" in entry
        assert entry["tags"] == ["finance", "pii"]
        assert entry["description"] == "My table"
        assert entry["bookmarked"] is True
