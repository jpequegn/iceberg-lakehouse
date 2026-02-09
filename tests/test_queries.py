"""Tests for saved queries and query history."""

import json
import pytest
from pathlib import Path

from lakehouse.queries import (
    save_query,
    list_saved_queries,
    get_saved_query,
    delete_saved_query,
    add_history_entry,
    get_history,
    clear_history,
    MAX_HISTORY_ENTRIES,
    _load_store,
    _save_store,
)


@pytest.fixture
def store_path(tmp_path):
    """Return a temporary queries store path."""
    return tmp_path / "queries.json"


class TestSaveQuery:
    """Test saving queries."""

    def test_save_simple(self, store_path):
        result = save_query("totals", "SELECT sum(amount) FROM expenses", store_path=store_path)
        assert result["name"] == "totals"
        assert result["sql"] == "SELECT sum(amount) FROM expenses"
        assert "Saved" in result["message"]

    def test_save_with_description(self, store_path):
        result = save_query(
            "totals", "SELECT sum(amount) FROM expenses",
            description="Sum of all expenses",
            store_path=store_path,
        )
        assert result["description"] == "Sum of all expenses"

    def test_save_persists_to_disk(self, store_path):
        save_query("totals", "SELECT 1", store_path=store_path)

        data = json.loads(store_path.read_text())
        assert "totals" in data["saved"]
        assert data["saved"]["totals"]["sql"] == "SELECT 1"

    def test_save_duplicate_raises(self, store_path):
        save_query("totals", "SELECT 1", store_path=store_path)
        with pytest.raises(ValueError, match="already exists"):
            save_query("totals", "SELECT 2", store_path=store_path)

    def test_save_empty_name_raises(self, store_path):
        with pytest.raises(ValueError, match="name must not be empty"):
            save_query("", "SELECT 1", store_path=store_path)

    def test_save_whitespace_name_raises(self, store_path):
        with pytest.raises(ValueError, match="name must not be empty"):
            save_query("   ", "SELECT 1", store_path=store_path)

    def test_save_empty_sql_raises(self, store_path):
        with pytest.raises(ValueError, match="SQL query must not be empty"):
            save_query("test", "", store_path=store_path)

    def test_save_whitespace_sql_raises(self, store_path):
        with pytest.raises(ValueError, match="SQL query must not be empty"):
            save_query("test", "   ", store_path=store_path)

    def test_save_includes_created_at(self, store_path):
        save_query("totals", "SELECT 1", store_path=store_path)

        data = json.loads(store_path.read_text())
        assert "created_at" in data["saved"]["totals"]

    def test_save_multiple(self, store_path):
        save_query("q1", "SELECT 1", store_path=store_path)
        save_query("q2", "SELECT 2", store_path=store_path)
        save_query("q3", "SELECT 3", store_path=store_path)

        data = json.loads(store_path.read_text())
        assert len(data["saved"]) == 3


class TestListSavedQueries:
    """Test listing saved queries."""

    def test_list_empty(self, store_path):
        queries = list_saved_queries(store_path=store_path)
        assert queries == []

    def test_list_returns_all(self, store_path):
        save_query("q1", "SELECT 1", store_path=store_path)
        save_query("q2", "SELECT 2", description="second", store_path=store_path)

        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 2

        names = [q["name"] for q in queries]
        assert "q1" in names
        assert "q2" in names

    def test_list_includes_fields(self, store_path):
        save_query("totals", "SELECT sum(amount) FROM expenses",
                    description="Sum of expenses", store_path=store_path)

        queries = list_saved_queries(store_path=store_path)
        q = queries[0]
        assert q["name"] == "totals"
        assert q["sql"] == "SELECT sum(amount) FROM expenses"
        assert q["description"] == "Sum of expenses"
        assert "created_at" in q


class TestGetSavedQuery:
    """Test getting a saved query by name."""

    def test_get_existing(self, store_path):
        save_query("totals", "SELECT sum(amount) FROM expenses",
                    description="Sum", store_path=store_path)

        result = get_saved_query("totals", store_path=store_path)
        assert result["name"] == "totals"
        assert result["sql"] == "SELECT sum(amount) FROM expenses"
        assert result["description"] == "Sum"

    def test_get_nonexistent_raises(self, store_path):
        with pytest.raises(ValueError, match="not found"):
            get_saved_query("nonexistent", store_path=store_path)

    def test_get_after_multiple_saves(self, store_path):
        save_query("q1", "SELECT 1", store_path=store_path)
        save_query("q2", "SELECT 2", store_path=store_path)
        save_query("q3", "SELECT 3", store_path=store_path)

        result = get_saved_query("q2", store_path=store_path)
        assert result["sql"] == "SELECT 2"


class TestDeleteSavedQuery:
    """Test deleting saved queries."""

    def test_delete_existing(self, store_path):
        save_query("totals", "SELECT 1", store_path=store_path)
        result = delete_saved_query("totals", store_path=store_path)
        assert result["name"] == "totals"
        assert "Deleted" in result["message"]

        # Verify it's gone
        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 0

    def test_delete_nonexistent_raises(self, store_path):
        with pytest.raises(ValueError, match="not found"):
            delete_saved_query("nonexistent", store_path=store_path)

    def test_delete_one_keeps_others(self, store_path):
        save_query("q1", "SELECT 1", store_path=store_path)
        save_query("q2", "SELECT 2", store_path=store_path)

        delete_saved_query("q1", store_path=store_path)

        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 1
        assert queries[0]["name"] == "q2"

    def test_delete_then_recreate(self, store_path):
        save_query("totals", "SELECT 1", store_path=store_path)
        delete_saved_query("totals", store_path=store_path)
        save_query("totals", "SELECT 2", store_path=store_path)

        result = get_saved_query("totals", store_path=store_path)
        assert result["sql"] == "SELECT 2"


class TestAddHistoryEntry:
    """Test adding query history entries."""

    def test_add_entry(self, store_path):
        add_history_entry("SELECT 1", rows_returned=1, duration_ms=10, store_path=store_path)

        history = get_history(store_path=store_path)
        assert len(history) == 1
        assert history[0]["sql"] == "SELECT 1"
        assert history[0]["rows_returned"] == 1
        assert history[0]["duration_ms"] == 10

    def test_add_multiple_entries(self, store_path):
        add_history_entry("SELECT 1", store_path=store_path)
        add_history_entry("SELECT 2", store_path=store_path)
        add_history_entry("SELECT 3", store_path=store_path)

        history = get_history(store_path=store_path)
        assert len(history) == 3

    def test_entries_include_timestamp(self, store_path):
        add_history_entry("SELECT 1", store_path=store_path)

        history = get_history(store_path=store_path)
        assert "executed_at" in history[0]

    def test_most_recent_first(self, store_path):
        add_history_entry("SELECT 1", store_path=store_path)
        add_history_entry("SELECT 2", store_path=store_path)
        add_history_entry("SELECT 3", store_path=store_path)

        history = get_history(store_path=store_path)
        assert history[0]["sql"] == "SELECT 3"
        assert history[1]["sql"] == "SELECT 2"
        assert history[2]["sql"] == "SELECT 1"

    def test_history_cap(self, store_path):
        """History is capped at MAX_HISTORY_ENTRIES."""
        for i in range(MAX_HISTORY_ENTRIES + 50):
            add_history_entry(f"SELECT {i}", store_path=store_path)

        data = json.loads(store_path.read_text())
        assert len(data["history"]) == MAX_HISTORY_ENTRIES

        # Most recent entries should be kept
        history = get_history(limit=1, store_path=store_path)
        assert history[0]["sql"] == f"SELECT {MAX_HISTORY_ENTRIES + 49}"

    def test_history_preserves_saved_queries(self, store_path):
        """Adding history entries doesn't affect saved queries."""
        save_query("q1", "SELECT 1", store_path=store_path)
        add_history_entry("SELECT 2", store_path=store_path)

        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 1
        assert queries[0]["name"] == "q1"


class TestGetHistory:
    """Test getting query history."""

    def test_get_empty(self, store_path):
        history = get_history(store_path=store_path)
        assert history == []

    def test_get_with_limit(self, store_path):
        for i in range(10):
            add_history_entry(f"SELECT {i}", store_path=store_path)

        history = get_history(limit=3, store_path=store_path)
        assert len(history) == 3

        # Most recent first
        assert history[0]["sql"] == "SELECT 9"
        assert history[1]["sql"] == "SELECT 8"
        assert history[2]["sql"] == "SELECT 7"

    def test_get_default_limit(self, store_path):
        for i in range(30):
            add_history_entry(f"SELECT {i}", store_path=store_path)

        history = get_history(store_path=store_path)
        assert len(history) == 20  # default limit

    def test_limit_larger_than_history(self, store_path):
        add_history_entry("SELECT 1", store_path=store_path)
        add_history_entry("SELECT 2", store_path=store_path)

        history = get_history(limit=100, store_path=store_path)
        assert len(history) == 2


class TestClearHistory:
    """Test clearing query history."""

    def test_clear_empty(self, store_path):
        result = clear_history(store_path=store_path)
        assert result["cleared"] == 0

    def test_clear_with_entries(self, store_path):
        for i in range(5):
            add_history_entry(f"SELECT {i}", store_path=store_path)

        result = clear_history(store_path=store_path)
        assert result["cleared"] == 5

        history = get_history(store_path=store_path)
        assert history == []

    def test_clear_preserves_saved_queries(self, store_path):
        save_query("q1", "SELECT 1", store_path=store_path)
        add_history_entry("SELECT 2", store_path=store_path)

        clear_history(store_path=store_path)

        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 1
        assert queries[0]["name"] == "q1"


class TestStoreResilience:
    """Test store loading/saving edge cases."""

    def test_load_nonexistent_file(self, store_path):
        data = _load_store(store_path)
        assert data == {"saved": {}, "history": []}

    def test_load_corrupt_json(self, store_path):
        store_path.parent.mkdir(parents=True, exist_ok=True)
        store_path.write_text("not valid json{{{")

        data = _load_store(store_path)
        assert data == {"saved": {}, "history": []}

    def test_store_creates_parent_dirs(self, store_path):
        nested_path = store_path.parent / "deep" / "nested" / "queries.json"
        save_query("q1", "SELECT 1", store_path=nested_path)
        assert nested_path.exists()

    def test_concurrent_save_and_history(self, store_path):
        """Saving queries and adding history don't interfere."""
        save_query("q1", "SELECT 1", store_path=store_path)
        add_history_entry("SELECT 2", store_path=store_path)
        save_query("q2", "SELECT 3", store_path=store_path)
        add_history_entry("SELECT 4", store_path=store_path)

        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 2

        history = get_history(store_path=store_path)
        assert len(history) == 2


class TestQueryWorkflow:
    """Test end-to-end query workflows."""

    def test_save_list_get_delete(self, store_path):
        """Full lifecycle: save -> list -> get -> delete."""
        save_query("totals", "SELECT sum(amount) FROM expenses",
                    description="Total spending", store_path=store_path)

        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 1

        result = get_saved_query("totals", store_path=store_path)
        assert result["sql"] == "SELECT sum(amount) FROM expenses"

        delete_saved_query("totals", store_path=store_path)
        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 0

    def test_history_accumulation_and_clear(self, store_path):
        """Add entries, verify ordering, then clear."""
        for i in range(5):
            add_history_entry(f"SELECT {i}", rows_returned=i, store_path=store_path)

        history = get_history(limit=3, store_path=store_path)
        assert len(history) == 3
        assert history[0]["sql"] == "SELECT 4"

        clear_history(store_path=store_path)
        assert get_history(store_path=store_path) == []

    def test_mixed_operations(self, store_path):
        """Interleave saved queries and history."""
        save_query("q1", "SELECT 1", store_path=store_path)
        add_history_entry("SELECT 1", rows_returned=1, store_path=store_path)

        save_query("q2", "SELECT 2", store_path=store_path)
        add_history_entry("SELECT 2", rows_returned=2, store_path=store_path)

        delete_saved_query("q1", store_path=store_path)
        add_history_entry("SELECT 3", rows_returned=3, store_path=store_path)

        queries = list_saved_queries(store_path=store_path)
        assert len(queries) == 1
        assert queries[0]["name"] == "q2"

        history = get_history(store_path=store_path)
        assert len(history) == 3
        assert history[0]["sql"] == "SELECT 3"
