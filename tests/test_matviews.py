"""Tests for materialized views."""

import json
import pytest
from pathlib import Path

from lakehouse.matviews import (
    create_materialized_view,
    refresh_materialized_view,
    list_materialized_views,
    drop_materialized_view,
    query_materialized_view,
    check_materialized_view_freshness,
)
from lakehouse.catalog import create_table, insert_rows, list_tables
from lakehouse.query import QueryEngine


@pytest.fixture
def mv_path(tmp_path):
    """Return a temporary materialized views store path."""
    return tmp_path / "materialized_views.json"


@pytest.fixture
def mv_data(test_catalog):
    """Create source tables with data for materialized view tests."""
    create_table(test_catalog, "mv_source", columns={"id": "long", "category": "string", "amount": "double"})
    insert_rows(test_catalog, "default.mv_source", [
        {"id": 1, "category": "food", "amount": 10.0},
        {"id": 2, "category": "food", "amount": 20.0},
        {"id": 3, "category": "travel", "amount": 30.0},
    ])
    return test_catalog


# --- create_materialized_view ---


class TestCreateMaterializedView:
    def test_basic(self, mv_data, mv_path):
        """Create a materialized view."""
        engine = QueryEngine(catalog=mv_data)
        result = create_materialized_view(
            "totals", "SELECT category, SUM(amount) AS total FROM mv_source GROUP BY category",
            engine, mv_data, store_path=mv_path,
        )
        assert result["name"] == "totals"
        assert result["row_count"] == 2
        assert "mv_totals" in result["backing_table"]
        assert "created" in result["message"].lower()

    def test_with_description(self, mv_data, mv_path):
        """Create with description."""
        engine = QueryEngine(catalog=mv_data)
        result = create_materialized_view(
            "desc_view", "SELECT * FROM mv_source",
            engine, mv_data, description="My view", store_path=mv_path,
        )
        assert result["description"] == "My view"

    def test_duplicate_raises(self, mv_data, mv_path):
        """Duplicate name raises ValueError."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view("dupe", "SELECT 1 AS val", engine, mv_data, store_path=mv_path)
        with pytest.raises(ValueError, match="already exists"):
            create_materialized_view("dupe", "SELECT 2 AS val", engine, mv_data, store_path=mv_path)

    def test_empty_name_raises(self, mv_data, mv_path):
        """Empty name raises ValueError."""
        engine = QueryEngine(catalog=mv_data)
        with pytest.raises(ValueError, match="empty"):
            create_materialized_view("", "SELECT 1", engine, mv_data, store_path=mv_path)

    def test_empty_sql_raises(self, mv_data, mv_path):
        """Empty SQL raises ValueError."""
        engine = QueryEngine(catalog=mv_data)
        with pytest.raises(ValueError, match="empty"):
            create_materialized_view("test", "", engine, mv_data, store_path=mv_path)

    def test_creates_backing_table(self, mv_data, mv_path):
        """Backing table is created in catalog."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view("backed", "SELECT * FROM mv_source", engine, mv_data, store_path=mv_path)
        tables = list_tables(mv_data)
        assert "default.mv_backed" in tables


# --- query_materialized_view ---


class TestQueryMaterializedView:
    def test_returns_cached_data(self, mv_data, mv_path):
        """Query returns cached results."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view(
            "cached", "SELECT category, SUM(amount) AS total FROM mv_source GROUP BY category",
            engine, mv_data, store_path=mv_path,
        )
        # Re-create engine to pick up new table
        engine2 = QueryEngine(catalog=mv_data)
        df = query_materialized_view("cached", engine2, store_path=mv_path)
        assert len(df) == 2

    def test_nonexistent_raises(self, mv_data, mv_path):
        """Query nonexistent view raises ValueError."""
        engine = QueryEngine(catalog=mv_data)
        with pytest.raises(ValueError, match="not found"):
            query_materialized_view("no_such", engine, store_path=mv_path)


# --- refresh_materialized_view ---


class TestRefreshMaterializedView:
    def test_refresh_picks_up_changes(self, mv_data, mv_path):
        """Refresh picks up new data."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view(
            "refresh_test", "SELECT * FROM mv_source",
            engine, mv_data, store_path=mv_path,
        )
        # Add more data
        insert_rows(mv_data, "default.mv_source", [
            {"id": 4, "category": "misc", "amount": 40.0},
        ])
        # Refresh with new engine
        engine2 = QueryEngine(catalog=mv_data)
        result = refresh_materialized_view("refresh_test", engine2, mv_data, store_path=mv_path)
        assert result["rows_before"] == 3
        assert result["rows_after"] == 4
        assert "refreshed" in result["message"].lower()

    def test_nonexistent_raises(self, mv_data, mv_path):
        """Refresh nonexistent view raises ValueError."""
        engine = QueryEngine(catalog=mv_data)
        with pytest.raises(ValueError, match="not found"):
            refresh_materialized_view("no_such", engine, mv_data, store_path=mv_path)


# --- check_materialized_view_freshness ---


class TestFreshnessCheck:
    def test_fresh(self, mv_data, mv_path):
        """Freshly created view is not stale."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view(
            "fresh_check", "SELECT * FROM mv_source",
            engine, mv_data, store_path=mv_path,
        )
        result = check_materialized_view_freshness("fresh_check", mv_data, store_path=mv_path)
        assert result["stale"] is False

    def test_stale_after_insert(self, mv_data, mv_path):
        """View becomes stale when source data changes."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view(
            "stale_check", "SELECT * FROM mv_source",
            engine, mv_data, store_path=mv_path,
        )
        # Modify source
        insert_rows(mv_data, "default.mv_source", [{"id": 5, "category": "new", "amount": 50.0}])
        result = check_materialized_view_freshness("stale_check", mv_data, store_path=mv_path)
        assert result["stale"] is True
        assert "default.mv_source" in result["changed_tables"]


# --- list_materialized_views ---


class TestListMaterializedViews:
    def test_empty(self, mv_path):
        """Empty store returns empty list."""
        assert list_materialized_views(store_path=mv_path) == []

    def test_with_views(self, mv_data, mv_path):
        """List created views."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view("v1", "SELECT 1 AS val", engine, mv_data, store_path=mv_path)
        create_materialized_view("v2", "SELECT 2 AS val", engine, mv_data, store_path=mv_path)
        views = list_materialized_views(store_path=mv_path)
        assert len(views) == 2
        names = [v["name"] for v in views]
        assert "v1" in names
        assert "v2" in names

    def test_includes_fields(self, mv_data, mv_path):
        """List includes expected fields."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view("fields_check", "SELECT 1 AS val", engine, mv_data, store_path=mv_path)
        views = list_materialized_views(store_path=mv_path)
        v = views[0]
        assert "name" in v
        assert "sql" in v
        assert "row_count" in v
        assert "last_refreshed" in v


# --- drop_materialized_view ---


class TestDropMaterializedView:
    def test_drop(self, mv_data, mv_path):
        """Drop removes view and backing table."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view("to_drop", "SELECT 1 AS val", engine, mv_data, store_path=mv_path)
        result = drop_materialized_view("to_drop", mv_data, store_path=mv_path)
        assert "dropped" in result["message"].lower()

        # Backing table should be gone
        tables = list_tables(mv_data)
        assert "default.mv_to_drop" not in tables

    def test_drop_nonexistent_raises(self, mv_data, mv_path):
        """Drop nonexistent raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            drop_materialized_view("no_such", mv_data, store_path=mv_path)


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, mv_data, mv_path):
        """Store is valid JSON with expected structure."""
        engine = QueryEngine(catalog=mv_data)
        create_materialized_view(
            "json_check", "SELECT * FROM mv_source",
            engine, mv_data, description="test", store_path=mv_path,
        )
        data = json.loads(mv_path.read_text())
        assert "json_check" in data
        entry = data["json_check"]
        assert entry["sql"] == "SELECT * FROM mv_source"
        assert entry["description"] == "test"
        assert entry["backing_table"] == "default.mv_json_check"
        assert entry["row_count"] == 3
        assert "created_at" in entry
        assert "last_refreshed" in entry
        assert "source_snapshot_ids" in entry
