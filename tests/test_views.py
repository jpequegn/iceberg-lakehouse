"""Tests for SQL views functionality."""

import json
import pytest
from pathlib import Path

from lakehouse.views import (
    create_view,
    list_views,
    get_view,
    drop_view,
    query_view,
)
from lakehouse.catalog import create_table, insert_rows


@pytest.fixture
def views_path(tmp_path):
    """Return a temporary views store path."""
    return tmp_path / "views.json"


# --- create_view ---


class TestCreateView:
    def test_simple_view(self, views_path):
        """Create a simple view."""
        result = create_view("test_view", "SELECT 1 AS val", store_path=views_path)
        assert result["name"] == "test_view"
        assert result["sql"] == "SELECT 1 AS val"
        assert "created" in result["message"].lower()

    def test_with_description(self, views_path):
        """Create a view with description."""
        result = create_view(
            "my_view", "SELECT * FROM t", description="My view", store_path=views_path,
        )
        assert result["description"] == "My view"

    def test_persists(self, views_path):
        """View is persisted to store."""
        create_view("stored_view", "SELECT 1", store_path=views_path)
        assert views_path.exists()
        data = json.loads(views_path.read_text())
        assert "stored_view" in data

    def test_has_timestamp(self, views_path):
        """View has created_at timestamp."""
        result = create_view("ts_view", "SELECT 1", store_path=views_path)
        assert result["created_at"] is not None

    def test_duplicate_raises(self, views_path):
        """Creating duplicate view name raises ValueError."""
        create_view("dupe", "SELECT 1", store_path=views_path)
        with pytest.raises(ValueError, match="already exists"):
            create_view("dupe", "SELECT 2", store_path=views_path)

    def test_empty_name_raises(self, views_path):
        """Empty name raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            create_view("", "SELECT 1", store_path=views_path)

    def test_empty_sql_raises(self, views_path):
        """Empty SQL raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            create_view("test", "", store_path=views_path)

    def test_whitespace_name_raises(self, views_path):
        """Whitespace-only name raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            create_view("   ", "SELECT 1", store_path=views_path)


# --- list_views ---


class TestListViews:
    def test_empty(self, views_path):
        """Empty store returns empty list."""
        assert list_views(views_path) == []

    def test_with_views(self, views_path):
        """Lists created views."""
        create_view("v1", "SELECT 1", store_path=views_path)
        create_view("v2", "SELECT 2", description="second", store_path=views_path)
        views = list_views(views_path)
        assert len(views) == 2
        names = [v["name"] for v in views]
        assert "v1" in names
        assert "v2" in names

    def test_includes_all_fields(self, views_path):
        """Each view has name, sql, description, created_at."""
        create_view("full", "SELECT *", description="all fields", store_path=views_path)
        views = list_views(views_path)
        v = views[0]
        assert v["name"] == "full"
        assert v["sql"] == "SELECT *"
        assert v["description"] == "all fields"
        assert v["created_at"]


# --- get_view ---


class TestGetView:
    def test_existing(self, views_path):
        """Get an existing view."""
        create_view("get_me", "SELECT 42", store_path=views_path)
        v = get_view("get_me", views_path)
        assert v["name"] == "get_me"
        assert v["sql"] == "SELECT 42"

    def test_nonexistent_raises(self, views_path):
        """Get nonexistent view raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            get_view("no_such", views_path)


# --- drop_view ---


class TestDropView:
    def test_drop_existing(self, views_path):
        """Drop an existing view."""
        create_view("to_drop", "SELECT 1", store_path=views_path)
        result = drop_view("to_drop", views_path)
        assert "dropped" in result["message"].lower()
        assert list_views(views_path) == []

    def test_drop_nonexistent_raises(self, views_path):
        """Drop nonexistent view raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            drop_view("no_such", views_path)


# --- query_view ---


class TestQueryView:
    def test_simple_query(self, query_engine, views_path):
        """Query a simple view."""
        create_view("simple", "SELECT 1 AS val", store_path=views_path)
        df = query_view("simple", query_engine, store_path=views_path)
        assert len(df) == 1
        assert df.iloc[0]["val"] == 1

    def test_view_over_table(self, test_catalog, query_engine, views_path):
        """Query a view that references an Iceberg table."""
        create_table(test_catalog, "view_data", columns={"id": "long", "name": "string"})
        insert_rows(test_catalog, "default.view_data", [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ])
        # Re-create engine to pick up new table
        from lakehouse.query import QueryEngine
        engine = QueryEngine(catalog=test_catalog)

        create_view("all_data", "SELECT * FROM view_data", store_path=views_path)
        df = query_view("all_data", engine, store_path=views_path)
        assert len(df) == 3

    def test_view_with_where(self, test_catalog, query_engine, views_path):
        """Query a view with a WHERE clause."""
        create_table(test_catalog, "filter_data", columns={"id": "long", "val": "double"})
        insert_rows(test_catalog, "default.filter_data", [
            {"id": 1, "val": 10.0},
            {"id": 2, "val": 20.0},
            {"id": 3, "val": 30.0},
        ])
        from lakehouse.query import QueryEngine
        engine = QueryEngine(catalog=test_catalog)

        create_view("big_vals", "SELECT * FROM filter_data WHERE val > 15", store_path=views_path)
        df = query_view("big_vals", engine, store_path=views_path)
        assert len(df) == 2

    def test_view_with_aggregation(self, test_catalog, views_path):
        """Query a view with aggregation."""
        create_table(test_catalog, "agg_data", columns={"cat": "string", "amount": "double"})
        insert_rows(test_catalog, "default.agg_data", [
            {"cat": "A", "amount": 10.0},
            {"cat": "A", "amount": 20.0},
            {"cat": "B", "amount": 30.0},
        ])
        from lakehouse.query import QueryEngine
        engine = QueryEngine(catalog=test_catalog)

        create_view(
            "by_cat",
            "SELECT cat, SUM(amount) AS total FROM agg_data GROUP BY cat",
            store_path=views_path,
        )
        df = query_view("by_cat", engine, store_path=views_path)
        assert len(df) == 2
        assert set(df["cat"].tolist()) == {"A", "B"}

    def test_max_rows(self, query_engine, views_path):
        """Max rows limits output."""
        create_view("limited", "SELECT * FROM generate_series(1, 100) t(val)", store_path=views_path)
        df = query_view("limited", query_engine, max_rows=5, store_path=views_path)
        assert len(df) == 5

    def test_nonexistent_raises(self, query_engine, views_path):
        """Query nonexistent view raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            query_view("missing", query_engine, store_path=views_path)


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, views_path):
        """Views store is valid JSON with expected structure."""
        create_view("v1", "SELECT 1", description="first", store_path=views_path)
        create_view("v2", "SELECT 2", store_path=views_path)

        data = json.loads(views_path.read_text())
        assert "v1" in data
        assert "v2" in data
        entry = data["v1"]
        assert entry["sql"] == "SELECT 1"
        assert entry["description"] == "first"
        assert "created_at" in entry
