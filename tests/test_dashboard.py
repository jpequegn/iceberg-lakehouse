"""Tests for lakehouse dashboard functionality."""

import json
import pytest
from pathlib import Path

from lakehouse.dashboard import get_dashboard, _format_size, _table_health
from lakehouse.catalog import (
    create_table,
    insert_rows,
    create_namespace,
)
from lakehouse.stats import compute_table_stats
from lakehouse.audit import log_operation
from lakehouse.queries import save_query


@pytest.fixture
def stats_path(tmp_path):
    return tmp_path / "stats_cache.json"


@pytest.fixture
def audit_path(tmp_path):
    return tmp_path / "audit.log"


@pytest.fixture
def queries_path(tmp_path):
    return tmp_path / "queries.json"


# --- Helpers ---


class TestFormatSize:
    def test_bytes(self):
        assert _format_size(500) == "500 B"

    def test_kilobytes(self):
        assert _format_size(2048) == "2.0 KB"

    def test_megabytes(self):
        assert _format_size(1024 * 1024 * 3) == "3.0 MB"

    def test_gigabytes(self):
        assert _format_size(1024 * 1024 * 1024 * 2) == "2.0 GB"

    def test_zero(self):
        assert _format_size(0) == "0 B"


class TestTableHealth:
    def test_good(self):
        assert _table_health(3, 0, False) == "Good"

    def test_compact(self):
        assert _table_health(15, 0, False) == "Compact"

    def test_orphans(self):
        assert _table_health(3, 2, False) == "Orphans"

    def test_stale(self):
        assert _table_health(3, 0, True) == "Stale"

    def test_stale_takes_priority(self):
        """Stale overrides compact and orphans."""
        assert _table_health(15, 2, True) == "Stale"

    def test_orphans_over_compact(self):
        """Orphans checked before compact."""
        assert _table_health(15, 2, False) == "Orphans"


# --- Dashboard ---


class TestDashboard:
    def test_default_tables(self, test_catalog, stats_path, audit_path, queries_path):
        """Dashboard works with default sample tables."""
        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        assert data["total_tables"] >= 1
        assert "storage_path" in data
        assert isinstance(data["namespaces"], list)
        assert "default" in data["namespaces"]
        assert isinstance(data["tables"], list)
        assert data["total_size_bytes"] >= 0

    def test_includes_table_info(self, test_catalog, stats_path, audit_path, queries_path):
        """Each table entry has expected fields."""
        create_table(test_catalog, "dash_test", columns={"id": "long", "val": "string"})
        insert_rows(test_catalog, "default.dash_test", [{"id": 1, "val": "hello"}])

        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        table_entry = next(t for t in data["tables"] if t["name"] == "default.dash_test")
        assert table_entry["rows"] == 1
        assert "size_bytes" in table_entry
        assert "size_display" in table_entry
        assert "data_files" in table_entry
        assert table_entry["health"] in ("Good", "Compact", "Orphans", "Stale")

    def test_uses_cached_stats(self, test_catalog, stats_path, audit_path, queries_path):
        """Dashboard uses cached stats when available."""
        create_table(test_catalog, "cached_dash", columns={"id": "long"})
        insert_rows(test_catalog, "default.cached_dash", [{"id": 1}, {"id": 2}])
        compute_table_stats(test_catalog, "default.cached_dash", stats_path)

        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        table_entry = next(t for t in data["tables"] if t["name"] == "default.cached_dash")
        assert table_entry["rows"] == 2

    def test_health_good(self, test_catalog, stats_path, audit_path, queries_path):
        """Table with few files shows Good health."""
        create_table(test_catalog, "good_health", columns={"id": "long"})
        insert_rows(test_catalog, "default.good_health", [{"id": 1}])
        compute_table_stats(test_catalog, "default.good_health", stats_path)

        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        table_entry = next(t for t in data["tables"] if t["name"] == "default.good_health")
        assert table_entry["health"] == "Good"

    def test_health_stale(self, test_catalog, stats_path, audit_path, queries_path):
        """Table with outdated cache shows Stale health."""
        create_table(test_catalog, "stale_health", columns={"id": "long"})
        insert_rows(test_catalog, "default.stale_health", [{"id": 1}])
        compute_table_stats(test_catalog, "default.stale_health", stats_path)

        # Insert more data to make cache stale
        insert_rows(test_catalog, "default.stale_health", [{"id": 2}])

        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        table_entry = next(t for t in data["tables"] if t["name"] == "default.stale_health")
        assert table_entry["health"] == "Stale"

    def test_recent_activity(self, test_catalog, stats_path, audit_path, queries_path):
        """Dashboard includes recent audit activity."""
        log_operation("default.test", "insert", rows_affected=5, store_path=audit_path)
        log_operation("default.test", "update", rows_affected=2, store_path=audit_path)

        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        assert len(data["recent_activity"]) == 2

    def test_saved_queries_count(self, test_catalog, stats_path, audit_path, queries_path):
        """Dashboard includes saved queries count."""
        save_query("q1", "SELECT 1", store_path=queries_path)
        save_query("q2", "SELECT 2", store_path=queries_path)

        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        assert data["saved_queries_count"] == 2

    def test_history_entries_count(self, test_catalog, stats_path, audit_path, queries_path):
        """Dashboard includes query history count."""
        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        assert data["history_entries_count"] >= 0

    def test_json_output_format(self, test_catalog, stats_path, audit_path, queries_path):
        """Dashboard output is JSON-serializable."""
        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        # Should not raise
        serialized = json.dumps(data, default=str)
        parsed = json.loads(serialized)
        expected_keys = {
            "storage_path", "namespaces", "total_tables", "total_size_bytes",
            "total_size_display", "tables", "recent_activity",
            "saved_queries_count", "history_entries_count",
        }
        assert expected_keys.issubset(set(parsed.keys()))

    def test_namespace_list(self, test_catalog, stats_path, audit_path, queries_path):
        """Dashboard includes namespace list."""
        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        assert "default" in data["namespaces"]

    def test_multiple_namespaces(self, test_catalog, stats_path, audit_path, queries_path):
        """Dashboard shows tables from multiple namespaces."""
        create_namespace(test_catalog, "staging")
        create_table(test_catalog, "staging.events", columns={"id": "long"})

        data = get_dashboard(
            test_catalog,
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        ns = data["namespaces"]
        assert "default" in ns
        assert "staging" in ns
        table_names = [t["name"] for t in data["tables"]]
        assert "staging.events" in table_names

    def test_empty_lakehouse(self, tmp_path, stats_path, audit_path, queries_path):
        """Dashboard handles empty lakehouse (no tables)."""
        import uuid
        from lakehouse.catalog import get_catalog, init_catalog

        catalog = get_catalog(
            warehouse_path=tmp_path / "wh",
            catalog_db=tmp_path / "cat.db",
            name=f"test_{uuid.uuid4().hex[:8]}",
        )
        init_catalog(catalog)

        data = get_dashboard(
            catalog,
            warehouse_path=tmp_path / "wh",
            stats_store_path=stats_path,
            audit_store_path=audit_path,
            queries_store_path=queries_path,
        )
        assert data["total_tables"] == 0
        assert data["tables"] == []
        assert data["total_size_bytes"] == 0
