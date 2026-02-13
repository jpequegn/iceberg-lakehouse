"""Tests for table statistics cache functionality."""

import json
import datetime
import pytest
from pathlib import Path

from lakehouse.stats import (
    compute_table_stats,
    get_cached_stats,
    get_all_cached_stats,
    refresh_stats,
    is_stats_stale,
    _load_cache,
    _save_cache,
)
from lakehouse.catalog import (
    insert_rows,
    create_table,
)


@pytest.fixture
def stats_path(tmp_path):
    """Return a temporary stats cache path."""
    return tmp_path / "stats_cache.json"


@pytest.fixture
def stats_table(test_catalog):
    """Create a table with data for stats testing."""
    create_table(
        test_catalog,
        "stats_test",
        columns={"id": "long", "name": "string", "amount": "double"},
    )
    insert_rows(
        test_catalog,
        "default.stats_test",
        [
            {"id": 1, "name": "Alice", "amount": 100.5},
            {"id": 2, "name": "Bob", "amount": 200.0},
            {"id": 3, "name": "Charlie", "amount": 150.75},
        ],
    )
    return "default.stats_test"


# --- Cache internals ---


class TestCacheInternals:
    def test_load_empty_cache(self, stats_path):
        """Loading from non-existent path returns empty dict."""
        result = _load_cache(stats_path)
        assert result == {}

    def test_save_and_load(self, stats_path):
        """Save then load round-trips correctly."""
        data = {"default.test": {"row_count": 10}}
        _save_cache(data, stats_path)
        loaded = _load_cache(stats_path)
        assert loaded == data

    def test_load_corrupt_json(self, stats_path):
        """Corrupt JSON returns empty dict."""
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        stats_path.write_text("{invalid json")
        result = _load_cache(stats_path)
        assert result == {}


# --- compute_table_stats ---


class TestComputeTableStats:
    def test_basic_stats(self, test_catalog, stats_table, stats_path):
        """Compute stats returns correct row_count and column_count."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        assert stats["row_count"] == 3
        assert stats["column_count"] == 3

    def test_snapshot_info(self, test_catalog, stats_table, stats_path):
        """Stats include snapshot_count and snapshot_id."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        assert stats["snapshot_count"] >= 1
        assert stats["snapshot_id_at_cache"] is not None

    def test_file_info(self, test_catalog, stats_table, stats_path):
        """Stats include data_files and size_bytes."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        assert stats["data_files"] >= 1
        assert stats["size_bytes"] > 0

    def test_last_modified(self, test_catalog, stats_table, stats_path):
        """Stats include last_modified timestamp."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        assert stats["last_modified"] is not None
        # Should be a parseable ISO timestamp
        datetime.datetime.fromisoformat(stats["last_modified"])

    def test_cached_at(self, test_catalog, stats_table, stats_path):
        """Stats include cached_at timestamp."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        assert "cached_at" in stats
        datetime.datetime.fromisoformat(stats["cached_at"])

    def test_column_stats_present(self, test_catalog, stats_table, stats_path):
        """Stats include per-column info."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        columns = stats["columns"]
        assert "id" in columns
        assert "name" in columns
        assert "amount" in columns

    def test_numeric_column_stats(self, test_catalog, stats_table, stats_path):
        """Numeric columns have min, max, mean."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        id_col = stats["columns"]["id"]
        assert id_col["type"] == "long"
        assert id_col["min"] == 1
        assert id_col["max"] == 3
        assert id_col["mean"] == 2.0
        assert id_col["nulls"] == 0
        assert id_col["unique"] == 3

    def test_string_column_stats(self, test_catalog, stats_table, stats_path):
        """String columns have nulls and unique count."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        name_col = stats["columns"]["name"]
        assert name_col["type"] == "string"
        assert name_col["nulls"] == 0
        assert name_col["unique"] == 3
        # String columns should NOT have min/max/mean
        assert "min" not in name_col
        assert "mean" not in name_col

    def test_double_column_stats(self, test_catalog, stats_table, stats_path):
        """Double columns have min, max, mean."""
        stats = compute_table_stats(test_catalog, stats_table, stats_path)
        amount_col = stats["columns"]["amount"]
        assert amount_col["type"] == "double"
        assert amount_col["min"] == 100.5
        assert amount_col["max"] == 200.0
        assert amount_col["mean"] is not None

    def test_persists_to_cache(self, test_catalog, stats_table, stats_path):
        """compute_table_stats saves results to cache file."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        assert stats_path.exists()
        cache = json.loads(stats_path.read_text())
        assert stats_table in cache

    def test_invalid_table(self, test_catalog, stats_path):
        """Raises ValueError for non-existent table."""
        with pytest.raises(ValueError, match="not found"):
            compute_table_stats(test_catalog, "no_such_table", stats_path)

    def test_bare_table_name(self, test_catalog, stats_table, stats_path):
        """Bare table name (without namespace) is auto-prefixed."""
        stats = compute_table_stats(test_catalog, "stats_test", stats_path)
        assert stats["row_count"] == 3

    def test_empty_table(self, test_catalog, stats_path):
        """Stats for empty table show zero rows."""
        create_table(
            test_catalog,
            "empty_stats",
            columns={"id": "long", "val": "string"},
        )
        stats = compute_table_stats(test_catalog, "default.empty_stats", stats_path)
        assert stats["row_count"] == 0
        assert stats["column_count"] == 2
        # Empty table columns still listed
        assert "id" in stats["columns"]
        assert stats["columns"]["id"]["nulls"] == 0
        assert stats["columns"]["id"]["unique"] == 0


# --- get_cached_stats ---


class TestGetCachedStats:
    def test_no_cache(self, stats_path):
        """Returns None when no cache exists."""
        result = get_cached_stats("default.test", stats_path)
        assert result is None

    def test_cache_hit(self, test_catalog, stats_table, stats_path):
        """Returns cached stats after compute."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        result = get_cached_stats(stats_table, stats_path)
        assert result is not None
        assert result["row_count"] == 3

    def test_bare_name_lookup(self, test_catalog, stats_table, stats_path):
        """Can look up with bare name."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        result = get_cached_stats("stats_test", stats_path)
        assert result is not None

    def test_miss_for_wrong_table(self, test_catalog, stats_table, stats_path):
        """Returns None for uncached table."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        result = get_cached_stats("default.other_table", stats_path)
        assert result is None


# --- get_all_cached_stats ---


class TestGetAllCachedStats:
    def test_empty(self, stats_path):
        """Empty cache returns empty dict."""
        result = get_all_cached_stats(stats_path)
        assert result == {}

    def test_multiple_tables(self, test_catalog, stats_table, stats_path):
        """Returns all cached tables."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        # Create and compute another table
        create_table(
            test_catalog,
            "stats_second",
            columns={"id": "long"},
        )
        insert_rows(test_catalog, "default.stats_second", [{"id": 1}])
        compute_table_stats(test_catalog, "default.stats_second", stats_path)

        all_stats = get_all_cached_stats(stats_path)
        assert len(all_stats) == 2
        assert stats_table in all_stats
        assert "default.stats_second" in all_stats


# --- refresh_stats ---


class TestRefreshStats:
    def test_refresh_single(self, test_catalog, stats_table, stats_path):
        """Refresh single table returns correct result."""
        result = refresh_stats(test_catalog, stats_table, stats_path)
        assert result["count"] == 1
        assert stats_table in result["tables_refreshed"]
        assert "duration_seconds" in result

    def test_refresh_detects_changes(self, test_catalog, stats_table, stats_path):
        """Refresh picks up new rows."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        old_stats = get_cached_stats(stats_table, stats_path)
        assert old_stats["row_count"] == 3

        insert_rows(test_catalog, stats_table, [{"id": 4, "name": "Dave", "amount": 300.0}])
        refresh_stats(test_catalog, stats_table, stats_path)
        new_stats = get_cached_stats(stats_table, stats_path)
        assert new_stats["row_count"] == 4

    def test_refresh_all(self, test_catalog, stats_table, stats_path):
        """Refresh all tables."""
        result = refresh_stats(test_catalog, store_path=stats_path)
        # Should refresh all tables in catalog (sample tables + stats_test)
        assert result["count"] >= 1
        assert "duration_seconds" in result
        assert "message" in result

    def test_refresh_bare_name(self, test_catalog, stats_table, stats_path):
        """Refresh with bare table name works."""
        result = refresh_stats(test_catalog, "stats_test", stats_path)
        assert result["count"] == 1


# --- is_stats_stale ---


class TestIsStatsStale:
    def test_stale_no_cache(self, stats_path):
        """No cache means stale."""
        assert is_stats_stale("default.test", store_path=stats_path) is True

    def test_not_stale_after_compute(self, test_catalog, stats_table, stats_path):
        """Fresh compute is not stale."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        assert is_stats_stale(stats_table, test_catalog, stats_path) is False

    def test_stale_after_insert(self, test_catalog, stats_table, stats_path):
        """Stale after data changes."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        insert_rows(test_catalog, stats_table, [{"id": 10, "name": "New", "amount": 0.0}])
        assert is_stats_stale(stats_table, test_catalog, stats_path) is True

    def test_not_stale_without_catalog(self, test_catalog, stats_table, stats_path):
        """Without catalog, cached stats are assumed fresh."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        assert is_stats_stale(stats_table, catalog=None, store_path=stats_path) is False

    def test_stale_bare_name(self, test_catalog, stats_table, stats_path):
        """Bare name lookup works for staleness check."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        assert is_stats_stale("stats_test", test_catalog, stats_path) is False


# --- Integration ---


class TestStatsIntegration:
    def test_full_workflow(self, test_catalog, stats_path):
        """Full workflow: create, compute, check, insert, detect stale, refresh."""
        create_table(
            test_catalog,
            "workflow_test",
            columns={"id": "long", "value": "double"},
        )
        table_name = "default.workflow_test"

        # Initially stale (no cache)
        assert is_stats_stale(table_name, test_catalog, stats_path) is True

        # Insert data and compute
        insert_rows(test_catalog, table_name, [
            {"id": 1, "value": 10.0},
            {"id": 2, "value": 20.0},
        ])
        stats = compute_table_stats(test_catalog, table_name, stats_path)
        assert stats["row_count"] == 2
        assert is_stats_stale(table_name, test_catalog, stats_path) is False

        # Insert more data — now stale
        insert_rows(test_catalog, table_name, [{"id": 3, "value": 30.0}])
        assert is_stats_stale(table_name, test_catalog, stats_path) is True

        # Refresh
        refresh_stats(test_catalog, table_name, stats_path)
        updated = get_cached_stats(table_name, stats_path)
        assert updated["row_count"] == 3
        assert is_stats_stale(table_name, test_catalog, stats_path) is False

    def test_json_output_format(self, test_catalog, stats_table, stats_path):
        """Cache file is valid JSON with expected structure."""
        compute_table_stats(test_catalog, stats_table, stats_path)
        raw = json.loads(stats_path.read_text())
        assert isinstance(raw, dict)
        entry = raw[stats_table]
        expected_keys = {
            "row_count", "column_count", "size_bytes", "data_files",
            "snapshot_count", "last_modified", "cached_at",
            "snapshot_id_at_cache", "columns",
        }
        assert expected_keys.issubset(set(entry.keys()))
