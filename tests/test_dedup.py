"""Tests for data deduplication."""

import pytest

from lakehouse.dedup import (
    find_duplicates,
    dedup_summary,
    remove_duplicates,
    dedup_report,
)
from lakehouse.catalog import create_table, insert_rows


@pytest.fixture
def dup_table(test_catalog):
    """Create a table with duplicate rows."""
    create_table(test_catalog, "dup_test", columns={"id": "long", "name": "string", "value": "double"})
    insert_rows(test_catalog, "default.dup_test", [
        {"id": 1, "name": "alice", "value": 10.0},
        {"id": 2, "name": "bob", "value": 20.0},
        {"id": 2, "name": "bob", "value": 20.0},  # Exact duplicate
        {"id": 3, "name": "charlie", "value": 30.0},
        {"id": 3, "name": "charlie_v2", "value": 35.0},  # Same key, different values
    ])
    return test_catalog


@pytest.fixture
def unique_table(test_catalog):
    """Create a table with no duplicates."""
    create_table(test_catalog, "unique_test", columns={"id": "long", "name": "string"})
    insert_rows(test_catalog, "default.unique_test", [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ])
    return test_catalog


# --- find_duplicates ---


class TestFindDuplicates:
    def test_find_exact_duplicates(self, dup_table):
        result = find_duplicates(dup_table, "dup_test")
        # One exact duplicate row (id=2, bob, 20.0)
        assert result["duplicate_groups"] >= 1
        assert result["duplicate_count"] >= 1

    def test_find_by_key_columns(self, dup_table):
        result = find_duplicates(dup_table, "dup_test", key_columns=["id"])
        # id=2 appears twice, id=3 appears twice
        assert result["duplicate_groups"] == 2
        keys = [d["id"] for d in result["duplicates"]]
        assert 2 in keys
        assert 3 in keys

    def test_no_duplicates(self, unique_table):
        result = find_duplicates(unique_table, "unique_test")
        assert result["duplicate_groups"] == 0
        assert result["duplicate_count"] == 0

    def test_single_column_key(self, dup_table):
        result = find_duplicates(dup_table, "dup_test", key_columns=["name"])
        # bob appears twice, charlie and charlie_v2 are different
        bob_group = [d for d in result["duplicates"] if d["name"] == "bob"]
        assert len(bob_group) == 1
        assert bob_group[0]["_dup_count"] == 2

    def test_empty_table(self, test_catalog):
        create_table(test_catalog, "empty_dup", columns={"id": "long"})
        result = find_duplicates(test_catalog, "empty_dup")
        assert result["duplicate_count"] == 0


# --- dedup_summary ---


class TestDedupSummary:
    def test_summary_with_dups(self, dup_table):
        result = dedup_summary(dup_table, "dup_test", key_columns=["id"])
        assert result["total_rows"] == 5
        assert result["unique_rows"] == 3  # 3 unique IDs
        assert result["duplicate_rows"] == 2
        assert result["duplicate_pct"] == 40.0

    def test_summary_no_dups(self, unique_table):
        result = dedup_summary(unique_table, "unique_test")
        assert result["duplicate_rows"] == 0
        assert result["duplicate_pct"] == 0.0

    def test_summary_all_columns(self, dup_table):
        result = dedup_summary(dup_table, "dup_test")
        # Only the exact duplicate (id=2, bob, 20.0) counts
        assert result["duplicate_rows"] == 1


# --- remove_duplicates ---


class TestRemoveDuplicates:
    def test_dry_run(self, dup_table):
        result = remove_duplicates(dup_table, "dup_test", key_columns=["id"], dry_run=True)
        assert result["dry_run"] is True
        assert result["removed"] == 2
        assert result["remaining"] == 3

        # Verify data unchanged
        tbl = dup_table.load_table("default.dup_test")
        assert tbl.scan().to_arrow().num_rows == 5

    def test_remove_keep_first(self, dup_table):
        result = remove_duplicates(dup_table, "dup_test", key_columns=["id"], keep="first", dry_run=False)
        assert result["dry_run"] is False
        assert result["removed"] == 2
        assert result["remaining"] == 3

        # Verify data changed
        tbl = dup_table.load_table("default.dup_test")
        assert tbl.scan().to_arrow().num_rows == 3

    def test_remove_keep_last(self, dup_table):
        result = remove_duplicates(dup_table, "dup_test", key_columns=["id"], keep="last", dry_run=False)
        assert result["removed"] == 2
        assert result["remaining"] == 3

    def test_no_duplicates_to_remove(self, unique_table):
        result = remove_duplicates(unique_table, "unique_test", dry_run=False)
        assert result["removed"] == 0
        assert result["remaining"] == 3

    def test_invalid_keep(self, dup_table):
        with pytest.raises(ValueError, match="keep must be"):
            remove_duplicates(dup_table, "dup_test", keep="middle")


# --- dedup_report ---


class TestDedupReport:
    def test_report(self, dup_table):
        result = dedup_report(dup_table, "dup_test", key_columns=["id"])
        assert result["total_rows"] == 5
        assert result["duplicate_rows"] == 2
        assert "column_analysis" in result
        assert len(result["column_analysis"]) == 3  # id, name, value
        assert "suggested_keys" in result

    def test_report_column_uniqueness(self, dup_table):
        result = dedup_report(dup_table, "dup_test")
        # Check that column analysis has uniqueness data
        for col in result["column_analysis"]:
            assert "uniqueness_pct" in col
            assert "good_dedup_key" in col
