"""Tests for snapshot diff functionality."""

import pytest

from lakehouse.catalog import (
    create_table,
    get_snapshots,
    insert_rows,
    delete_rows,
    update_rows,
    snapshot_diff,
)


@pytest.fixture
def diff_table(test_catalog):
    """Create a simple table with initial data and return (catalog, table_name)."""
    create_table(test_catalog, "diff_test", {"id": "long", "val": "string", "num": "double"})
    insert_rows(test_catalog, "diff_test", [
        {"id": 1, "val": "alpha", "num": 10.0},
        {"id": 2, "val": "beta", "num": 20.0},
        {"id": 3, "val": "gamma", "num": 30.0},
    ])
    return test_catalog, "diff_test"


class TestSnapshotDiffAdded:
    """Test diff with added rows."""

    def test_diff_added_rows(self, diff_table):
        catalog, table = diff_table
        snaps = get_snapshots(catalog, table)
        from_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, table, [
            {"id": 4, "val": "delta", "num": 40.0},
            {"id": 5, "val": "epsilon", "num": 50.0},
        ])

        result = snapshot_diff(catalog, table, from_snap)

        assert result["summary"]["added"] == 2
        assert result["summary"]["deleted"] == 0
        added_ids = {row["id"] for row in result["added"]}
        assert added_ids == {4, 5}

    def test_diff_first_snapshot_all_added(self, test_catalog):
        """Create a fresh table: first insert creates all-added diff."""
        create_table(test_catalog, "fresh", {"val": "long"})
        insert_rows(test_catalog, "fresh", [{"val": 1}])

        snap1 = str(get_snapshots(test_catalog, "fresh")[-1]["snapshot_id"])

        insert_rows(test_catalog, "fresh", [{"val": 2}, {"val": 3}])

        result = snapshot_diff(test_catalog, "fresh", snap1)
        assert result["summary"]["added"] == 2
        added_vals = {row["val"] for row in result["added"]}
        assert added_vals == {2, 3}


class TestSnapshotDiffDeleted:
    """Test diff with deleted rows."""

    def test_diff_deleted_rows(self, diff_table):
        catalog, table = diff_table
        snaps = get_snapshots(catalog, table)
        from_snap = str(snaps[-1]["snapshot_id"])

        delete_rows(catalog, table, "id = 1")

        result = snapshot_diff(catalog, table, from_snap)

        assert result["summary"]["deleted"] >= 1
        deleted_ids = {row["id"] for row in result["deleted"]}
        assert 1 in deleted_ids


class TestSnapshotDiffModified:
    """Test diff with modified rows (shows as delete + add)."""

    def test_diff_update_shows_as_delete_and_add(self, diff_table):
        catalog, table = diff_table
        snaps = get_snapshots(catalog, table)
        from_snap = str(snaps[-1]["snapshot_id"])

        update_rows(catalog, table, "id = 1", {"num": 999.99})

        result = snapshot_diff(catalog, table, from_snap)

        # The old row should be in deleted, new row in added
        assert result["summary"]["deleted"] >= 1
        assert result["summary"]["added"] >= 1

        added_nums = {row["num"] for row in result["added"]}
        assert 999.99 in added_nums


class TestSnapshotDiffMixed:
    """Test diff with mixed changes."""

    def test_mixed_add_delete(self, diff_table):
        catalog, table = diff_table
        snaps = get_snapshots(catalog, table)
        from_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, table, [
            {"id": 10, "val": "new", "num": 100.0},
        ])
        delete_rows(catalog, table, "id = 2")

        result = snapshot_diff(catalog, table, from_snap)

        assert result["summary"]["added"] >= 1
        assert result["summary"]["deleted"] >= 1

        added_ids = {row["id"] for row in result["added"]}
        deleted_ids = {row["id"] for row in result["deleted"]}
        assert 10 in added_ids
        assert 2 in deleted_ids


class TestSnapshotDiffSameSnapshot:
    """Test diff with same snapshot (no changes)."""

    def test_same_snapshot_no_changes(self, diff_table):
        catalog, table = diff_table
        snaps = get_snapshots(catalog, table)
        snap_id = str(snaps[-1]["snapshot_id"])

        result = snapshot_diff(catalog, table, snap_id, snap_id)

        assert result["summary"]["added"] == 0
        assert result["summary"]["deleted"] == 0
        assert result["summary"]["modified"] == 0
        assert result["added"] == []
        assert result["deleted"] == []


class TestSnapshotDiffToSnapshot:
    """Test explicit to_snapshot parameter."""

    def test_diff_between_two_explicit_snapshots(self, diff_table):
        catalog, table = diff_table
        snaps_before = get_snapshots(catalog, table)
        from_snap = str(snaps_before[-1]["snapshot_id"])

        insert_rows(catalog, table, [
            {"id": 20, "val": "explicit", "num": 200.0},
        ])

        snaps_after = get_snapshots(catalog, table)
        to_snap = str(snaps_after[-1]["snapshot_id"])

        result = snapshot_diff(catalog, table, from_snap, to_snap)

        assert result["summary"]["added"] >= 1
        assert result["from_snapshot_id"] == int(from_snap)
        assert result["to_snapshot_id"] == int(to_snap)


class TestSnapshotDiffTimestamp:
    """Test timestamp-based diff."""

    def test_diff_with_timestamp(self, diff_table):
        catalog, table = diff_table
        snaps = get_snapshots(catalog, table)
        from_ts = snaps[-1]["timestamp"]

        insert_rows(catalog, table, [
            {"id": 30, "val": "timestamp", "num": 300.0},
        ])

        result = snapshot_diff(catalog, table, from_ts)

        assert result["summary"]["added"] >= 1
        added_ids = {row["id"] for row in result["added"]}
        assert 30 in added_ids


class TestSnapshotDiffErrors:
    """Test error handling."""

    def test_invalid_table(self, test_catalog):
        with pytest.raises(ValueError, match="not found"):
            snapshot_diff(test_catalog, "nonexistent_table", "12345")

    def test_invalid_snapshot_id(self, diff_table):
        catalog, table = diff_table
        with pytest.raises(ValueError):
            snapshot_diff(catalog, table, "99999999999999")

    def test_invalid_from_ref(self, diff_table):
        catalog, table = diff_table
        with pytest.raises(ValueError):
            snapshot_diff(catalog, table, "not-a-valid-ref-at-all")


class TestSnapshotDiffOutput:
    """Test output structure."""

    def test_result_structure(self, diff_table):
        catalog, table = diff_table
        snaps = get_snapshots(catalog, table)
        from_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, table, [
            {"id": 40, "val": "struct", "num": 400.0},
        ])

        result = snapshot_diff(catalog, table, from_snap)

        assert "added" in result
        assert "deleted" in result
        assert "modified" in result
        assert "summary" in result
        assert "from_snapshot_id" in result
        assert "to_snapshot_id" in result

        assert isinstance(result["added"], list)
        assert isinstance(result["deleted"], list)
        assert isinstance(result["summary"], dict)
        assert isinstance(result["from_snapshot_id"], int)

    def test_added_rows_contain_all_columns(self, diff_table):
        catalog, table = diff_table
        snaps = get_snapshots(catalog, table)
        from_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, table, [
            {"id": 41, "val": "cols", "num": 410.0},
        ])

        result = snapshot_diff(catalog, table, from_snap)

        assert len(result["added"]) >= 1
        row = result["added"][0]
        expected_cols = {"id", "val", "num"}
        assert set(row.keys()) == expected_cols
