"""Tests for change data capture."""

import json
import pytest

from lakehouse.cdc import (
    get_changes,
    get_change_log,
    get_change_summary,
    export_changes,
    replay_changes,
)
from lakehouse.catalog import (
    create_table,
    insert_rows,
    update_rows,
    delete_rows,
    get_snapshots,
)


@pytest.fixture
def cdc_table(test_catalog):
    """Create a table with initial data for CDC testing."""
    create_table(test_catalog, "cdc_test", columns={"id": "long", "name": "string", "value": "double"})
    insert_rows(test_catalog, "default.cdc_test", [
        {"id": 1, "name": "alice", "value": 10.0},
        {"id": 2, "name": "bob", "value": 20.0},
        {"id": 3, "name": "charlie", "value": 30.0},
    ])
    return test_catalog


@pytest.fixture
def cdc_snapshots(cdc_table):
    """Return catalog and snapshot IDs after initial insert."""
    snaps = get_snapshots(cdc_table, "cdc_test")
    return cdc_table, snaps


# --- Insert detection ---


class TestInsertDetection:
    def test_detect_inserts(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        before_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, "default.cdc_test", [
            {"id": 4, "name": "diana", "value": 40.0},
        ])
        snaps_after = get_snapshots(catalog, "cdc_test")
        after_snap = str(snaps_after[-1]["snapshot_id"])

        result = get_changes(catalog, "cdc_test", from_snapshot=before_snap, to_snapshot=after_snap, key_columns=["id"])
        assert result["summary"]["inserts"] == 1
        assert result["summary"]["deletes"] == 0
        assert result["summary"]["updates"] == 0
        inserted = [c for c in result["changes"] if c["type"] == "INSERT"]
        assert inserted[0]["row"]["name"] == "diana"

    def test_all_inserts_from_empty(self, test_catalog):
        """First snapshot — all rows are inserts."""
        create_table(test_catalog, "fresh", columns={"id": "long", "val": "string"})
        insert_rows(test_catalog, "default.fresh", [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])

        result = get_changes(test_catalog, "fresh", key_columns=["id"])
        # With only one snapshot, from_snapshot is None so all are inserts
        assert result["summary"]["inserts"] == 2


# --- Delete detection ---


class TestDeleteDetection:
    def test_detect_deletes(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        before_snap = str(snaps[-1]["snapshot_id"])

        delete_rows(catalog, "default.cdc_test", '"id" = 2')
        snaps_after = get_snapshots(catalog, "cdc_test")
        after_snap = str(snaps_after[-1]["snapshot_id"])

        result = get_changes(catalog, "cdc_test", from_snapshot=before_snap, to_snapshot=after_snap, key_columns=["id"])
        assert result["summary"]["deletes"] == 1
        deleted = [c for c in result["changes"] if c["type"] == "DELETE"]
        assert deleted[0]["row"]["id"] == 2


# --- Update detection ---


class TestUpdateDetection:
    def test_detect_updates(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        before_snap = str(snaps[-1]["snapshot_id"])

        update_rows(catalog, "default.cdc_test", '"id" = 1', {"name": "alice_updated"})
        snaps_after = get_snapshots(catalog, "cdc_test")
        after_snap = str(snaps_after[-1]["snapshot_id"])

        result = get_changes(catalog, "cdc_test", from_snapshot=before_snap, to_snapshot=after_snap, key_columns=["id"])
        assert result["summary"]["updates"] == 1
        updated = [c for c in result["changes"] if c["type"] == "UPDATE"]
        assert updated[0]["before"]["name"] == "alice"
        assert updated[0]["after"]["name"] == "alice_updated"
        assert "name" in updated[0]["changed_columns"]

    def test_detect_mixed_operations(self, cdc_snapshots):
        """Insert + update + delete in one diff."""
        catalog, snaps = cdc_snapshots
        before_snap = str(snaps[-1]["snapshot_id"])

        # Insert new row
        insert_rows(catalog, "default.cdc_test", [{"id": 4, "name": "diana", "value": 40.0}])
        # Update existing
        update_rows(catalog, "default.cdc_test", '"id" = 1', {"value": 99.0})
        # Delete existing
        delete_rows(catalog, "default.cdc_test", '"id" = 3')

        snaps_after = get_snapshots(catalog, "cdc_test")
        after_snap = str(snaps_after[-1]["snapshot_id"])

        result = get_changes(catalog, "cdc_test", from_snapshot=before_snap, to_snapshot=after_snap, key_columns=["id"])
        assert result["summary"]["inserts"] >= 1
        assert result["summary"]["updates"] >= 1
        assert result["summary"]["deletes"] >= 1


# --- Same snapshot / no changes ---


class TestEdgeCases:
    def test_same_snapshot_no_changes(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        snap_id = str(snaps[-1]["snapshot_id"])
        result = get_changes(catalog, "cdc_test", from_snapshot=snap_id, to_snapshot=snap_id)
        assert result["summary"]["inserts"] == 0
        assert result["summary"]["updates"] == 0
        assert result["summary"]["deletes"] == 0

    def test_default_snapshots(self, cdc_snapshots):
        """Without explicit snapshots, uses last two."""
        catalog, snaps = cdc_snapshots
        # Add a new snapshot
        insert_rows(catalog, "default.cdc_test", [{"id": 5, "name": "eve", "value": 50.0}])
        result = get_changes(catalog, "cdc_test", key_columns=["id"])
        assert result["summary"]["inserts"] >= 1


# --- Change log ---


class TestChangeLog:
    def test_change_log(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        # Make more changes to create multiple snapshots
        insert_rows(catalog, "default.cdc_test", [{"id": 4, "name": "d", "value": 40.0}])
        insert_rows(catalog, "default.cdc_test", [{"id": 5, "name": "e", "value": 50.0}])

        log = get_change_log(catalog, "cdc_test", key_columns=["id"])
        assert len(log) >= 2
        for entry in log:
            assert "from_snapshot" in entry
            assert "to_snapshot" in entry
            assert "timestamp" in entry
            assert "summary" in entry
            assert "change_count" in entry

    def test_change_log_single_snapshot(self, test_catalog):
        """Table with one snapshot has empty log."""
        create_table(test_catalog, "solo", columns={"id": "long"})
        insert_rows(test_catalog, "default.solo", [{"id": 1}])
        log = get_change_log(test_catalog, "solo")
        assert log == []


# --- Change summary ---


class TestChangeSummary:
    def test_summary(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        before_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, "default.cdc_test", [{"id": 4, "name": "diana", "value": 40.0}])
        snaps_after = get_snapshots(catalog, "cdc_test")
        after_snap = str(snaps_after[-1]["snapshot_id"])

        result = get_change_summary(catalog, "cdc_test", from_snapshot=before_snap, to_snapshot=after_snap, key_columns=["id"])
        assert result["inserts"] == 1
        assert result["total_changes"] == 1
        assert "affected_columns" in result
        assert len(result["affected_columns"]) >= 1


# --- Export ---


class TestExport:
    def test_export_json(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        before_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, "default.cdc_test", [{"id": 4, "name": "diana", "value": 40.0}])
        snaps_after = get_snapshots(catalog, "cdc_test")
        after_snap = str(snaps_after[-1]["snapshot_id"])

        output = export_changes(catalog, "cdc_test", before_snap, after_snap, format="json", key_columns=["id"])
        data = json.loads(output)
        assert data["table"] == "default.cdc_test"
        assert data["summary"]["inserts"] == 1
        assert len(data["changes"]) == 1

    def test_export_csv(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        before_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, "default.cdc_test", [{"id": 4, "name": "diana", "value": 40.0}])
        snaps_after = get_snapshots(catalog, "cdc_test")
        after_snap = str(snaps_after[-1]["snapshot_id"])

        output = export_changes(catalog, "cdc_test", before_snap, after_snap, format="csv", key_columns=["id"])
        assert "change_type" in output
        assert "INSERT" in output

    def test_export_invalid_format(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        with pytest.raises(ValueError, match="Unsupported format"):
            export_changes(catalog, "cdc_test", str(snaps[-1]["snapshot_id"]), str(snaps[-1]["snapshot_id"]), format="xml")


# --- Replay ---


class TestReplay:
    def test_replay_inserts(self, cdc_snapshots):
        catalog, snaps = cdc_snapshots
        before_snap = str(snaps[-1]["snapshot_id"])

        insert_rows(catalog, "default.cdc_test", [{"id": 4, "name": "diana", "value": 40.0}])
        snaps_after = get_snapshots(catalog, "cdc_test")
        after_snap = str(snaps_after[-1]["snapshot_id"])

        changes_result = get_changes(catalog, "cdc_test", from_snapshot=before_snap, to_snapshot=after_snap, key_columns=["id"])

        # Create target table
        create_table(catalog, "cdc_target", columns={"id": "long", "name": "string", "value": "double"})
        insert_rows(catalog, "default.cdc_target", [
            {"id": 1, "name": "alice", "value": 10.0},
            {"id": 2, "name": "bob", "value": 20.0},
            {"id": 3, "name": "charlie", "value": 30.0},
        ])

        result = replay_changes(catalog, changes_result["changes"], "cdc_target")
        assert result["total_applied"] == 1
        assert result["applied"]["inserts"] == 1
