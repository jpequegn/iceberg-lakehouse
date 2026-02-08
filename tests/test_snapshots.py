"""Tests for snapshot management (rollback, expire)."""

import datetime
import pytest

from lakehouse.catalog import (
    insert_rows,
    update_rows,
    get_snapshots,
    rollback_table,
    expire_snapshots,
)


class TestRollback:
    """Test rollback_table function."""

    def test_rollback_to_snapshot_id(self, test_catalog, query_engine):
        """Test rolling back to a specific snapshot."""
        insert_rows(test_catalog, "expenses", [
            {"id": 6000, "category": "original", "amount": 50.0, "currency": "USD"},
        ])
        snapshots = get_snapshots(test_catalog, "expenses")
        first_snap = snapshots[-1]["snapshot_id"]

        # Make a change
        insert_rows(test_catalog, "expenses", [
            {"id": 6001, "category": "added_later", "amount": 100.0, "currency": "USD"},
        ])

        # Rollback
        result = rollback_table(test_catalog, "expenses", snapshot_id=first_snap)
        assert "Rolled back" in result["message"]

        # Verify only original data remains
        query_engine.refresh()
        rows = query_engine.execute("SELECT * FROM expenses WHERE id IN (6000, 6001)")
        ids = rows["id"].tolist()
        assert 6000 in ids
        assert 6001 not in ids

    def test_rollback_to_timestamp(self, test_catalog, query_engine):
        """Test rolling back to a timestamp."""
        insert_rows(test_catalog, "expenses", [
            {"id": 6010, "category": "original", "amount": 10.0, "currency": "USD"},
        ])

        # Use a future timestamp that includes this snapshot
        future = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=5)).isoformat()

        # Add more data
        insert_rows(test_catalog, "expenses", [
            {"id": 6011, "category": "later", "amount": 20.0, "currency": "USD"},
        ])

        # Get the first snapshot timestamp
        snapshots = get_snapshots(test_catalog, "expenses")
        first_ts = snapshots[0]["timestamp"]

        result = rollback_table(test_catalog, "expenses", timestamp=first_ts)
        assert "Rolled back" in result["message"]

    def test_rollback_already_at_snapshot(self, test_catalog):
        """Test rollback when already at target snapshot."""
        insert_rows(test_catalog, "expenses", [
            {"id": 6020, "category": "test", "amount": 10.0, "currency": "USD"},
        ])
        snapshots = get_snapshots(test_catalog, "expenses")
        current = snapshots[-1]["snapshot_id"]

        result = rollback_table(test_catalog, "expenses", snapshot_id=current)
        assert "no rollback needed" in result["message"].lower()

    def test_rollback_no_params_raises_error(self, test_catalog):
        """Test rollback with neither snapshot_id nor timestamp."""
        with pytest.raises(ValueError, match="Either snapshot_id or timestamp"):
            rollback_table(test_catalog, "expenses")

    def test_rollback_invalid_snapshot_raises_error(self, test_catalog):
        """Test rollback to non-existent snapshot."""
        insert_rows(test_catalog, "expenses", [
            {"id": 6030, "category": "test", "amount": 10.0, "currency": "USD"},
        ])
        with pytest.raises(ValueError, match="not found"):
            rollback_table(test_catalog, "expenses", snapshot_id=999999999999)

    def test_rollback_nonexistent_table_raises_error(self, test_catalog):
        """Test rollback on non-existent table."""
        with pytest.raises(ValueError, match="not found"):
            rollback_table(test_catalog, "nonexistent", snapshot_id=1)

    def test_rollback_empty_table_raises_error(self, test_catalog):
        """Test rollback on table with no snapshots."""
        with pytest.raises(ValueError, match="no snapshots"):
            rollback_table(test_catalog, "expenses", snapshot_id=1)


class TestExpireSnapshots:
    """Test expire_snapshots function."""

    def test_expire_no_params_raises_error(self, test_catalog):
        """Test expire without older_than or retain_last raises error."""
        with pytest.raises(ValueError, match="Either older_than or retain_last"):
            expire_snapshots(test_catalog, "expenses")

    def test_expire_nonexistent_table_raises_error(self, test_catalog):
        """Test expire on non-existent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            expire_snapshots(test_catalog, "nonexistent", older_than="30d")

    def test_expire_retain_last(self, test_catalog):
        """Test expiring snapshots with retain_last."""
        # Create multiple snapshots
        for i in range(4):
            insert_rows(test_catalog, "expenses", [
                {"id": 6100 + i, "category": f"batch_{i}", "amount": float(i), "currency": "USD"},
            ])

        before = get_snapshots(test_catalog, "expenses")
        assert len(before) >= 4

        result = expire_snapshots(test_catalog, "expenses", retain_last=2)
        assert "remaining" in result
        assert result["remaining"] >= 2

    def test_expire_older_than_duration(self, tmp_path):
        """Test expiring with duration string (recent data not expired)."""
        from pyiceberg.catalog.sql import SqlCatalog
        catalog = SqlCatalog(
            "expire_dur_test",
            **{"uri": f"sqlite:///{tmp_path / 'c.db'}", "warehouse": f"file://{tmp_path / 'w'}"},
        )
        (tmp_path / "w").mkdir()
        catalog.create_namespace("default")
        from lakehouse.catalog import create_sample_tables
        create_sample_tables(catalog)

        insert_rows(catalog, "expenses", [
            {"id": 6200, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        result = expire_snapshots(catalog, "expenses", older_than="30d")
        assert result["expired"] == 0

    def test_expire_older_than_iso_timestamp(self, tmp_path):
        """Test expiring with ISO timestamp in the past (nothing to expire)."""
        from pyiceberg.catalog.sql import SqlCatalog
        catalog = SqlCatalog(
            "expire_ts_test",
            **{"uri": f"sqlite:///{tmp_path / 'c.db'}", "warehouse": f"file://{tmp_path / 'w'}"},
        )
        (tmp_path / "w").mkdir()
        catalog.create_namespace("default")
        from lakehouse.catalog import create_sample_tables
        create_sample_tables(catalog)

        insert_rows(catalog, "expenses", [
            {"id": 6210, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        result = expire_snapshots(catalog, "expenses", older_than="2020-01-01T00:00:00")
        assert result["expired"] == 0
