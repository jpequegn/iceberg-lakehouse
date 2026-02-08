"""Tests for time travel query support."""

import datetime
import time
import pytest

from lakehouse.catalog import insert_rows, get_snapshots, scan_as_of


class TestTimeTravel:
    """Test time travel features."""

    def _insert_and_get_snapshot(self, test_catalog):
        """Helper: insert data and return the snapshot ID."""
        insert_rows(test_catalog, "expenses", [
            {"id": 4000, "category": "original", "amount": 50.0, "currency": "USD"},
        ])
        snapshots = get_snapshots(test_catalog, "expenses")
        return snapshots[-1]["snapshot_id"]

    def test_list_snapshots_empty_table(self, test_catalog):
        """Test listing snapshots for table with no data."""
        snapshots = get_snapshots(test_catalog, "expenses")
        assert isinstance(snapshots, list)
        assert len(snapshots) == 0

    def test_list_snapshots_after_insert(self, test_catalog):
        """Test that insert creates a snapshot."""
        insert_rows(test_catalog, "expenses", [
            {"id": 4010, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        snapshots = get_snapshots(test_catalog, "expenses")
        assert len(snapshots) >= 1
        assert "snapshot_id" in snapshots[0]
        assert "timestamp" in snapshots[0]

    def test_list_snapshots_multiple(self, test_catalog):
        """Test multiple snapshots after multiple writes."""
        insert_rows(test_catalog, "expenses", [
            {"id": 4020, "category": "first", "amount": 10.0, "currency": "USD"},
        ])
        insert_rows(test_catalog, "expenses", [
            {"id": 4021, "category": "second", "amount": 20.0, "currency": "USD"},
        ])

        snapshots = get_snapshots(test_catalog, "expenses")
        assert len(snapshots) >= 2

    def test_list_snapshots_nonexistent_table(self, test_catalog):
        """Test listing snapshots for non-existent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            get_snapshots(test_catalog, "nonexistent")

    def test_scan_as_of_snapshot_id(self, test_catalog):
        """Test scanning at a specific snapshot ID."""
        insert_rows(test_catalog, "expenses", [
            {"id": 4030, "category": "v1", "amount": 10.0, "currency": "USD"},
        ])
        snapshots = get_snapshots(test_catalog, "expenses")
        first_snap = snapshots[-1]["snapshot_id"]

        # Make another change
        insert_rows(test_catalog, "expenses", [
            {"id": 4031, "category": "v2", "amount": 20.0, "currency": "USD"},
        ])

        # Scan at the first snapshot - should only have the first row
        arrow = scan_as_of(test_catalog, "expenses", str(first_snap))
        df = arrow.to_pandas()
        ids = df["id"].tolist()
        assert 4030 in ids
        assert 4031 not in ids

    def test_scan_as_of_timestamp(self, test_catalog):
        """Test scanning at a specific timestamp."""
        insert_rows(test_catalog, "expenses", [
            {"id": 4040, "category": "before", "amount": 10.0, "currency": "USD"},
        ])

        # Use a future timestamp to include all data
        future_ts = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)).isoformat()
        arrow = scan_as_of(test_catalog, "expenses", future_ts)
        df = arrow.to_pandas()
        assert 4040 in df["id"].tolist()

    def test_scan_as_of_invalid_snapshot_id(self, test_catalog):
        """Test scanning with non-existent snapshot ID."""
        insert_rows(test_catalog, "expenses", [
            {"id": 4050, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        with pytest.raises(ValueError, match="not found"):
            scan_as_of(test_catalog, "expenses", "999999999999")

    def test_scan_as_of_nonexistent_table(self, test_catalog):
        """Test scanning non-existent table."""
        with pytest.raises(ValueError, match="not found"):
            scan_as_of(test_catalog, "nonexistent", "12345")

    def test_snapshot_fields(self, test_catalog):
        """Test that snapshot dicts have expected fields."""
        insert_rows(test_catalog, "expenses", [
            {"id": 4060, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        snapshots = get_snapshots(test_catalog, "expenses")
        snap = snapshots[0]
        assert "snapshot_id" in snap
        assert "timestamp" in snap
        assert "operation" in snap
        assert isinstance(snap["snapshot_id"], int)
