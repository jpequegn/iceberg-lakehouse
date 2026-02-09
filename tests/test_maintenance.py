"""Tests for table maintenance operations (compaction, status, cleanup)."""

from pathlib import Path

import pytest

from lakehouse.catalog import (
    compact_table,
    maintenance_status,
    cleanup_orphans,
    insert_rows,
    _get_table_data_dir,
)


class TestCompactTable:
    """Test table compaction."""

    def test_compact_multiple_files(self, test_catalog):
        """Compaction reduces file count after many small inserts."""
        # Do 5 separate inserts to create 5 data files
        for i in range(5):
            insert_rows(test_catalog, "expenses", [
                {"id": i * 10 + j, "amount": float(j)} for j in range(1, 4)
            ])

        result = compact_table(test_catalog, "expenses")

        assert result["files_before"] == 5
        assert result["files_after"] == 1
        assert result["rows"] == 15
        assert "Compacted" in result["message"]

    def test_compact_preserves_data(self, test_catalog):
        """Compaction preserves all rows."""
        for i in range(3):
            insert_rows(test_catalog, "expenses", [
                {"id": i + 1, "amount": float((i + 1) * 10)},
            ])

        compact_table(test_catalog, "expenses")

        # Verify data is intact by doing another scan
        table = test_catalog.load_table("default.expenses")
        data = table.scan().to_arrow()
        assert data.num_rows == 3

        ids = sorted(data.column("id").to_pylist())
        assert ids == [1, 2, 3]

    def test_compact_empty_table(self, test_catalog):
        """Compaction on empty table is a no-op."""
        result = compact_table(test_catalog, "expenses")

        assert result["files_before"] == 0
        assert result["files_after"] == 0
        assert result["rows"] == 0
        assert "empty" in result["message"].lower()

    def test_compact_single_file(self, test_catalog):
        """Compaction with single file still works."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
            {"id": 2, "amount": 20.0},
        ])

        result = compact_table(test_catalog, "expenses")

        assert result["files_before"] == 1
        assert result["files_after"] == 1
        assert result["rows"] == 2

    def test_compact_with_target_size(self, test_catalog):
        """Compaction accepts target_size_mb parameter."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
        ])

        result = compact_table(test_catalog, "expenses", target_size_mb=256)
        assert result["rows"] == 1

    def test_compact_nonexistent_table(self, test_catalog):
        """Compaction on nonexistent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            compact_table(test_catalog, "nonexistent")

    def test_compact_with_namespace(self, test_catalog):
        """Compaction with explicit namespace."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
        ])

        result = compact_table(test_catalog, "default.expenses")
        assert result["table"] == "default.expenses"


class TestMaintenanceStatus:
    """Test maintenance status reporting."""

    def test_status_after_inserts(self, test_catalog):
        """Status shows correct file count after multiple inserts."""
        for i in range(3):
            insert_rows(test_catalog, "expenses", [
                {"id": i + 1, "amount": float(i + 1)},
            ])

        status = maintenance_status(test_catalog, "expenses")

        assert status["table"] == "default.expenses"
        assert status["data_files"] == 3
        assert status["total_size_bytes"] > 0
        assert status["avg_file_size"] > 0
        assert status["snapshots"] == 3

    def test_status_empty_table(self, test_catalog):
        """Status for empty table shows zeros."""
        status = maintenance_status(test_catalog, "expenses")

        assert status["data_files"] == 0
        assert status["total_size_bytes"] == 0
        assert status["avg_file_size"] == 0
        assert status["snapshots"] == 0
        assert status["orphan_files"] == 0

    def test_status_nonexistent_table(self, test_catalog):
        """Status on nonexistent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            maintenance_status(test_catalog, "nonexistent")

    def test_status_with_namespace(self, test_catalog):
        """Status with explicit namespace."""
        insert_rows(test_catalog, "expenses", [{"id": 1, "amount": 10.0}])

        status = maintenance_status(test_catalog, "default.expenses")
        assert status["table"] == "default.expenses"
        assert status["data_files"] == 1

    def test_status_snapshot_count(self, test_catalog):
        """Status reports correct snapshot count."""
        for i in range(5):
            insert_rows(test_catalog, "expenses", [{"id": i + 1, "amount": float(i)}])

        status = maintenance_status(test_catalog, "expenses")
        assert status["snapshots"] == 5

    def test_status_detects_orphans(self, test_catalog):
        """Status detects orphan files on disk."""
        insert_rows(test_catalog, "expenses", [{"id": 1, "amount": 10.0}])

        # Place a fake orphan file in the data directory
        table = test_catalog.load_table("default.expenses")
        data_dir = _get_table_data_dir(table)
        orphan = data_dir / "orphan-stale-file.parquet"
        orphan.write_bytes(b"fake parquet data")

        status = maintenance_status(test_catalog, "expenses")
        assert status["orphan_files"] == 1
        assert status["orphan_bytes"] > 0


class TestCleanupOrphans:
    """Test orphan file cleanup."""

    def test_cleanup_no_orphans(self, test_catalog):
        """Cleanup with no orphans reports zero."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
        ])

        result = cleanup_orphans(test_catalog, "expenses", dry_run=True)

        assert result["orphan_files_found"] == 0
        assert result["orphan_files_removed"] == 0

    def test_cleanup_dry_run_detects_orphans(self, test_catalog):
        """Dry run finds orphans but doesn't delete them."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
        ])

        # Create orphan files manually
        table = test_catalog.load_table("default.expenses")
        data_dir = _get_table_data_dir(table)
        orphan1 = data_dir / "orphan-1.parquet"
        orphan2 = data_dir / "orphan-2.parquet"
        orphan1.write_bytes(b"fake data 1")
        orphan2.write_bytes(b"fake data 2")

        result = cleanup_orphans(test_catalog, "expenses", dry_run=True)

        assert result["orphan_files_found"] == 2
        assert result["orphan_files_removed"] == 0
        assert result["dry_run"] is True
        assert "files" in result
        assert len(result["files"]) == 2
        # Files should still exist
        assert orphan1.exists()
        assert orphan2.exists()

    def test_cleanup_removes_orphans(self, test_catalog):
        """Actual cleanup removes orphan files from disk."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
        ])

        # Create orphan files manually
        table = test_catalog.load_table("default.expenses")
        data_dir = _get_table_data_dir(table)
        orphan1 = data_dir / "orphan-old-1.parquet"
        orphan2 = data_dir / "orphan-old-2.parquet"
        orphan3 = data_dir / "orphan-old-3.parquet"
        for orphan in [orphan1, orphan2, orphan3]:
            orphan.write_bytes(b"fake parquet content")

        result = cleanup_orphans(test_catalog, "expenses", dry_run=False)

        assert result["orphan_files_found"] == 3
        assert result["orphan_files_removed"] == 3
        assert result["bytes_reclaimed"] > 0
        # Files should be deleted
        assert not orphan1.exists()
        assert not orphan2.exists()
        assert not orphan3.exists()

    def test_cleanup_after_cleanup_finds_nothing(self, test_catalog):
        """Running cleanup twice leaves no orphans."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
        ])

        # Create orphan
        table = test_catalog.load_table("default.expenses")
        data_dir = _get_table_data_dir(table)
        orphan = data_dir / "orphan-temp.parquet"
        orphan.write_bytes(b"temp data")

        # First cleanup
        cleanup_orphans(test_catalog, "expenses", dry_run=False)
        assert not orphan.exists()

        # Second cleanup should find nothing
        result = cleanup_orphans(test_catalog, "expenses", dry_run=False)
        assert result["orphan_files_found"] == 0

    def test_cleanup_empty_table(self, test_catalog):
        """Cleanup on empty table finds no orphans."""
        result = cleanup_orphans(test_catalog, "expenses", dry_run=True)
        assert result["orphan_files_found"] == 0

    def test_cleanup_nonexistent_table(self, test_catalog):
        """Cleanup on nonexistent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            cleanup_orphans(test_catalog, "nonexistent")

    def test_cleanup_preserves_current_data(self, test_catalog):
        """Cleanup only removes orphans, not current data files."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
            {"id": 2, "amount": 20.0},
        ])

        # Create an orphan
        table = test_catalog.load_table("default.expenses")
        data_dir = _get_table_data_dir(table)
        orphan = data_dir / "orphan-stale.parquet"
        orphan.write_bytes(b"stale data")

        cleanup_orphans(test_catalog, "expenses", dry_run=False)

        # Verify current data is still intact
        table = test_catalog.load_table("default.expenses")
        data = table.scan().to_arrow()
        assert data.num_rows == 2

    def test_cleanup_ignores_non_parquet(self, test_catalog):
        """Cleanup only targets .parquet files."""
        insert_rows(test_catalog, "expenses", [{"id": 1, "amount": 10.0}])

        table = test_catalog.load_table("default.expenses")
        data_dir = _get_table_data_dir(table)
        non_parquet = data_dir / "some-log.txt"
        non_parquet.write_text("this is a log file")

        result = cleanup_orphans(test_catalog, "expenses", dry_run=False)
        assert result["orphan_files_found"] == 0
        assert non_parquet.exists()  # Non-parquet files are untouched


class TestMaintenanceWorkflow:
    """Test the full maintenance workflow."""

    def test_compact_then_verify_status(self, test_catalog):
        """Compact reduces file count, status reflects the change."""
        # Create many small files
        for i in range(5):
            insert_rows(test_catalog, "expenses", [
                {"id": i + 1, "amount": float(i + 1)},
            ])

        status = maintenance_status(test_catalog, "expenses")
        assert status["data_files"] == 5
        assert status["snapshots"] == 5

        # Compact
        result = compact_table(test_catalog, "expenses")
        assert result["files_before"] == 5
        assert result["files_after"] == 1

        # Verify status after compact
        status = maintenance_status(test_catalog, "expenses")
        assert status["data_files"] == 1

        # Data should be intact
        table = test_catalog.load_table("default.expenses")
        data = table.scan().to_arrow()
        assert data.num_rows == 5

    def test_compact_creates_orphan_candidates(self, test_catalog):
        """After compact + expire, old data files become orphans."""
        # This test verifies the concept by manually placing orphans
        for i in range(3):
            insert_rows(test_catalog, "expenses", [
                {"id": i + 1, "amount": float(i + 1)},
            ])

        compact_table(test_catalog, "expenses")

        # Simulate orphans (as if expire had removed old snapshots)
        table = test_catalog.load_table("default.expenses")
        data_dir = _get_table_data_dir(table)
        for i in range(3):
            orphan = data_dir / f"old-shard-{i}.parquet"
            orphan.write_bytes(b"old data file contents")

        result = cleanup_orphans(test_catalog, "expenses", dry_run=False)
        assert result["orphan_files_removed"] == 3

        # Data should still be accessible
        table = test_catalog.load_table("default.expenses")
        data = table.scan().to_arrow()
        assert data.num_rows == 3

    def test_compact_after_updates(self, test_catalog):
        """Compaction works after update operations."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
            {"id": 2, "amount": 20.0},
            {"id": 3, "amount": 30.0},
        ])

        from lakehouse.catalog import update_rows
        update_rows(test_catalog, "expenses", "id = 1", {"amount": 99.0})
        update_rows(test_catalog, "expenses", "id = 2", {"amount": 88.0})

        result = compact_table(test_catalog, "expenses")
        assert result["files_after"] == 1
        assert result["rows"] == 3

    def test_compact_after_deletes(self, test_catalog):
        """Compaction works after delete operations."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
            {"id": 2, "amount": 20.0},
            {"id": 3, "amount": 30.0},
        ])

        from lakehouse.catalog import delete_rows
        delete_rows(test_catalog, "expenses", "id = 3")

        result = compact_table(test_catalog, "expenses")
        assert result["files_after"] == 1
        assert result["rows"] == 2
