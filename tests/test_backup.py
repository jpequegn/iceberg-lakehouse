"""Tests for backup and restore."""

import json
import tarfile
import pytest

from lakehouse.backup import (
    backup_table,
    backup_namespace,
    restore_table,
    restore_namespace,
    list_backups,
    verify_backup,
)
from lakehouse.catalog import create_table, insert_rows, list_tables


@pytest.fixture
def backup_dir(tmp_path):
    return tmp_path / "backups"


@pytest.fixture
def backup_table_data(test_catalog):
    """Create a table with data for backup testing."""
    create_table(test_catalog, "backup_src", columns={"id": "long", "name": "string", "value": "double"})
    insert_rows(test_catalog, "default.backup_src", [
        {"id": 1, "name": "alice", "value": 10.0},
        {"id": 2, "name": "bob", "value": 20.0},
        {"id": 3, "name": "charlie", "value": 30.0},
    ])
    return test_catalog


# --- Backup table ---


class TestBackupTable:
    def test_backup_creates_archive(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        assert result["row_count"] == 3
        assert result["size_bytes"] > 0
        from pathlib import Path
        assert Path(result["archive"]).exists()

    def test_backup_archive_contents(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        with tarfile.open(result["archive"], "r:gz") as tar:
            names = tar.getnames()
            assert any("metadata.json" in n for n in names)
            assert any(".parquet" in n for n in names)

    def test_backup_metadata_structure(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        with tarfile.open(result["archive"], "r:gz") as tar:
            meta = [m for m in tar.getnames() if "metadata.json" in m][0]
            data = json.loads(tar.extractfile(meta).read())
            assert data["table_name"] == "default.backup_src"
            assert data["row_count"] == 3
            assert "columns" in data
            assert "data_checksum" in data


# --- Restore table ---


class TestRestoreTable:
    def test_restore_creates_table(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        restore_result = restore_table(backup_table_data, result["archive"], table_name="restored_tbl")
        assert restore_result["rows_restored"] == 3
        assert restore_result["table"] == "default.restored_tbl"

    def test_restore_preserves_data(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        restore_table(backup_table_data, result["archive"], table_name="restored2")

        # Verify data
        tbl = backup_table_data.load_table("default.restored2")
        arrow = tbl.scan().to_arrow()
        assert arrow.num_rows == 3

    def test_restore_with_rename(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        restore_result = restore_table(backup_table_data, result["archive"], table_name="renamed_tbl")
        assert restore_result["table"] == "default.renamed_tbl"

    def test_restore_existing_without_overwrite_raises(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        with pytest.raises(ValueError, match="already exists"):
            restore_table(backup_table_data, result["archive"], table_name="backup_src")

    def test_restore_with_overwrite(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        restore_result = restore_table(backup_table_data, result["archive"], table_name="backup_src", overwrite=True)
        assert restore_result["rows_restored"] == 3

    def test_restore_nonexistent_archive(self, backup_table_data):
        with pytest.raises(FileNotFoundError):
            restore_table(backup_table_data, "/nonexistent/archive.tar.gz")


# --- Backup namespace ---


class TestBackupNamespace:
    def test_backup_namespace(self, backup_table_data, backup_dir):
        # test_catalog already has sample tables from conftest
        result = backup_namespace(backup_table_data, "default", output_dir=backup_dir)
        assert result["table_count"] >= 1
        assert result["size_bytes"] > 0
        from pathlib import Path
        assert Path(result["archive"]).exists()


# --- Restore namespace ---


class TestRestoreNamespace:
    def test_restore_namespace(self, backup_table_data, backup_dir):
        # Backup the namespace
        bk = backup_namespace(backup_table_data, "default", output_dir=backup_dir)

        # Restore with overwrite (tables already exist)
        result = restore_namespace(backup_table_data, bk["archive"], overwrite=True)
        assert result["table_count"] >= 1


# --- List backups ---


class TestListBackups:
    def test_list_empty(self, backup_dir):
        assert list_backups(backup_dir) == []

    def test_list_after_backup(self, backup_table_data, backup_dir):
        backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        backups = list_backups(backup_dir)
        assert len(backups) == 1
        assert "file" in backups[0]
        assert "size_bytes" in backups[0]
        assert backups[0]["row_count"] == 3


# --- Verify backup ---


class TestVerifyBackup:
    def test_verify_valid(self, backup_table_data, backup_dir):
        result = backup_table(backup_table_data, "backup_src", output_dir=backup_dir)
        verify = verify_backup(result["archive"])
        assert verify["valid"] is True
        assert len(verify["tables_verified"]) == 1
        assert len(verify["issues"]) == 0

    def test_verify_nonexistent(self):
        with pytest.raises(FileNotFoundError):
            verify_backup("/nonexistent/archive.tar.gz")

    def test_verify_empty_table(self, test_catalog, backup_dir):
        """Backup and verify an empty table."""
        create_table(test_catalog, "empty_tbl", columns={"id": "long"})
        result = backup_table(test_catalog, "empty_tbl", output_dir=backup_dir)
        verify = verify_backup(result["archive"])
        assert verify["valid"] is True
