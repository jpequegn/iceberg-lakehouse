"""Tests for audit log functionality."""

import json
import datetime
import pytest
from pathlib import Path

from lakehouse.audit import (
    log_operation,
    get_audit_log,
    clear_audit_log,
    MAX_AUDIT_ENTRIES,
)
from lakehouse.catalog import (
    insert_rows,
    update_rows,
    delete_rows,
    create_table,
)


@pytest.fixture
def audit_path(tmp_path):
    """Return a temporary audit log path."""
    return tmp_path / "audit.log"


# --- Core logging ---

class TestLogOperation:
    """Test logging operations."""

    def test_log_insert(self, audit_path):
        log_operation("expenses", "insert", rows_affected=3, source="cli", store_path=audit_path)

        entries = get_audit_log(store_path=audit_path)
        assert len(entries) == 1
        assert entries[0]["table"] == "expenses"
        assert entries[0]["operation"] == "insert"
        assert entries[0]["rows_affected"] == 3
        assert entries[0]["source"] == "cli"

    def test_log_update(self, audit_path):
        log_operation("expenses", "update", rows_affected=1, details={"filter": "id = 5"}, store_path=audit_path)

        entries = get_audit_log(store_path=audit_path)
        assert entries[0]["operation"] == "update"
        assert entries[0]["details"]["filter"] == "id = 5"

    def test_log_delete(self, audit_path):
        log_operation("expenses", "delete", rows_affected=2, store_path=audit_path)

        entries = get_audit_log(store_path=audit_path)
        assert entries[0]["operation"] == "delete"
        assert entries[0]["rows_affected"] == 2

    def test_log_includes_timestamp(self, audit_path):
        log_operation("expenses", "insert", store_path=audit_path)

        entries = get_audit_log(store_path=audit_path)
        assert "timestamp" in entries[0]
        # Should be ISO format
        datetime.datetime.fromisoformat(entries[0]["timestamp"])

    def test_log_multiple(self, audit_path):
        log_operation("expenses", "insert", rows_affected=3, store_path=audit_path)
        log_operation("expenses", "update", rows_affected=1, store_path=audit_path)
        log_operation("health", "delete", rows_affected=5, store_path=audit_path)

        entries = get_audit_log(store_path=audit_path)
        assert len(entries) == 3

    def test_log_default_source(self, audit_path):
        log_operation("expenses", "insert", store_path=audit_path)

        entries = get_audit_log(store_path=audit_path)
        assert entries[0]["source"] == "api"

    def test_jsonl_format(self, audit_path):
        """Each entry is a valid JSON line."""
        log_operation("expenses", "insert", rows_affected=1, store_path=audit_path)
        log_operation("expenses", "update", rows_affected=2, store_path=audit_path)

        lines = audit_path.read_text().strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            entry = json.loads(line)
            assert "timestamp" in entry
            assert "table" in entry


# --- Querying ---

class TestGetAuditLog:
    """Test querying the audit log."""

    def test_empty_log(self, audit_path):
        entries = get_audit_log(store_path=audit_path)
        assert entries == []

    def test_most_recent_first(self, audit_path):
        log_operation("t", "insert", rows_affected=1, store_path=audit_path)
        log_operation("t", "update", rows_affected=2, store_path=audit_path)
        log_operation("t", "delete", rows_affected=3, store_path=audit_path)

        entries = get_audit_log(store_path=audit_path)
        assert entries[0]["operation"] == "delete"
        assert entries[1]["operation"] == "update"
        assert entries[2]["operation"] == "insert"

    def test_filter_by_table(self, audit_path):
        log_operation("expenses", "insert", store_path=audit_path)
        log_operation("health", "insert", store_path=audit_path)
        log_operation("expenses", "update", store_path=audit_path)

        entries = get_audit_log(table_name="expenses", store_path=audit_path)
        assert len(entries) == 2
        assert all(e["table"] == "expenses" for e in entries)

    def test_filter_by_operation(self, audit_path):
        log_operation("expenses", "insert", store_path=audit_path)
        log_operation("expenses", "update", store_path=audit_path)
        log_operation("expenses", "insert", store_path=audit_path)

        entries = get_audit_log(operation="insert", store_path=audit_path)
        assert len(entries) == 2

    def test_filter_by_since(self, audit_path):
        # Log an entry with a past timestamp by writing directly
        old_entry = {
            "timestamp": "2020-01-01T00:00:00+00:00",
            "table": "expenses",
            "operation": "insert",
            "rows_affected": 1,
            "source": "api",
            "details": {},
        }
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with open(audit_path, "w") as f:
            f.write(json.dumps(old_entry) + "\n")

        log_operation("expenses", "update", store_path=audit_path)

        entries = get_audit_log(since="2025-01-01", store_path=audit_path)
        assert len(entries) == 1
        assert entries[0]["operation"] == "update"

    def test_combined_filters(self, audit_path):
        log_operation("expenses", "insert", store_path=audit_path)
        log_operation("expenses", "update", store_path=audit_path)
        log_operation("health", "insert", store_path=audit_path)

        entries = get_audit_log(table_name="expenses", operation="insert", store_path=audit_path)
        assert len(entries) == 1

    def test_limit(self, audit_path):
        for i in range(10):
            log_operation("t", "insert", rows_affected=i, store_path=audit_path)

        entries = get_audit_log(limit=3, store_path=audit_path)
        assert len(entries) == 3
        # Most recent first
        assert entries[0]["rows_affected"] == 9


# --- Clearing ---

class TestClearAuditLog:
    """Test clearing audit entries."""

    def test_clear_all(self, audit_path):
        for i in range(5):
            log_operation("t", "insert", store_path=audit_path)

        result = clear_audit_log(store_path=audit_path)
        assert result["cleared"] == 5

        entries = get_audit_log(store_path=audit_path)
        assert entries == []

    def test_clear_empty(self, audit_path):
        result = clear_audit_log(store_path=audit_path)
        assert result["cleared"] == 0

    def test_clear_older_than(self, audit_path):
        # Write old and new entries
        old = {
            "timestamp": "2020-01-01T00:00:00+00:00",
            "table": "t",
            "operation": "insert",
            "rows_affected": 1,
            "source": "api",
            "details": {},
        }
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with open(audit_path, "w") as f:
            f.write(json.dumps(old) + "\n")

        log_operation("t", "update", store_path=audit_path)

        result = clear_audit_log(older_than="2025-01-01T00:00:00+00:00", store_path=audit_path)
        assert result["cleared"] == 1

        entries = get_audit_log(store_path=audit_path)
        assert len(entries) == 1
        assert entries[0]["operation"] == "update"

    def test_clear_with_duration(self, audit_path):
        # Write an old entry
        old = {
            "timestamp": "2020-01-01T00:00:00+00:00",
            "table": "t",
            "operation": "old",
            "rows_affected": 0,
            "source": "api",
            "details": {},
        }
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with open(audit_path, "w") as f:
            f.write(json.dumps(old) + "\n")

        log_operation("t", "new", store_path=audit_path)

        result = clear_audit_log(older_than="1d", store_path=audit_path)
        assert result["cleared"] == 1

        entries = get_audit_log(store_path=audit_path)
        assert len(entries) == 1
        assert entries[0]["operation"] == "new"


# --- Cap enforcement ---

class TestCapEnforcement:
    """Test max entries cap."""

    def test_cap_enforced(self, audit_path):
        for i in range(MAX_AUDIT_ENTRIES + 50):
            log_operation("t", "insert", rows_affected=i, store_path=audit_path)

        lines = audit_path.read_text().strip().splitlines()
        assert len(lines) == MAX_AUDIT_ENTRIES

        # Most recent should be kept
        last_entry = json.loads(lines[-1])
        assert last_entry["rows_affected"] == MAX_AUDIT_ENTRIES + 49


# --- Integration ---

class TestInsertAuditIntegration:
    """Test that insert_rows auto-logs to audit."""

    def test_insert_logs_audit(self, test_catalog, audit_path):
        import lakehouse.audit as audit_mod
        original = audit_mod.DEFAULT_AUDIT_PATH
        audit_mod.DEFAULT_AUDIT_PATH = audit_path
        try:
            create_table(test_catalog, "audit_test", {"id": "long", "val": "string"})
            insert_rows(test_catalog, "audit_test", [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])

            entries = get_audit_log(store_path=audit_path)
            # Should have at least the insert entry
            insert_entries = [e for e in entries if e["operation"] == "insert"]
            assert len(insert_entries) >= 1
            assert insert_entries[0]["rows_affected"] == 2
        finally:
            audit_mod.DEFAULT_AUDIT_PATH = original

    def test_update_logs_audit(self, test_catalog, audit_path):
        import lakehouse.audit as audit_mod
        original = audit_mod.DEFAULT_AUDIT_PATH
        audit_mod.DEFAULT_AUDIT_PATH = audit_path
        try:
            create_table(test_catalog, "audit_upd", {"id": "long", "val": "string"})
            insert_rows(test_catalog, "audit_upd", [{"id": 1, "val": "a"}])
            update_rows(test_catalog, "audit_upd", "id = 1", {"val": "b"})

            entries = get_audit_log(operation="update", store_path=audit_path)
            assert len(entries) >= 1
            assert entries[0]["details"]["filter"] == "id = 1"
        finally:
            audit_mod.DEFAULT_AUDIT_PATH = original

    def test_delete_logs_audit(self, test_catalog, audit_path):
        import lakehouse.audit as audit_mod
        original = audit_mod.DEFAULT_AUDIT_PATH
        audit_mod.DEFAULT_AUDIT_PATH = audit_path
        try:
            create_table(test_catalog, "audit_del", {"id": "long", "val": "string"})
            insert_rows(test_catalog, "audit_del", [{"id": 1, "val": "a"}])
            delete_rows(test_catalog, "audit_del", "id = 1")

            entries = get_audit_log(operation="delete", store_path=audit_path)
            assert len(entries) >= 1
            assert entries[0]["rows_affected"] == 1
        finally:
            audit_mod.DEFAULT_AUDIT_PATH = original


# --- Edge cases ---

class TestEdgeCases:
    """Test edge cases."""

    def test_creates_parent_dirs(self, audit_path):
        nested = audit_path.parent / "deep" / "nested" / "audit.log"
        log_operation("t", "insert", store_path=nested)
        assert nested.exists()

    def test_corrupt_lines_skipped(self, audit_path):
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with open(audit_path, "w") as f:
            f.write("not valid json\n")
            f.write(json.dumps({"timestamp": "2026-01-01", "table": "t", "operation": "insert", "rows_affected": 1, "source": "api", "details": {}}) + "\n")

        entries = get_audit_log(store_path=audit_path)
        assert len(entries) == 1
