"""Tests for maintenance policies functionality."""

import json
import pytest
from pathlib import Path

from lakehouse.maintenance import (
    set_maintenance_policy,
    get_maintenance_policy,
    remove_maintenance_policy,
    run_maintenance,
    check_maintenance_needed,
    DEFAULT_POLICY,
)
from lakehouse.catalog import (
    create_table,
    insert_rows,
)


@pytest.fixture
def maint_path(tmp_path):
    """Return a temporary maintenance store path."""
    return tmp_path / "maintenance.json"


@pytest.fixture
def maint_table(test_catalog):
    """Create a table for maintenance testing."""
    create_table(
        test_catalog,
        "maint_test",
        columns={"id": "long", "val": "string"},
    )
    insert_rows(
        test_catalog,
        "default.maint_test",
        [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}],
    )
    return "default.maint_test"


# --- set_maintenance_policy ---


class TestSetPolicy:
    def test_complete_policy(self, maint_path):
        """Set a complete policy with all keys."""
        policy = {
            "auto_compact_threshold": 15,
            "auto_expire_retain_last": 3,
            "auto_expire_older_than": "7d",
            "auto_cleanup_orphans": False,
        }
        result = set_maintenance_policy("expenses", policy, maint_path)
        assert result["table"] == "default.expenses"
        assert result["policy"]["auto_compact_threshold"] == 15
        assert result["policy"]["auto_expire_retain_last"] == 3
        assert result["policy"]["auto_expire_older_than"] == "7d"
        assert result["policy"]["auto_cleanup_orphans"] is False

    def test_partial_policy(self, maint_path):
        """Set a partial policy — defaults fill in."""
        result = set_maintenance_policy("expenses", {"auto_compact_threshold": 20}, maint_path)
        p = result["policy"]
        assert p["auto_compact_threshold"] == 20
        assert p["auto_expire_retain_last"] == DEFAULT_POLICY["auto_expire_retain_last"]
        assert p["auto_cleanup_orphans"] == DEFAULT_POLICY["auto_cleanup_orphans"]

    def test_empty_policy_uses_defaults(self, maint_path):
        """Empty policy dict uses all defaults."""
        result = set_maintenance_policy("expenses", {}, maint_path)
        p = result["policy"]
        assert p["auto_compact_threshold"] == DEFAULT_POLICY["auto_compact_threshold"]
        assert p["auto_expire_retain_last"] == DEFAULT_POLICY["auto_expire_retain_last"]

    def test_persists(self, maint_path):
        """Policy is persisted to file."""
        set_maintenance_policy("expenses", {"auto_compact_threshold": 5}, maint_path)
        assert maint_path.exists()
        data = json.loads(maint_path.read_text())
        assert "default.expenses" in data

    def test_has_timestamps(self, maint_path):
        """Policy includes created_at and last_run fields."""
        result = set_maintenance_policy("expenses", {}, maint_path)
        p = result["policy"]
        assert p["created_at"] is not None
        assert p["last_run"] is None

    def test_overwrite_existing(self, maint_path):
        """Setting policy again overwrites the old one."""
        set_maintenance_policy("expenses", {"auto_compact_threshold": 5}, maint_path)
        result = set_maintenance_policy("expenses", {"auto_compact_threshold": 20}, maint_path)
        assert result["policy"]["auto_compact_threshold"] == 20


# --- get_maintenance_policy ---


class TestGetPolicy:
    def test_existing(self, maint_path):
        """Get an existing policy."""
        set_maintenance_policy("expenses", {"auto_compact_threshold": 8}, maint_path)
        policy = get_maintenance_policy("expenses", maint_path)
        assert policy is not None
        assert policy["auto_compact_threshold"] == 8

    def test_nonexistent(self, maint_path):
        """Get policy for table with no policy returns None."""
        result = get_maintenance_policy("no_table", maint_path)
        assert result is None

    def test_bare_name(self, maint_path):
        """Bare name is normalized."""
        set_maintenance_policy("default.expenses", {"auto_compact_threshold": 8}, maint_path)
        policy = get_maintenance_policy("expenses", maint_path)
        assert policy is not None


# --- remove_maintenance_policy ---


class TestRemovePolicy:
    def test_remove_existing(self, maint_path):
        """Remove an existing policy."""
        set_maintenance_policy("expenses", {}, maint_path)
        result = remove_maintenance_policy("expenses", maint_path)
        assert "removed" in result["message"].lower()
        assert get_maintenance_policy("expenses", maint_path) is None

    def test_remove_nonexistent(self, maint_path):
        """Remove non-existent policy returns info message."""
        result = remove_maintenance_policy("no_table", maint_path)
        assert "no policy" in result["message"].lower()


# --- check_maintenance_needed ---


class TestCheckMaintenanceNeeded:
    def test_no_policy(self, test_catalog, maint_table, maint_path):
        """Check returns has_policy=False when no policy."""
        result = check_maintenance_needed(test_catalog, maint_table, maint_path)
        assert result["has_policy"] is False
        assert result["needs_compact"] is False

    def test_no_maintenance_needed(self, test_catalog, maint_table, maint_path):
        """Table with few files and default policy needs nothing."""
        set_maintenance_policy(maint_table, {"auto_compact_threshold": 10}, maint_path)
        result = check_maintenance_needed(test_catalog, maint_table, maint_path)
        assert result["has_policy"] is True
        assert result["needs_compact"] is False
        assert len(result["actions_needed"]) == 0

    def test_needs_compact(self, test_catalog, maint_path):
        """Table exceeding compact threshold shows needs_compact."""
        create_table(test_catalog, "many_files", columns={"id": "long"})
        for i in range(12):
            insert_rows(test_catalog, "default.many_files", [{"id": i}])

        set_maintenance_policy("default.many_files", {"auto_compact_threshold": 5}, maint_path)
        result = check_maintenance_needed(test_catalog, "default.many_files", maint_path)
        assert result["needs_compact"] is True
        assert any("compact" in a for a in result["actions_needed"])

    def test_needs_expire(self, test_catalog, maint_path):
        """Table with many snapshots shows needs_expire."""
        create_table(test_catalog, "many_snaps", columns={"id": "long"})
        for i in range(6):
            insert_rows(test_catalog, "default.many_snaps", [{"id": i}])

        set_maintenance_policy("default.many_snaps", {"auto_expire_retain_last": 2}, maint_path)
        result = check_maintenance_needed(test_catalog, "default.many_snaps", maint_path)
        assert result["needs_expire"] is True
        assert any("expire" in a for a in result["actions_needed"])


# --- run_maintenance ---


class TestRunMaintenance:
    def test_no_op(self, test_catalog, maint_table, maint_path):
        """Run with nothing needed returns empty list."""
        set_maintenance_policy(maint_table, {"auto_compact_threshold": 100}, maint_path)
        actions = run_maintenance(test_catalog, maint_table, store_path=maint_path)
        assert actions == []

    def test_dry_run(self, test_catalog, maint_path):
        """Dry run reports what would happen."""
        create_table(test_catalog, "dry_test", columns={"id": "long"})
        for i in range(12):
            insert_rows(test_catalog, "default.dry_test", [{"id": i}])

        set_maintenance_policy("default.dry_test", {"auto_compact_threshold": 5}, maint_path)
        actions = run_maintenance(
            test_catalog, "default.dry_test", dry_run=True, store_path=maint_path,
        )
        assert len(actions) >= 1
        assert all(a["status"] == "dry_run" for a in actions)

    def test_compact_runs(self, test_catalog, maint_path):
        """Run maintenance triggers compaction when needed."""
        create_table(test_catalog, "compact_me", columns={"id": "long"})
        for i in range(12):
            insert_rows(test_catalog, "default.compact_me", [{"id": i}])

        set_maintenance_policy("default.compact_me", {"auto_compact_threshold": 5}, maint_path)
        actions = run_maintenance(
            test_catalog, "default.compact_me", store_path=maint_path,
        )
        compact_actions = [a for a in actions if a["action"] == "compact"]
        assert len(compact_actions) >= 1
        assert compact_actions[0]["status"] == "completed"

    def test_run_all_tables(self, test_catalog, maint_table, maint_path):
        """Run maintenance for all tables with policies."""
        set_maintenance_policy(maint_table, {"auto_compact_threshold": 100}, maint_path)
        actions = run_maintenance(test_catalog, store_path=maint_path)
        assert isinstance(actions, list)

    def test_updates_last_run(self, test_catalog, maint_path):
        """Last run timestamp is updated after real maintenance."""
        create_table(test_catalog, "lastrun_test", columns={"id": "long"})
        for i in range(12):
            insert_rows(test_catalog, "default.lastrun_test", [{"id": i}])

        set_maintenance_policy("default.lastrun_test", {"auto_compact_threshold": 5}, maint_path)
        policy_before = get_maintenance_policy("default.lastrun_test", maint_path)
        assert policy_before["last_run"] is None

        run_maintenance(test_catalog, "default.lastrun_test", store_path=maint_path)
        policy_after = get_maintenance_policy("default.lastrun_test", maint_path)
        assert policy_after["last_run"] is not None


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, maint_path):
        """Maintenance store is valid JSON with expected structure."""
        set_maintenance_policy("expenses", {"auto_compact_threshold": 8}, maint_path)
        set_maintenance_policy("health", {"auto_expire_retain_last": 3}, maint_path)

        data = json.loads(maint_path.read_text())
        assert "default.expenses" in data
        assert "default.health" in data

        entry = data["default.expenses"]
        assert "auto_compact_threshold" in entry
        assert "auto_expire_retain_last" in entry
        assert "auto_cleanup_orphans" in entry
        assert "created_at" in entry
        assert "last_run" in entry

    def test_default_values_applied(self, maint_path):
        """Missing keys get default values."""
        set_maintenance_policy("test", {}, maint_path)
        policy = get_maintenance_policy("test", maint_path)
        assert policy["auto_compact_threshold"] == 10
        assert policy["auto_expire_retain_last"] == 5
        assert policy["auto_expire_older_than"] is None
        assert policy["auto_cleanup_orphans"] is True
