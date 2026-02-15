"""Tests for snapshot retention policies."""

import json
import pytest

from lakehouse.retention import (
    set_retention_policy,
    get_retention_policy,
    list_retention_policies,
    remove_retention_policy,
    evaluate_retention,
)
from lakehouse.catalog import create_table, insert_rows


@pytest.fixture
def retention_path(tmp_path):
    """Return a temporary retention store path."""
    return tmp_path / "retention.json"


@pytest.fixture
def snapshotted_table(test_catalog):
    """Create a table with multiple snapshots."""
    create_table(test_catalog, "snap_test", columns={"id": "long", "name": "string"})
    for i in range(5):
        insert_rows(test_catalog, "default.snap_test", [{"id": i, "name": f"row_{i}"}])
    return test_catalog


# --- set_retention_policy ---


class TestSetRetentionPolicy:
    def test_set_basic(self, retention_path):
        """Set a basic retention policy."""
        result = set_retention_policy(
            "my_table",
            {"max_snapshot_count": 5, "min_snapshots_to_keep": 2},
            store_path=retention_path,
        )
        assert result["table"] == "default.my_table"
        assert result["policy"]["max_snapshot_count"] == 5
        assert result["policy"]["min_snapshots_to_keep"] == 2
        assert "created_at" in result["policy"]

    def test_set_age_policy(self, retention_path):
        """Set an age-based retention policy."""
        result = set_retention_policy(
            "my_table",
            {"max_snapshot_age_hours": 168},
            store_path=retention_path,
        )
        assert result["policy"]["max_snapshot_age_hours"] == 168

    def test_overwrite_policy(self, retention_path):
        """Setting a policy overwrites the previous one."""
        set_retention_policy("t", {"max_snapshot_count": 10}, store_path=retention_path)
        set_retention_policy("t", {"max_snapshot_count": 5}, store_path=retention_path)
        result = get_retention_policy("t", store_path=retention_path)
        assert result["policy"]["max_snapshot_count"] == 5

    def test_empty_name_raises(self, retention_path):
        """Empty table name raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            set_retention_policy("", {}, store_path=retention_path)

    def test_invalid_age_raises(self, retention_path):
        """Invalid age value raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            set_retention_policy("t", {"max_snapshot_age_hours": -1}, store_path=retention_path)

    def test_invalid_count_raises(self, retention_path):
        """Invalid count value raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            set_retention_policy("t", {"max_snapshot_count": 0}, store_path=retention_path)

    def test_invalid_min_keep_raises(self, retention_path):
        """Invalid min_snapshots_to_keep raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            set_retention_policy("t", {"min_snapshots_to_keep": 0}, store_path=retention_path)


# --- get_retention_policy ---


class TestGetRetentionPolicy:
    def test_get_existing(self, retention_path):
        """Get an existing policy."""
        set_retention_policy("t", {"max_snapshot_count": 5}, store_path=retention_path)
        result = get_retention_policy("t", store_path=retention_path)
        assert result["policy"] is not None
        assert result["policy"]["max_snapshot_count"] == 5

    def test_get_nonexistent(self, retention_path):
        """Get a nonexistent policy returns None."""
        result = get_retention_policy("no_such", store_path=retention_path)
        assert result["policy"] is None


# --- list_retention_policies ---


class TestListRetentionPolicies:
    def test_list_empty(self, retention_path):
        """Empty store returns empty list."""
        assert list_retention_policies(store_path=retention_path) == []

    def test_list_multiple(self, retention_path):
        """List multiple policies."""
        set_retention_policy("a", {"max_snapshot_count": 3}, store_path=retention_path)
        set_retention_policy("b", {"max_snapshot_age_hours": 48}, store_path=retention_path)
        policies = list_retention_policies(store_path=retention_path)
        assert len(policies) == 2
        tables = [p["table"] for p in policies]
        assert "default.a" in tables
        assert "default.b" in tables


# --- remove_retention_policy ---


class TestRemoveRetentionPolicy:
    def test_remove_existing(self, retention_path):
        """Remove an existing policy."""
        set_retention_policy("t", {"max_snapshot_count": 5}, store_path=retention_path)
        result = remove_retention_policy("t", store_path=retention_path)
        assert "removed" in result["message"].lower()
        assert get_retention_policy("t", store_path=retention_path)["policy"] is None

    def test_remove_nonexistent(self, retention_path):
        """Remove a nonexistent policy is a no-op."""
        result = remove_retention_policy("no_such", store_path=retention_path)
        assert "no retention policy" in result["message"].lower()


# --- evaluate_retention ---


class TestEvaluateRetention:
    def test_no_policy_returns_empty(self, snapshotted_table, retention_path):
        """No policy means no action."""
        results = evaluate_retention(snapshotted_table, "snap_test", store_path=retention_path)
        assert results == []

    def test_expire_by_count(self, snapshotted_table, retention_path):
        """Expire snapshots exceeding max count."""
        set_retention_policy("snap_test", {"max_snapshot_count": 2}, store_path=retention_path)
        results = evaluate_retention(snapshotted_table, "snap_test", store_path=retention_path)
        assert len(results) == 1
        assert results[0]["action"] == "expired"
        assert results[0]["expired"] >= 1

    def test_dry_run(self, snapshotted_table, retention_path):
        """Dry run previews without acting."""
        set_retention_policy("snap_test", {"max_snapshot_count": 2}, store_path=retention_path)
        results = evaluate_retention(snapshotted_table, "snap_test", dry_run=True, store_path=retention_path)
        assert len(results) == 1
        assert results[0]["action"] == "would_expire"
        assert results[0]["dry_run"] is True
        assert results[0]["would_expire"] >= 1

        # Verify nothing was actually expired
        table = snapshotted_table.load_table("default.snap_test")
        assert len(table.metadata.snapshots) == 5

    def test_min_keep_prevents_over_expiration(self, snapshotted_table, retention_path):
        """min_snapshots_to_keep prevents expiring too many."""
        set_retention_policy(
            "snap_test",
            {"max_snapshot_count": 1, "min_snapshots_to_keep": 4},
            store_path=retention_path,
        )
        results = evaluate_retention(snapshotted_table, "snap_test", dry_run=True, store_path=retention_path)
        assert len(results) == 1
        # Should only expire 1 (5 total, keep at least 4)
        assert results[0]["would_expire"] == 1
        assert results[0]["would_remain"] == 4

    def test_no_action_when_within_limits(self, snapshotted_table, retention_path):
        """No action when snapshots are within policy limits."""
        set_retention_policy("snap_test", {"max_snapshot_count": 10}, store_path=retention_path)
        results = evaluate_retention(snapshotted_table, "snap_test", store_path=retention_path)
        assert len(results) == 1
        assert results[0]["action"] == "no_action"

    def test_evaluate_all_tables(self, snapshotted_table, retention_path):
        """Evaluate all tables with policies."""
        set_retention_policy("snap_test", {"max_snapshot_count": 3}, store_path=retention_path)
        results = evaluate_retention(snapshotted_table, store_path=retention_path)
        assert len(results) >= 1
        assert results[0]["table"] == "default.snap_test"


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, retention_path):
        """Store is valid JSON with expected structure."""
        set_retention_policy("t", {"max_snapshot_count": 5, "max_snapshot_age_hours": 168}, store_path=retention_path)
        data = json.loads(retention_path.read_text())
        assert "default.t" in data
        entry = data["default.t"]
        assert "max_snapshot_count" in entry
        assert "max_snapshot_age_hours" in entry
        assert "min_snapshots_to_keep" in entry
        assert "created_at" in entry
        assert "last_evaluated" in entry
