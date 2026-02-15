"""Tests for dependency auto-refresh."""

import pytest

from lakehouse.auto_refresh import (
    set_auto_refresh,
    get_auto_refresh,
    list_auto_refresh,
    remove_auto_refresh,
    get_refresh_plan,
    trigger_refresh,
    get_refresh_history,
)
from lakehouse.lineage import record_lineage


@pytest.fixture
def store(tmp_path):
    return tmp_path / "auto_refresh.json"


@pytest.fixture
def lineage_store(tmp_path):
    return tmp_path / "lineage.json"


@pytest.fixture
def lineage_chain(lineage_store):
    """Set up A -> B -> C lineage chain."""
    record_lineage(["default.table_a"], "default.table_b", operation="pipeline", store_path=lineage_store)
    record_lineage(["default.table_b"], "default.table_c", operation="materialized_view", store_path=lineage_store)
    return lineage_store


# --- set/get auto-refresh ---


class TestSetGetAutoRefresh:
    def test_set_and_get(self, store):
        set_auto_refresh("tbl", enabled=True, store_path=store)
        result = get_auto_refresh("tbl", store_path=store)
        assert result["enabled"] is True
        assert result["table"] == "default.tbl"

    def test_set_with_config(self, store):
        set_auto_refresh("tbl", config={"cascade_depth": 5, "refresh_matviews": False}, store_path=store)
        result = get_auto_refresh("tbl", store_path=store)
        assert result["cascade_depth"] == 5
        assert result["refresh_matviews"] is False

    def test_get_nonexistent(self, store):
        result = get_auto_refresh("nonexistent", store_path=store)
        assert result["enabled"] is False

    def test_disable(self, store):
        set_auto_refresh("tbl", enabled=False, store_path=store)
        result = get_auto_refresh("tbl", store_path=store)
        assert result["enabled"] is False


# --- list/remove ---


class TestListRemove:
    def test_list_empty(self, store):
        assert list_auto_refresh(store_path=store) == []

    def test_list_multiple(self, store):
        set_auto_refresh("t1", store_path=store)
        set_auto_refresh("t2", store_path=store)
        result = list_auto_refresh(store_path=store)
        assert len(result) == 2

    def test_remove(self, store):
        set_auto_refresh("tbl", store_path=store)
        remove_auto_refresh("tbl", store_path=store)
        assert list_auto_refresh(store_path=store) == []

    def test_remove_nonexistent(self, store):
        result = remove_auto_refresh("nonexistent", store_path=store)
        assert "No auto-refresh" in result["message"]


# --- refresh plan ---


class TestRefreshPlan:
    def test_plan_with_downstream(self, test_catalog, store, lineage_chain):
        set_auto_refresh("table_a", store_path=store)
        plan = get_refresh_plan(test_catalog, "table_a", store_path=store, lineage_store_path=lineage_chain)
        assert plan["downstream_count"] == 2
        assert len(plan["actions"]) > 0

    def test_plan_no_downstream(self, test_catalog, store, lineage_store):
        set_auto_refresh("isolated", store_path=store)
        plan = get_refresh_plan(test_catalog, "isolated", store_path=store, lineage_store_path=lineage_store)
        # Only cache invalidation for the source itself
        assert plan["downstream_count"] == 0

    def test_plan_respects_depth(self, test_catalog, store, lineage_chain):
        set_auto_refresh("table_a", config={"cascade_depth": 1}, store_path=store)
        plan = get_refresh_plan(test_catalog, "table_a", store_path=store, lineage_store_path=lineage_chain)
        # Depth limit 1 should only get table_b, not table_c
        downstream_tables = [a["table"] for a in plan["actions"] if a["depth"] > 0]
        assert "default.table_b" in downstream_tables
        assert "default.table_c" not in downstream_tables

    def test_plan_actions_sorted_by_depth(self, test_catalog, store, lineage_chain):
        set_auto_refresh("table_a", store_path=store)
        plan = get_refresh_plan(test_catalog, "table_a", store_path=store, lineage_store_path=lineage_chain)
        depths = [a["depth"] for a in plan["actions"]]
        assert depths == sorted(depths)

    def test_circular_dependency(self, test_catalog, store, lineage_store):
        """Circular deps shouldn't infinite loop (lineage's BFS handles this)."""
        record_lineage(["default.x"], "default.y", store_path=lineage_store)
        record_lineage(["default.y"], "default.x", store_path=lineage_store)
        set_auto_refresh("x", store_path=store)
        plan = get_refresh_plan(test_catalog, "x", store_path=store, lineage_store_path=lineage_store)
        # Should complete without infinite loop
        assert plan["downstream_count"] >= 1


# --- trigger refresh ---


class TestTriggerRefresh:
    def test_trigger_with_cache_invalidation(self, test_catalog, store, lineage_chain):
        set_auto_refresh("table_a", config={"refresh_matviews": False, "rerun_pipelines": False}, store_path=store)
        result = trigger_refresh(test_catalog, "table_a", store_path=store, lineage_store_path=lineage_chain)
        assert result["actions_executed"] > 0
        # All cache invalidation should succeed
        cache_actions = [r for r in result["results"] if r["action"] == "invalidate_cache"]
        for a in cache_actions:
            assert a["status"] == "success"

    def test_trigger_records_history(self, test_catalog, store, lineage_chain):
        set_auto_refresh("table_a", store_path=store)
        trigger_refresh(test_catalog, "table_a", store_path=store, lineage_store_path=lineage_chain)
        history = get_refresh_history(store_path=store)
        assert len(history) == 1
        assert history[0]["table"] == "default.table_a"

    def test_trigger_no_dependencies(self, test_catalog, store, lineage_store):
        set_auto_refresh("isolated", store_path=store)
        result = trigger_refresh(test_catalog, "isolated", store_path=store, lineage_store_path=lineage_store)
        # Only the source table's cache invalidation
        assert result["actions_executed"] >= 1


# --- refresh history ---


class TestRefreshHistory:
    def test_empty_history(self, store):
        assert get_refresh_history(store_path=store) == []

    def test_history_accumulates(self, test_catalog, store, lineage_store):
        set_auto_refresh("tbl", store_path=store)
        trigger_refresh(test_catalog, "tbl", store_path=store, lineage_store_path=lineage_store)
        trigger_refresh(test_catalog, "tbl", store_path=store, lineage_store_path=lineage_store)
        history = get_refresh_history(store_path=store)
        assert len(history) == 2

    def test_history_filter_by_table(self, test_catalog, store, lineage_store):
        set_auto_refresh("t1", store_path=store)
        set_auto_refresh("t2", store_path=store)
        trigger_refresh(test_catalog, "t1", store_path=store, lineage_store_path=lineage_store)
        trigger_refresh(test_catalog, "t2", store_path=store, lineage_store_path=lineage_store)
        history = get_refresh_history(table_name="t1", store_path=store)
        assert len(history) == 1
        assert history[0]["table"] == "default.t1"

    def test_history_limit(self, test_catalog, store, lineage_store):
        set_auto_refresh("tbl", store_path=store)
        for _ in range(10):
            trigger_refresh(test_catalog, "tbl", store_path=store, lineage_store_path=lineage_store)
        history = get_refresh_history(limit=3, store_path=store)
        assert len(history) == 3
