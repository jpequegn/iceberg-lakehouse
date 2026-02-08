"""Tests for batch/transaction operations."""

import pytest

from lakehouse.catalog import insert_rows, execute_batch


class TestExecuteBatch:
    """Test execute_batch function."""

    def test_batch_multiple_inserts(self, test_catalog, query_engine):
        """Test batch with multiple insert operations."""
        ops = [
            {"action": "insert", "table_name": "expenses", "rows": [
                {"id": 7000, "category": "batch1", "amount": 10.0, "currency": "USD"},
            ]},
            {"action": "insert", "table_name": "expenses", "rows": [
                {"id": 7001, "category": "batch2", "amount": 20.0, "currency": "EUR"},
            ]},
        ]

        results = execute_batch(test_catalog, ops)
        assert len(results) == 2
        assert all(r["status"] == "ok" for r in results)
        assert results[0]["rows_affected"] == 1
        assert results[1]["rows_affected"] == 1

        query_engine.refresh()
        rows = query_engine.execute("SELECT * FROM expenses WHERE id IN (7000, 7001) ORDER BY id")
        assert len(rows) == 2

    def test_batch_insert_update_delete(self, test_catalog, query_engine):
        """Test batch with mixed operations."""
        # Setup
        insert_rows(test_catalog, "expenses", [
            {"id": 7010, "category": "to_update", "amount": 50.0, "currency": "USD"},
            {"id": 7011, "category": "to_delete", "amount": 30.0, "currency": "USD"},
        ])

        ops = [
            {"action": "insert", "table_name": "expenses", "rows": [
                {"id": 7012, "category": "new", "amount": 15.0, "currency": "GBP"},
            ]},
            {"action": "update", "table_name": "expenses", "filter": "id = 7010", "updates": {"amount": 99.0}},
            {"action": "delete", "table_name": "expenses", "filter": "id = 7011"},
        ]

        results = execute_batch(test_catalog, ops)
        assert len(results) == 3
        assert all(r["status"] == "ok" for r in results)

        query_engine.refresh()
        # Verify insert
        new_row = query_engine.execute("SELECT * FROM expenses WHERE id = 7012")
        assert len(new_row) == 1

        # Verify update
        updated = query_engine.execute("SELECT * FROM expenses WHERE id = 7010")
        assert updated.iloc[0]["amount"] == 99.0

        # Verify delete
        deleted = query_engine.execute("SELECT * FROM expenses WHERE id = 7011")
        assert len(deleted) == 0

    def test_batch_stops_on_first_error(self, test_catalog):
        """Test that batch stops on first failure and skips remaining."""
        ops = [
            {"action": "insert", "table_name": "expenses", "rows": [
                {"id": 7020, "category": "ok", "amount": 10.0, "currency": "USD"},
            ]},
            {"action": "insert", "table_name": "nonexistent_table", "rows": [
                {"id": 1, "field": "bad"},
            ]},
            {"action": "insert", "table_name": "expenses", "rows": [
                {"id": 7021, "category": "skipped", "amount": 20.0, "currency": "USD"},
            ]},
        ]

        results = execute_batch(test_catalog, ops)
        assert len(results) == 3
        assert results[0]["status"] == "ok"
        assert results[1]["status"] == "error"
        assert results[2]["status"] == "skipped"

    def test_batch_missing_action(self, test_catalog):
        """Test batch with missing action field."""
        ops = [{"table_name": "expenses", "rows": [{"id": 1}]}]
        results = execute_batch(test_catalog, ops)
        assert results[0]["status"] == "error"
        assert "action" in results[0]["message"].lower()

    def test_batch_missing_table_name(self, test_catalog):
        """Test batch with missing table_name."""
        ops = [{"action": "insert", "rows": [{"id": 1}]}]
        results = execute_batch(test_catalog, ops)
        assert results[0]["status"] == "error"
        assert "table_name" in results[0]["message"].lower()

    def test_batch_missing_rows_for_insert(self, test_catalog):
        """Test batch insert without rows."""
        ops = [{"action": "insert", "table_name": "expenses"}]
        results = execute_batch(test_catalog, ops)
        assert results[0]["status"] == "error"
        assert "rows" in results[0]["message"].lower()

    def test_batch_missing_filter_for_update(self, test_catalog):
        """Test batch update without filter."""
        ops = [{"action": "update", "table_name": "expenses", "updates": {"amount": 10}}]
        results = execute_batch(test_catalog, ops)
        assert results[0]["status"] == "error"

    def test_batch_missing_filter_for_delete(self, test_catalog):
        """Test batch delete without filter."""
        ops = [{"action": "delete", "table_name": "expenses"}]
        results = execute_batch(test_catalog, ops)
        assert results[0]["status"] == "error"

    def test_batch_unknown_action(self, test_catalog):
        """Test batch with unknown action."""
        ops = [{"action": "truncate", "table_name": "expenses"}]
        results = execute_batch(test_catalog, ops)
        assert results[0]["status"] == "error"
        assert "Unknown action" in results[0]["message"]

    def test_batch_empty_raises_error(self, test_catalog):
        """Test batch with empty operations list."""
        with pytest.raises(ValueError, match="must not be empty"):
            execute_batch(test_catalog, [])

    def test_batch_cross_table(self, test_catalog, query_engine):
        """Test batch across multiple tables."""
        ops = [
            {"action": "insert", "table_name": "expenses", "rows": [
                {"id": 7050, "category": "cross", "amount": 10.0, "currency": "USD"},
            ]},
            {"action": "insert", "table_name": "health", "rows": [
                {"id": 7050, "metric_type": "steps", "value": 5000.0, "unit": "count", "source": "test"},
            ]},
        ]

        results = execute_batch(test_catalog, ops)
        assert all(r["status"] == "ok" for r in results)

        query_engine.refresh()
        exp = query_engine.execute("SELECT * FROM expenses WHERE id = 7050")
        assert len(exp) == 1
        health = query_engine.execute("SELECT * FROM health WHERE id = 7050")
        assert len(health) == 1
