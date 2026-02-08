"""Tests for DELETE operations."""

import pytest

from lakehouse.catalog import insert_rows, delete_rows


class TestDeleteRows:
    """Test delete_rows function."""

    def test_delete_single_row_by_id(self, test_catalog, query_engine):
        """Test deleting a single row by ID."""
        insert_rows(test_catalog, "expenses", [
            {"id": 2000, "category": "food", "amount": 50.0, "currency": "USD"},
            {"id": 2001, "category": "transport", "amount": 20.0, "currency": "USD"},
        ])

        count = delete_rows(test_catalog, "expenses", "id = 2000")
        assert count == 1

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id IN (2000, 2001)")
        assert len(result) == 1
        assert result.iloc[0]["id"] == 2001

    def test_delete_multiple_rows(self, test_catalog, query_engine):
        """Test deleting multiple rows matching a filter."""
        insert_rows(test_catalog, "expenses", [
            {"id": 2010, "category": "food", "amount": 25.0, "currency": "USD"},
            {"id": 2011, "category": "food", "amount": 30.0, "currency": "USD"},
            {"id": 2012, "category": "transport", "amount": 15.0, "currency": "USD"},
        ])

        count = delete_rows(test_catalog, "expenses", "category = 'food'")
        assert count == 2

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id IN (2010, 2011, 2012)")
        assert len(result) == 1
        assert result.iloc[0]["id"] == 2012

    def test_delete_no_matching_rows(self, test_catalog, query_engine):
        """Test deleting when no rows match."""
        insert_rows(test_catalog, "expenses", [
            {"id": 2020, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        count = delete_rows(test_catalog, "expenses", "category = 'nonexistent'")
        assert count == 0

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 2020")
        assert len(result) == 1

    def test_delete_with_numeric_filter(self, test_catalog, query_engine):
        """Test deleting with numeric comparison."""
        insert_rows(test_catalog, "expenses", [
            {"id": 2030, "category": "a", "amount": 100.0, "currency": "USD"},
            {"id": 2031, "category": "b", "amount": 50.0, "currency": "USD"},
            {"id": 2032, "category": "c", "amount": 200.0, "currency": "USD"},
        ])

        count = delete_rows(test_catalog, "expenses", "amount > 75")
        assert count == 2

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id IN (2030, 2031, 2032)")
        assert len(result) == 1
        assert result.iloc[0]["amount"] == 50.0

    def test_delete_with_and_filter(self, test_catalog, query_engine):
        """Test deleting with compound filter."""
        insert_rows(test_catalog, "expenses", [
            {"id": 2040, "category": "food", "amount": 100.0, "currency": "USD"},
            {"id": 2041, "category": "food", "amount": 30.0, "currency": "USD"},
        ])

        count = delete_rows(test_catalog, "expenses", "category = 'food' AND amount > 50")
        assert count == 1

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id IN (2040, 2041)")
        assert len(result) == 1
        assert result.iloc[0]["id"] == 2041

    def test_delete_empty_filter_raises_error(self, test_catalog):
        """Test that empty filter raises an error."""
        with pytest.raises(ValueError, match="Filter expression is required"):
            delete_rows(test_catalog, "expenses", "")

    def test_delete_nonexistent_table_raises_error(self, test_catalog):
        """Test that deleting from non-existent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            delete_rows(test_catalog, "nonexistent_table", "id = 1")

    def test_delete_from_empty_table(self, test_catalog):
        """Test deleting from empty table returns 0."""
        count = delete_rows(test_catalog, "expenses", "id = 9999")
        assert count == 0

    def test_delete_preserves_other_rows(self, test_catalog, query_engine):
        """Test that delete doesn't affect non-matching rows."""
        insert_rows(test_catalog, "expenses", [
            {"id": 2050, "category": "target", "amount": 10.0, "currency": "USD"},
            {"id": 2051, "category": "keep", "amount": 20.0, "currency": "EUR"},
        ])

        delete_rows(test_catalog, "expenses", "id = 2050")

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 2051")
        assert len(result) == 1
        assert result.iloc[0]["amount"] == 20.0
        assert result.iloc[0]["currency"] == "EUR"
