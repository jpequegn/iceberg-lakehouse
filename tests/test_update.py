"""Tests for UPDATE operations."""

import pytest

from lakehouse.catalog import insert_rows, update_rows


class TestUpdateRows:
    """Test update_rows function."""

    def test_update_single_row_by_id(self, test_catalog, query_engine):
        """Test updating a single row by ID."""
        # Insert test data
        insert_rows(test_catalog, "expenses", [
            {"id": 1000, "category": "food", "amount": 50.0, "currency": "USD"},
        ])

        # Update the row
        count = update_rows(
            test_catalog,
            "expenses",
            "id = 1000",
            {"amount": 75.0, "category": "groceries"},
        )
        assert count == 1

        # Verify update
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 1000")
        assert len(result) == 1
        assert result.iloc[0]["amount"] == 75.0
        assert result.iloc[0]["category"] == "groceries"

    def test_update_multiple_rows(self, test_catalog, query_engine):
        """Test updating multiple rows matching a filter."""
        # Insert test data
        insert_rows(test_catalog, "expenses", [
            {"id": 1001, "category": "food", "amount": 25.0, "currency": "USD"},
            {"id": 1002, "category": "food", "amount": 30.0, "currency": "USD"},
            {"id": 1003, "category": "transport", "amount": 15.0, "currency": "USD"},
        ])

        # Update all food expenses
        count = update_rows(
            test_catalog,
            "expenses",
            "category = 'food'",
            {"currency": "EUR"},
        )
        assert count == 2

        # Verify updates
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id IN (1001, 1002, 1003) ORDER BY id")
        assert len(result) == 3
        assert result.iloc[0]["currency"] == "EUR"  # id=1001, was food
        assert result.iloc[1]["currency"] == "EUR"  # id=1002, was food
        assert result.iloc[2]["currency"] == "USD"  # id=1003, was transport (unchanged)

    def test_update_with_numeric_filter(self, test_catalog, query_engine):
        """Test updating with numeric comparison in filter."""
        # Insert test data
        insert_rows(test_catalog, "expenses", [
            {"id": 1010, "category": "misc", "amount": 100.0, "currency": "USD"},
            {"id": 1011, "category": "misc", "amount": 200.0, "currency": "USD"},
            {"id": 1012, "category": "misc", "amount": 50.0, "currency": "USD"},
        ])

        # Update expenses over 75
        count = update_rows(
            test_catalog,
            "expenses",
            "amount > 75",
            {"category": "expensive"},
        )
        assert count == 2

        # Verify
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id IN (1010, 1011, 1012) ORDER BY id")
        assert result.iloc[0]["category"] == "expensive"  # 100 > 75
        assert result.iloc[1]["category"] == "expensive"  # 200 > 75
        assert result.iloc[2]["category"] == "misc"  # 50 <= 75

    def test_update_no_matching_rows(self, test_catalog, query_engine):
        """Test updating when no rows match the filter."""
        # Insert test data
        insert_rows(test_catalog, "expenses", [
            {"id": 1020, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        # Try to update non-existent category
        count = update_rows(
            test_catalog,
            "expenses",
            "category = 'nonexistent'",
            {"amount": 999.0},
        )
        assert count == 0

        # Verify original data unchanged
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 1020")
        assert result.iloc[0]["amount"] == 10.0

    def test_update_to_null(self, test_catalog, query_engine):
        """Test updating a column to NULL."""
        # Insert test data
        insert_rows(test_catalog, "expenses", [
            {"id": 1030, "category": "test", "description": "Has description", "amount": 10.0, "currency": "USD"},
        ])

        # Update description to null
        count = update_rows(
            test_catalog,
            "expenses",
            "id = 1030",
            {"description": None},
        )
        assert count == 1

        # Verify
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 1030")
        assert result.iloc[0]["description"] is None

    def test_update_with_string_containing_quotes(self, test_catalog, query_engine):
        """Test updating with string values containing quotes."""
        # Insert test data
        insert_rows(test_catalog, "expenses", [
            {"id": 1040, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        # Update with a string containing quotes
        count = update_rows(
            test_catalog,
            "expenses",
            "id = 1040",
            {"description": "It's a test with 'quotes'"},
        )
        assert count == 1

        # Verify
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 1040")
        assert result.iloc[0]["description"] == "It's a test with 'quotes'"

    def test_update_empty_filter_raises_error(self, test_catalog):
        """Test that empty filter raises an error."""
        with pytest.raises(ValueError, match="Filter expression is required"):
            update_rows(test_catalog, "expenses", "", {"amount": 10.0})

    def test_update_empty_updates_raises_error(self, test_catalog):
        """Test that empty updates dict raises an error."""
        with pytest.raises(ValueError, match="Updates dictionary cannot be empty"):
            update_rows(test_catalog, "expenses", "id = 1", {})

    def test_update_nonexistent_column_raises_error(self, test_catalog):
        """Test that updating non-existent column raises an error."""
        with pytest.raises(ValueError, match="does not exist"):
            update_rows(test_catalog, "expenses", "id = 1", {"nonexistent_col": 10})

    def test_update_nonexistent_table_raises_error(self, test_catalog):
        """Test that updating non-existent table raises an error."""
        with pytest.raises(ValueError, match="not found"):
            update_rows(test_catalog, "nonexistent_table", "id = 1", {"col": 10})

    def test_update_with_and_filter(self, test_catalog, query_engine):
        """Test updating with AND condition in filter."""
        # Insert test data
        insert_rows(test_catalog, "expenses", [
            {"id": 1050, "category": "food", "amount": 100.0, "currency": "USD"},
            {"id": 1051, "category": "food", "amount": 50.0, "currency": "USD"},
            {"id": 1052, "category": "transport", "amount": 100.0, "currency": "USD"},
        ])

        # Update food expenses over 75
        count = update_rows(
            test_catalog,
            "expenses",
            "category = 'food' AND amount > 75",
            {"description": "expensive food"},
        )
        assert count == 1

        # Verify
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 1050")
        assert result.iloc[0]["description"] == "expensive food"

    def test_update_preserves_other_rows(self, test_catalog, query_engine):
        """Test that update doesn't affect non-matching rows."""
        # Insert test data
        insert_rows(test_catalog, "expenses", [
            {"id": 1060, "category": "target", "amount": 10.0, "currency": "USD"},
            {"id": 1061, "category": "other", "amount": 20.0, "currency": "EUR"},
        ])

        # Update only target
        update_rows(test_catalog, "expenses", "id = 1060", {"amount": 99.0})

        # Verify other row unchanged
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 1061")
        assert result.iloc[0]["amount"] == 20.0
        assert result.iloc[0]["currency"] == "EUR"
