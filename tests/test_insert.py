"""Tests for INSERT operations."""

import datetime
import pytest

from lakehouse.catalog import insert_rows


class TestInsertRows:
    """Test insert_rows function."""

    def test_insert_single_row(self, test_catalog, query_engine):
        """Test inserting a single row."""
        rows = [
            {
                "id": 100,
                "date": "2025-01-15",
                "category": "test",
                "description": "Test expense",
                "amount": 42.50,
                "currency": "USD",
            }
        ]

        count = insert_rows(test_catalog, "expenses", rows)
        assert count == 1

        # Verify data was inserted
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 100")
        assert len(result) == 1
        assert result.iloc[0]["category"] == "test"
        assert result.iloc[0]["amount"] == 42.50

    def test_insert_multiple_rows(self, test_catalog, query_engine):
        """Test inserting multiple rows."""
        rows = [
            {"id": 101, "date": "2025-01-16", "category": "food", "amount": 25.00, "currency": "USD"},
            {"id": 102, "date": "2025-01-17", "category": "transport", "amount": 15.00, "currency": "USD"},
            {"id": 103, "date": "2025-01-18", "category": "entertainment", "amount": 50.00, "currency": "EUR"},
        ]

        count = insert_rows(test_catalog, "expenses", rows)
        assert count == 3

        # Verify all rows inserted
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id >= 101 ORDER BY id")
        assert len(result) == 3
        assert list(result["category"]) == ["food", "transport", "entertainment"]

    def test_insert_with_missing_optional_fields(self, test_catalog, query_engine):
        """Test inserting rows with missing optional fields."""
        rows = [
            {"id": 200, "category": "partial"},  # Missing most fields
        ]

        count = insert_rows(test_catalog, "expenses", rows)
        assert count == 1

        # Verify row was inserted with nulls
        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 200")
        assert len(result) == 1
        assert result.iloc[0]["category"] == "partial"
        assert result.iloc[0]["amount"] is None or str(result.iloc[0]["amount"]) == "nan"

    def test_insert_with_namespace_prefix(self, test_catalog, query_engine):
        """Test inserting with fully qualified table name."""
        rows = [{"id": 300, "category": "namespaced", "amount": 10.0, "currency": "GBP"}]

        count = insert_rows(test_catalog, "default.expenses", rows)
        assert count == 1

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 300")
        assert len(result) == 1

    def test_insert_empty_rows(self, test_catalog):
        """Test inserting empty list returns 0."""
        count = insert_rows(test_catalog, "expenses", [])
        assert count == 0

    def test_insert_into_nonexistent_table(self, test_catalog):
        """Test inserting into non-existent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            insert_rows(test_catalog, "nonexistent_table", [{"id": 1}])

    def test_insert_with_date_objects(self, test_catalog, query_engine):
        """Test inserting with Python date objects."""
        rows = [
            {
                "id": 400,
                "date": datetime.date(2025, 6, 15),
                "category": "date_test",
                "amount": 100.0,
                "currency": "USD",
            }
        ]

        count = insert_rows(test_catalog, "expenses", rows)
        assert count == 1

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 400")
        assert len(result) == 1

    def test_insert_with_type_coercion(self, test_catalog, query_engine):
        """Test that string numbers are coerced to proper types."""
        rows = [
            {
                "id": "500",  # String instead of int
                "amount": "75.25",  # String instead of float
                "category": "coercion_test",
                "currency": "USD",
            }
        ]

        count = insert_rows(test_catalog, "expenses", rows)
        assert count == 1

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 500")
        assert len(result) == 1
        assert result.iloc[0]["id"] == 500
        assert result.iloc[0]["amount"] == 75.25

    def test_insert_into_health_table(self, test_catalog, query_engine):
        """Test inserting into health table with timestamp."""
        rows = [
            {
                "id": 100,
                "timestamp": "2025-02-01T10:30:00",
                "metric_type": "heart_rate",
                "value": 72.0,
                "unit": "bpm",
                "source": "watch",
            }
        ]

        count = insert_rows(test_catalog, "health", rows)
        assert count == 1

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM health WHERE id = 100")
        assert len(result) == 1
        assert result.iloc[0]["metric_type"] == "heart_rate"

    def test_insert_into_notes_table(self, test_catalog, query_engine):
        """Test inserting into notes table."""
        rows = [
            {
                "id": 100,
                "created_at": "2025-02-01T14:00:00",
                "title": "Test Note",
                "content": "This is a test note content.",
                "source": "test",
                "tags": '["test", "example"]',
            }
        ]

        count = insert_rows(test_catalog, "notes", rows)
        assert count == 1

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM notes WHERE id = 100")
        assert len(result) == 1
        assert result.iloc[0]["title"] == "Test Note"
