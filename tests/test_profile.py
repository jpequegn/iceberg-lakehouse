"""Tests for data profiling and column statistics."""

import pytest

from lakehouse.catalog import profile_table, insert_rows


class TestProfileNumeric:
    """Test profiling numeric columns."""

    def test_profile_numeric_stats(self, test_catalog):
        """Numeric columns have min, max, mean, std, percentiles."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
            {"id": 2, "amount": 20.0},
            {"id": 3, "amount": 30.0},
            {"id": 4, "amount": 40.0},
            {"id": 5, "amount": 50.0},
        ])

        stats = profile_table(test_catalog, "expenses")
        amount = stats["columns"]["amount"]

        assert amount["type"] == "double"
        assert amount["nulls"] == 0
        assert amount["unique"] == 5
        assert amount["min"] == 10.0
        assert amount["max"] == 50.0
        assert amount["mean"] == 30.0
        assert amount["p50"] == 30.0
        assert "std" in amount
        assert "p25" in amount
        assert "p75" in amount

    def test_profile_integer_column(self, test_catalog):
        """Integer columns also get numeric stats."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
            {"id": 2, "amount": 20.0},
            {"id": 3, "amount": 30.0},
        ])

        stats = profile_table(test_catalog, "expenses")
        id_col = stats["columns"]["id"]

        assert id_col["type"] == "long"
        assert id_col["min"] == 1
        assert id_col["max"] == 3
        assert id_col["mean"] == 2.0


class TestProfileString:
    """Test profiling string columns."""

    def test_profile_string_top_values(self, test_catalog):
        """String columns have top values."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 10.0},
            {"id": 2, "category": "food", "amount": 20.0},
            {"id": 3, "category": "transport", "amount": 30.0},
            {"id": 4, "category": "food", "amount": 40.0},
            {"id": 5, "category": "transport", "amount": 50.0},
        ])

        stats = profile_table(test_catalog, "expenses")
        cat = stats["columns"]["category"]

        assert cat["type"] == "string"
        assert cat["unique"] == 2
        assert "top_values" in cat
        assert cat["top_values"]["food"] == 3
        assert cat["top_values"]["transport"] == 2

    def test_profile_string_nulls(self, test_catalog):
        """String columns count nulls correctly."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 10.0},
            {"id": 2, "category": None, "amount": 20.0},
            {"id": 3, "category": None, "amount": 30.0},
        ])

        stats = profile_table(test_catalog, "expenses")
        cat = stats["columns"]["category"]

        assert cat["nulls"] == 2
        assert cat["unique"] == 1


class TestProfileWithNulls:
    """Test profiling with null values."""

    def test_profile_numeric_with_nulls(self, test_catalog):
        """Numeric columns handle nulls in stats."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
            {"id": 2, "amount": None},
            {"id": 3, "amount": 30.0},
        ])

        stats = profile_table(test_catalog, "expenses")
        amount = stats["columns"]["amount"]

        assert amount["nulls"] == 1
        assert amount["unique"] == 2
        assert amount["min"] == 10.0
        assert amount["max"] == 30.0


class TestProfileFiltering:
    """Test profiling specific columns."""

    def test_profile_specific_columns(self, test_catalog):
        """Profile only selected columns."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 10.0, "currency": "USD"},
        ])

        stats = profile_table(test_catalog, "expenses", columns=["id", "amount"])

        assert "id" in stats["columns"]
        assert "amount" in stats["columns"]
        assert "category" not in stats["columns"]
        assert "currency" not in stats["columns"]
        assert stats["column_count"] == 2

    def test_profile_invalid_column(self, test_catalog):
        """Profile with invalid column name raises error."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "amount": 10.0},
        ])

        with pytest.raises(ValueError, match="Columns not found"):
            profile_table(test_catalog, "expenses", columns=["nonexistent"])


class TestProfileTableMetadata:
    """Test profile metadata."""

    def test_profile_row_count(self, test_catalog):
        """Profile reports correct row count."""
        insert_rows(test_catalog, "expenses", [
            {"id": i, "amount": float(i)} for i in range(1, 11)
        ])

        stats = profile_table(test_catalog, "expenses")
        assert stats["row_count"] == 10
        assert stats["table"] == "default.expenses"

    def test_profile_column_count(self, test_catalog):
        """Profile reports correct column count."""
        insert_rows(test_catalog, "expenses", [{"id": 1}])

        stats = profile_table(test_catalog, "expenses")
        # expenses table has 6 columns
        assert stats["column_count"] == 6

    def test_profile_empty_table(self, test_catalog):
        """Profile empty table returns zero counts."""
        stats = profile_table(test_catalog, "expenses")

        assert stats["row_count"] == 0
        assert all(c["nulls"] == 0 for c in stats["columns"].values())
        assert all(c["unique"] == 0 for c in stats["columns"].values())

    def test_profile_nonexistent_table(self, test_catalog):
        """Profile nonexistent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            profile_table(test_catalog, "nonexistent")

    def test_profile_with_namespace(self, test_catalog):
        """Profile with explicit namespace."""
        insert_rows(test_catalog, "expenses", [{"id": 1, "amount": 10.0}])

        stats = profile_table(test_catalog, "default.expenses")
        assert stats["table"] == "default.expenses"
        assert stats["row_count"] == 1


class TestProfileDateColumns:
    """Test profiling date/timestamp columns."""

    def test_profile_date_column(self, test_catalog):
        """Date columns have min and max."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "date": "2025-01-01", "amount": 10.0},
            {"id": 2, "date": "2025-06-15", "amount": 20.0},
            {"id": 3, "date": "2025-12-31", "amount": 30.0},
        ])

        stats = profile_table(test_catalog, "expenses", columns=["date"])
        date_col = stats["columns"]["date"]

        assert date_col["type"] == "date"
        assert "2025-01-01" in date_col["min"]
        assert "2025-12-31" in date_col["max"]
