"""Tests for UPSERT operations."""

import pytest

from lakehouse.catalog import insert_rows, upsert_rows


class TestUpsertRows:
    """Test upsert_rows function."""

    def test_upsert_insert_path(self, test_catalog, query_engine):
        """Test upsert inserts when no key match."""
        result = upsert_rows(test_catalog, "expenses", ["id"], [
            {"id": 3000, "category": "new", "amount": 42.0, "currency": "USD"},
        ])

        assert result["inserted"] == 1
        assert result["updated"] == 0

        query_engine.refresh()
        row = query_engine.execute("SELECT * FROM expenses WHERE id = 3000")
        assert len(row) == 1
        assert row.iloc[0]["category"] == "new"

    def test_upsert_update_path(self, test_catalog, query_engine):
        """Test upsert updates when key matches."""
        insert_rows(test_catalog, "expenses", [
            {"id": 3010, "category": "old", "amount": 10.0, "currency": "USD"},
        ])

        result = upsert_rows(test_catalog, "expenses", ["id"], [
            {"id": 3010, "category": "updated", "amount": 99.0, "currency": "EUR"},
        ])

        assert result["updated"] == 1
        assert result["inserted"] == 0

        query_engine.refresh()
        row = query_engine.execute("SELECT * FROM expenses WHERE id = 3010")
        assert len(row) == 1
        assert row.iloc[0]["category"] == "updated"
        assert row.iloc[0]["amount"] == 99.0

    def test_upsert_mixed(self, test_catalog, query_engine):
        """Test upsert with both inserts and updates."""
        insert_rows(test_catalog, "expenses", [
            {"id": 3020, "category": "existing", "amount": 50.0, "currency": "USD"},
        ])

        result = upsert_rows(test_catalog, "expenses", ["id"], [
            {"id": 3020, "category": "updated", "amount": 75.0, "currency": "USD"},
            {"id": 3021, "category": "brand_new", "amount": 25.0, "currency": "EUR"},
        ])

        assert result["updated"] == 1
        assert result["inserted"] == 1

        query_engine.refresh()
        rows = query_engine.execute("SELECT * FROM expenses WHERE id IN (3020, 3021) ORDER BY id")
        assert len(rows) == 2
        assert rows.iloc[0]["category"] == "updated"
        assert rows.iloc[1]["category"] == "brand_new"

    def test_upsert_into_empty_table(self, test_catalog, query_engine):
        """Test upsert into empty table (all inserts)."""
        result = upsert_rows(test_catalog, "expenses", ["id"], [
            {"id": 3030, "category": "a", "amount": 10.0, "currency": "USD"},
            {"id": 3031, "category": "b", "amount": 20.0, "currency": "USD"},
        ])

        assert result["inserted"] == 2
        assert result["updated"] == 0

    def test_upsert_empty_rows(self, test_catalog):
        """Test upsert with empty rows returns zeros."""
        result = upsert_rows(test_catalog, "expenses", ["id"], [])
        assert result["inserted"] == 0
        assert result["updated"] == 0

    def test_upsert_empty_key_columns_raises_error(self, test_catalog):
        """Test upsert with empty key_columns raises error."""
        with pytest.raises(ValueError, match="key_columns must not be empty"):
            upsert_rows(test_catalog, "expenses", [], [{"id": 1}])

    def test_upsert_invalid_key_column_raises_error(self, test_catalog):
        """Test upsert with non-existent key column raises error."""
        with pytest.raises(ValueError, match="does not exist"):
            upsert_rows(test_catalog, "expenses", ["nonexistent"], [{"id": 1}])

    def test_upsert_nonexistent_table_raises_error(self, test_catalog):
        """Test upsert into non-existent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            upsert_rows(test_catalog, "nonexistent_table", ["id"], [{"id": 1}])

    def test_upsert_preserves_unmatched_existing_rows(self, test_catalog, query_engine):
        """Test that upsert preserves existing rows not in the upsert set."""
        insert_rows(test_catalog, "expenses", [
            {"id": 3040, "category": "keep", "amount": 100.0, "currency": "USD"},
            {"id": 3041, "category": "target", "amount": 50.0, "currency": "USD"},
        ])

        upsert_rows(test_catalog, "expenses", ["id"], [
            {"id": 3041, "category": "changed", "amount": 75.0, "currency": "EUR"},
        ])

        query_engine.refresh()
        kept = query_engine.execute("SELECT * FROM expenses WHERE id = 3040")
        assert len(kept) == 1
        assert kept.iloc[0]["category"] == "keep"
        assert kept.iloc[0]["amount"] == 100.0
