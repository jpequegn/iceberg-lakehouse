"""Tests for schema evolution operations."""

import pytest

from lakehouse.catalog import alter_table, get_table_schema, insert_rows


class TestSchemaEvolution:
    """Test alter_table function."""

    def test_add_column(self, test_catalog):
        """Test adding a column to a table."""
        result = alter_table(test_catalog, "expenses", "add_column", "tags", column_type="string")
        assert "Added column" in result
        assert "tags" in result

        schema = get_table_schema(test_catalog, "expenses")
        field_names = [f["name"] for f in schema["fields"]]
        assert "tags" in field_names

    def test_drop_column(self, test_catalog):
        """Test dropping a column from a table."""
        result = alter_table(test_catalog, "expenses", "drop_column", "currency")
        assert "Dropped column" in result

        schema = get_table_schema(test_catalog, "expenses")
        field_names = [f["name"] for f in schema["fields"]]
        assert "currency" not in field_names

    def test_rename_column(self, test_catalog):
        """Test renaming a column."""
        result = alter_table(test_catalog, "expenses", "rename_column", "description", new_name="desc")
        assert "Renamed column" in result

        schema = get_table_schema(test_catalog, "expenses")
        field_names = [f["name"] for f in schema["fields"]]
        assert "desc" in field_names
        assert "description" not in field_names

    def test_add_column_all_types(self, test_catalog):
        """Test adding columns of all supported types."""
        for col_type in ["string", "long", "double", "date", "timestamp"]:
            col_name = f"test_{col_type}"
            result = alter_table(test_catalog, "expenses", "add_column", col_name, column_type=col_type)
            assert "Added column" in result

    def test_add_column_missing_type_raises_error(self, test_catalog):
        """Test that add_column without type raises error."""
        with pytest.raises(ValueError, match="column_type is required"):
            alter_table(test_catalog, "expenses", "add_column", "new_col")

    def test_add_column_invalid_type_raises_error(self, test_catalog):
        """Test that add_column with invalid type raises error."""
        with pytest.raises(ValueError, match="Unsupported column type"):
            alter_table(test_catalog, "expenses", "add_column", "new_col", column_type="invalid_type")

    def test_rename_column_missing_new_name_raises_error(self, test_catalog):
        """Test that rename without new_name raises error."""
        with pytest.raises(ValueError, match="new_name is required"):
            alter_table(test_catalog, "expenses", "rename_column", "description")

    def test_unknown_operation_raises_error(self, test_catalog):
        """Test that unknown operation raises error."""
        with pytest.raises(ValueError, match="Unknown operation"):
            alter_table(test_catalog, "expenses", "widen_column", "id")

    def test_nonexistent_table_raises_error(self, test_catalog):
        """Test that altering non-existent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            alter_table(test_catalog, "nonexistent", "add_column", "col", column_type="string")

    def test_add_column_then_insert(self, test_catalog, query_engine):
        """Test that new column can be populated after adding."""
        alter_table(test_catalog, "expenses", "add_column", "priority", column_type="string")

        insert_rows(test_catalog, "expenses", [
            {"id": 5000, "category": "test", "amount": 10.0, "priority": "high"},
        ])

        query_engine.refresh()
        result = query_engine.execute("SELECT * FROM expenses WHERE id = 5000")
        assert len(result) == 1
        assert result.iloc[0]["priority"] == "high"

    def test_add_column_with_namespace_prefix(self, test_catalog):
        """Test adding column with fully qualified table name."""
        result = alter_table(test_catalog, "default.expenses", "add_column", "notes", column_type="string")
        assert "Added column" in result
