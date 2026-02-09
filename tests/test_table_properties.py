"""Tests for Iceberg table property management."""

import pytest

from lakehouse.catalog import (
    get_table_property,
    set_table_property,
    remove_table_property,
)


class TestGetTableProperty:
    """Test get_table_property function."""

    def test_get_nonexistent_property(self, test_catalog):
        """Test getting a property that doesn't exist returns None."""
        result = get_table_property(test_catalog, "expenses", "write.format.default")
        assert result is None

    def test_get_after_set(self, test_catalog):
        """Test getting a property after setting it."""
        set_table_property(test_catalog, "expenses", "write.format.default", "vortex")
        result = get_table_property(test_catalog, "expenses", "write.format.default")
        assert result == "vortex"

    def test_get_nonexistent_table_raises(self, test_catalog):
        """Test getting property from non-existent table raises."""
        with pytest.raises(ValueError, match="not found"):
            get_table_property(test_catalog, "nonexistent", "some.key")

    def test_get_with_namespace(self, test_catalog):
        """Test getting property with qualified table name."""
        set_table_property(test_catalog, "default.expenses", "custom.key", "value1")
        result = get_table_property(test_catalog, "default.expenses", "custom.key")
        assert result == "value1"


class TestSetTableProperty:
    """Test set_table_property function."""

    def test_set_property(self, test_catalog):
        """Test setting a table property."""
        msg = set_table_property(test_catalog, "expenses", "write.format.default", "vortex")
        assert "write.format.default" in msg
        assert "vortex" in msg

    def test_set_overwrites_existing(self, test_catalog):
        """Test setting a property that already exists overwrites it."""
        set_table_property(test_catalog, "expenses", "custom.key", "value1")
        set_table_property(test_catalog, "expenses", "custom.key", "value2")
        result = get_table_property(test_catalog, "expenses", "custom.key")
        assert result == "value2"

    def test_set_multiple_properties(self, test_catalog):
        """Test setting multiple properties."""
        set_table_property(test_catalog, "expenses", "key1", "val1")
        set_table_property(test_catalog, "expenses", "key2", "val2")
        assert get_table_property(test_catalog, "expenses", "key1") == "val1"
        assert get_table_property(test_catalog, "expenses", "key2") == "val2"

    def test_set_nonexistent_table_raises(self, test_catalog):
        """Test setting property on non-existent table raises."""
        with pytest.raises(ValueError, match="not found"):
            set_table_property(test_catalog, "nonexistent", "key", "val")

    def test_set_different_tables(self, test_catalog):
        """Test setting properties on different tables."""
        set_table_property(test_catalog, "expenses", "key", "expenses_val")
        set_table_property(test_catalog, "health", "key", "health_val")
        assert get_table_property(test_catalog, "expenses", "key") == "expenses_val"
        assert get_table_property(test_catalog, "health", "key") == "health_val"


class TestRemoveTableProperty:
    """Test remove_table_property function."""

    def test_remove_property(self, test_catalog):
        """Test removing a table property."""
        set_table_property(test_catalog, "expenses", "custom.key", "value")
        msg = remove_table_property(test_catalog, "expenses", "custom.key")
        assert "Removed" in msg
        assert get_table_property(test_catalog, "expenses", "custom.key") is None

    def test_remove_nonexistent_property_raises(self, test_catalog):
        """Test removing non-existent property raises."""
        with pytest.raises(ValueError, match="not found"):
            remove_table_property(test_catalog, "expenses", "nonexistent.key")

    def test_remove_nonexistent_table_raises(self, test_catalog):
        """Test removing property from non-existent table raises."""
        with pytest.raises(ValueError, match="not found"):
            remove_table_property(test_catalog, "nonexistent", "key")
