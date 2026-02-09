"""Tests for multi-namespace support."""

import pytest

from lakehouse.catalog import (
    list_namespaces,
    create_namespace,
    drop_namespace,
    get_namespace_properties,
    list_tables,
    create_table,
    insert_rows,
)


class TestListNamespaces:
    """Test listing namespaces."""

    def test_default_namespace_exists(self, test_catalog):
        namespaces = list_namespaces(test_catalog)
        assert "default" in namespaces

    def test_lists_created_namespaces(self, test_catalog):
        create_namespace(test_catalog, "staging")
        namespaces = list_namespaces(test_catalog)
        assert "default" in namespaces
        assert "staging" in namespaces

    def test_lists_multiple_namespaces(self, test_catalog):
        create_namespace(test_catalog, "staging")
        create_namespace(test_catalog, "analytics")
        namespaces = list_namespaces(test_catalog)
        assert len(namespaces) >= 3  # default + staging + analytics


class TestCreateNamespace:
    """Test creating namespaces."""

    def test_create_simple(self, test_catalog):
        result = create_namespace(test_catalog, "staging")
        assert result["namespace"] == "staging"
        assert "Created" in result["message"]

        namespaces = list_namespaces(test_catalog)
        assert "staging" in namespaces

    def test_create_with_properties(self, test_catalog):
        result = create_namespace(
            test_catalog, "analytics",
            properties={"owner": "data-team", "env": "prod"},
        )
        assert result["namespace"] == "analytics"
        assert result["properties"]["owner"] == "data-team"

    def test_create_duplicate_raises(self, test_catalog):
        create_namespace(test_catalog, "staging")
        with pytest.raises(ValueError, match="already exists"):
            create_namespace(test_catalog, "staging")

    def test_create_without_properties(self, test_catalog):
        result = create_namespace(test_catalog, "staging")
        assert result["properties"] == {}


class TestDropNamespace:
    """Test dropping namespaces."""

    def test_drop_empty_namespace(self, test_catalog):
        create_namespace(test_catalog, "staging")
        result = drop_namespace(test_catalog, "staging")
        assert result["namespace"] == "staging"
        assert "Dropped" in result["message"]

        namespaces = list_namespaces(test_catalog)
        assert "staging" not in namespaces

    def test_drop_nonexistent_raises(self, test_catalog):
        with pytest.raises(ValueError, match="not found"):
            drop_namespace(test_catalog, "nonexistent")

    def test_drop_nonempty_raises(self, test_catalog):
        """Dropping a namespace with tables should fail."""
        # 'default' has tables from create_sample_tables
        with pytest.raises(ValueError, match="not empty"):
            drop_namespace(test_catalog, "default")

    def test_drop_with_created_table_raises(self, test_catalog):
        create_namespace(test_catalog, "staging")
        create_table(test_catalog, "staging.events", {"id": "long", "name": "string"})

        with pytest.raises(ValueError, match="not empty"):
            drop_namespace(test_catalog, "staging")


class TestGetNamespaceProperties:
    """Test getting namespace properties."""

    def test_get_properties(self, test_catalog):
        create_namespace(
            test_catalog, "staging",
            properties={"owner": "data-team"},
        )
        result = get_namespace_properties(test_catalog, "staging")
        assert result["namespace"] == "staging"
        assert "owner" in result["properties"]
        assert result["properties"]["owner"] == "data-team"

    def test_get_empty_properties(self, test_catalog):
        create_namespace(test_catalog, "staging")
        result = get_namespace_properties(test_catalog, "staging")
        assert result["namespace"] == "staging"
        # Properties might be empty or have system defaults
        assert isinstance(result["properties"], dict)

    def test_get_nonexistent_raises(self, test_catalog):
        with pytest.raises(ValueError, match="not found"):
            get_namespace_properties(test_catalog, "nonexistent")


class TestListTablesWithNamespace:
    """Test listing tables with namespace filtering."""

    def test_list_default_tables(self, test_catalog):
        tables = list_tables(test_catalog, namespace="default")
        assert len(tables) >= 3  # expenses, health, notes from create_sample_tables

    def test_list_all_namespaces(self, test_catalog):
        create_namespace(test_catalog, "staging")
        create_table(test_catalog, "staging.events", {"id": "long"})

        all_tables = list_tables(test_catalog, namespace="*")
        # Should have default tables + staging.events
        assert any("staging.events" in t for t in all_tables)
        assert any("expenses" in t for t in all_tables)

    def test_list_empty_namespace(self, test_catalog):
        create_namespace(test_catalog, "staging")
        tables = list_tables(test_catalog, namespace="staging")
        assert tables == []

    def test_list_specific_namespace(self, test_catalog):
        create_namespace(test_catalog, "staging")
        create_table(test_catalog, "staging.events", {"id": "long"})
        create_table(test_catalog, "staging.logs", {"id": "long", "message": "string"})

        tables = list_tables(test_catalog, namespace="staging")
        assert len(tables) == 2
        table_names = [t.split(".")[-1] for t in tables]
        assert "events" in table_names
        assert "logs" in table_names


class TestCreateTableInNamespace:
    """Test creating tables in non-default namespaces."""

    def test_create_in_staging(self, test_catalog):
        create_namespace(test_catalog, "staging")
        result = create_table(
            test_catalog, "staging.events",
            {"id": "long", "payload": "string"},
        )
        assert result["table"] == "staging.events"

    def test_create_in_staging_with_partitions(self, test_catalog):
        create_namespace(test_catalog, "staging")
        result = create_table(
            test_catalog, "staging.events",
            {"id": "long", "event_date": "date", "value": "double"},
            partitions=["month(event_date)"],
        )
        assert result["table"] == "staging.events"
        assert result["partitions"] == ["month(event_date)"]

    def test_insert_into_namespace_table(self, test_catalog):
        create_namespace(test_catalog, "staging")
        create_table(test_catalog, "staging.events", {"id": "long", "name": "string"})

        count = insert_rows(test_catalog, "staging.events", [
            {"id": 1, "name": "event1"},
            {"id": 2, "name": "event2"},
        ])
        assert count == 2

        table = test_catalog.load_table("staging.events")
        data = table.scan().to_arrow()
        assert data.num_rows == 2

    def test_query_namespace_table(self, test_catalog):
        """Tables in other namespaces are queryable by full name."""
        create_namespace(test_catalog, "staging")
        create_table(test_catalog, "staging.events", {"id": "long", "name": "string"})
        insert_rows(test_catalog, "staging.events", [
            {"id": 1, "name": "event1"},
        ])

        # Verify via direct table load
        table = test_catalog.load_table("staging.events")
        data = table.scan().to_arrow()
        assert data.num_rows == 1
        assert data.column("name").to_pylist() == ["event1"]


class TestNamespaceWorkflow:
    """Test end-to-end namespace workflows."""

    def test_full_lifecycle(self, test_catalog):
        """Create namespace -> create table -> insert -> query -> drop table -> drop namespace."""
        # Create namespace
        create_namespace(test_catalog, "staging")
        assert "staging" in list_namespaces(test_catalog)

        # Create table
        create_table(test_catalog, "staging.events", {"id": "long", "value": "double"})
        tables = list_tables(test_catalog, namespace="staging")
        assert len(tables) == 1

        # Insert data
        insert_rows(test_catalog, "staging.events", [
            {"id": 1, "value": 10.0},
            {"id": 2, "value": 20.0},
        ])

        # Query data
        table = test_catalog.load_table("staging.events")
        data = table.scan().to_arrow()
        assert data.num_rows == 2

        # Drop table
        test_catalog.drop_table("staging.events")
        tables = list_tables(test_catalog, namespace="staging")
        assert len(tables) == 0

        # Drop namespace
        result = drop_namespace(test_catalog, "staging")
        assert "Dropped" in result["message"]
        assert "staging" not in list_namespaces(test_catalog)

    def test_multiple_namespaces_with_same_table_name(self, test_catalog):
        """Different namespaces can have tables with the same name."""
        create_namespace(test_catalog, "staging")
        create_namespace(test_catalog, "prod")

        create_table(test_catalog, "staging.events", {"id": "long", "name": "string"})
        create_table(test_catalog, "prod.events", {"id": "long", "name": "string"})

        insert_rows(test_catalog, "staging.events", [{"id": 1, "name": "staging_event"}])
        insert_rows(test_catalog, "prod.events", [{"id": 1, "name": "prod_event"}])

        staging_data = test_catalog.load_table("staging.events").scan().to_arrow()
        prod_data = test_catalog.load_table("prod.events").scan().to_arrow()

        assert staging_data.column("name").to_pylist() == ["staging_event"]
        assert prod_data.column("name").to_pylist() == ["prod_event"]

    def test_default_namespace_unaffected(self, test_catalog):
        """Creating other namespaces doesn't affect default."""
        create_namespace(test_catalog, "staging")
        create_namespace(test_catalog, "analytics")

        # Default tables should still be there
        default_tables = list_tables(test_catalog, namespace="default")
        table_names = [t.split(".")[-1] for t in default_tables]
        assert "expenses" in table_names
        assert "health" in table_names
        assert "notes" in table_names
