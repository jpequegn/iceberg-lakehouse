"""Tests for partitioning support (create table, get partitions, stats)."""

import datetime
from pathlib import Path

import pytest

from lakehouse.catalog import (
    create_table,
    get_partitions,
    get_partition_stats,
    insert_rows,
    _parse_transform,
)


class TestParseTransform:
    """Test partition transform string parsing."""

    def test_identity(self):
        transform, col = _parse_transform("identity(category)")
        assert col == "category"
        assert str(transform) == "identity"

    def test_year(self):
        transform, col = _parse_transform("year(event_date)")
        assert col == "event_date"
        assert str(transform) == "year"

    def test_month(self):
        transform, col = _parse_transform("month(event_date)")
        assert col == "event_date"
        assert str(transform) == "month"

    def test_day(self):
        transform, col = _parse_transform("day(event_date)")
        assert col == "event_date"
        assert str(transform) == "day"

    def test_hour(self):
        transform, col = _parse_transform("hour(ts)")
        assert col == "ts"
        assert str(transform) == "hour"

    def test_bucket(self):
        transform, col = _parse_transform("bucket(16, user_id)")
        assert col == "user_id"
        assert "bucket" in str(transform)
        assert "16" in str(transform)

    def test_truncate(self):
        transform, col = _parse_transform("truncate(10, zipcode)")
        assert col == "zipcode"
        assert "truncate" in str(transform)
        assert "10" in str(transform)

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid partition transform"):
            _parse_transform("not_a_transform")

    def test_unknown_transform(self):
        with pytest.raises(ValueError, match="Unknown partition transform"):
            _parse_transform("foobar(col)")

    def test_bucket_wrong_args(self):
        with pytest.raises(ValueError, match="bucket transform requires 2 args"):
            _parse_transform("bucket(16)")

    def test_truncate_wrong_args(self):
        with pytest.raises(ValueError, match="truncate transform requires 2 args"):
            _parse_transform("truncate(10)")

    def test_bucket_non_integer(self):
        with pytest.raises(ValueError, match="bucket size must be an integer"):
            _parse_transform("bucket(abc, col)")

    def test_whitespace_handling(self):
        transform, col = _parse_transform("  month( event_date )  ")
        assert col == "event_date"
        assert str(transform) == "month"


class TestCreateTable:
    """Test table creation with partitioning."""

    def test_create_simple_table(self, test_catalog):
        result = create_table(test_catalog, "events", {
            "id": "long",
            "name": "string",
        })

        assert result["table"] == "default.events"
        assert result["columns"] == ["id", "name"]
        assert result["partitions"] == []
        assert "Created" in result["message"]

    def test_create_with_identity_partition(self, test_catalog):
        result = create_table(
            test_catalog, "events",
            {"id": "long", "category": "string", "value": "double"},
            partitions=["identity(category)"],
        )

        assert result["partitions"] == ["identity(category)"]

        # Verify partition spec
        info = get_partitions(test_catalog, "events")
        assert info["is_partitioned"]
        assert len(info["fields"]) == 1
        assert info["fields"][0]["source_column"] == "category"
        assert info["fields"][0]["transform"] == "identity"

    def test_create_with_month_partition(self, test_catalog):
        result = create_table(
            test_catalog, "events",
            {"id": "long", "event_date": "date", "value": "double"},
            partitions=["month(event_date)"],
        )

        assert result["partitions"] == ["month(event_date)"]

        info = get_partitions(test_catalog, "events")
        assert info["is_partitioned"]
        assert info["fields"][0]["transform"] == "month"

    def test_create_with_year_partition(self, test_catalog):
        create_table(
            test_catalog, "events",
            {"id": "long", "event_date": "date"},
            partitions=["year(event_date)"],
        )

        info = get_partitions(test_catalog, "events")
        assert info["fields"][0]["transform"] == "year"

    def test_create_with_bucket_partition(self, test_catalog):
        create_table(
            test_catalog, "events",
            {"id": "long", "name": "string"},
            partitions=["bucket(16, id)"],
        )

        info = get_partitions(test_catalog, "events")
        assert info["is_partitioned"]
        assert "bucket" in info["fields"][0]["transform"]

    def test_create_with_truncate_partition(self, test_catalog):
        create_table(
            test_catalog, "events",
            {"id": "long", "zipcode": "string"},
            partitions=["truncate(5, zipcode)"],
        )

        info = get_partitions(test_catalog, "events")
        assert info["is_partitioned"]
        assert "truncate" in info["fields"][0]["transform"]

    def test_create_with_multiple_partitions(self, test_catalog):
        result = create_table(
            test_catalog, "events",
            {"id": "long", "event_date": "date", "category": "string", "value": "double"},
            partitions=["month(event_date)", "identity(category)"],
        )

        assert len(result["partitions"]) == 2

        info = get_partitions(test_catalog, "events")
        assert len(info["fields"]) == 2
        assert info["fields"][0]["transform"] == "month"
        assert info["fields"][1]["transform"] == "identity"

    def test_create_with_namespace(self, test_catalog):
        result = create_table(test_catalog, "default.events", {"id": "long"})
        assert result["table"] == "default.events"

    def test_create_empty_columns_raises(self, test_catalog):
        with pytest.raises(ValueError, match="columns must not be empty"):
            create_table(test_catalog, "events", {})

    def test_create_invalid_type_raises(self, test_catalog):
        with pytest.raises(ValueError, match="Unsupported column type"):
            create_table(test_catalog, "events", {"id": "bigint"})

    def test_create_partition_unknown_column_raises(self, test_catalog):
        with pytest.raises(ValueError, match="not found in table columns"):
            create_table(
                test_catalog, "events",
                {"id": "long"},
                partitions=["month(nonexistent)"],
            )

    def test_create_duplicate_table_raises(self, test_catalog):
        create_table(test_catalog, "events", {"id": "long"})
        with pytest.raises(ValueError, match="already exists"):
            create_table(test_catalog, "events", {"id": "long"})

    def test_create_all_column_types(self, test_catalog):
        result = create_table(test_catalog, "events", {
            "id": "long",
            "name": "string",
            "score": "double",
            "event_date": "date",
            "created_at": "timestamp",
            "active": "boolean",
        })
        assert len(result["columns"]) == 6


class TestInsertIntoPartitionedTable:
    """Test inserting data into partitioned tables."""

    def test_insert_into_identity_partitioned(self, test_catalog):
        create_table(
            test_catalog, "events",
            {"id": "long", "category": "string", "value": "double"},
            partitions=["identity(category)"],
        )

        insert_rows(test_catalog, "events", [
            {"id": 1, "category": "A", "value": 10.0},
            {"id": 2, "category": "B", "value": 20.0},
            {"id": 3, "category": "A", "value": 30.0},
        ])

        table = test_catalog.load_table("default.events")
        data = table.scan().to_arrow()
        assert data.num_rows == 3

    def test_insert_into_month_partitioned(self, test_catalog):
        create_table(
            test_catalog, "events",
            {"id": "long", "event_date": "date", "value": "double"},
            partitions=["month(event_date)"],
        )

        insert_rows(test_catalog, "events", [
            {"id": 1, "event_date": "2025-01-15", "value": 10.0},
            {"id": 2, "event_date": "2025-02-20", "value": 20.0},
            {"id": 3, "event_date": "2025-01-10", "value": 30.0},
        ])

        table = test_catalog.load_table("default.events")
        data = table.scan().to_arrow()
        assert data.num_rows == 3

    def test_insert_into_bucket_partitioned(self, test_catalog):
        create_table(
            test_catalog, "events",
            {"id": "long", "name": "string"},
            partitions=["bucket(4, id)"],
        )

        insert_rows(test_catalog, "events", [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
            {"id": 3, "name": "charlie"},
        ])

        table = test_catalog.load_table("default.events")
        data = table.scan().to_arrow()
        assert data.num_rows == 3


class TestGetPartitions:
    """Test get_partitions."""

    def test_partitioned_table(self, test_catalog):
        """expenses table is partitioned by month(date) from create_sample_tables."""
        info = get_partitions(test_catalog, "expenses")

        assert info["table"] == "default.expenses"
        assert info["is_partitioned"]
        assert len(info["fields"]) == 1
        assert info["fields"][0]["source_column"] == "date"
        assert info["fields"][0]["transform"] == "month"

    def test_unpartitioned_table(self, test_catalog):
        info = get_partitions(test_catalog, "health")

        assert info["table"] == "default.health"
        assert not info["is_partitioned"]
        assert info["fields"] == []

    def test_nonexistent_table_raises(self, test_catalog):
        with pytest.raises(ValueError, match="not found"):
            get_partitions(test_catalog, "nonexistent")

    def test_with_namespace(self, test_catalog):
        info = get_partitions(test_catalog, "default.expenses")
        assert info["table"] == "default.expenses"

    def test_custom_partitioned_table(self, test_catalog):
        create_table(
            test_catalog, "events",
            {"id": "long", "event_date": "date", "category": "string"},
            partitions=["month(event_date)", "identity(category)"],
        )

        info = get_partitions(test_catalog, "events")
        assert len(info["fields"]) == 2
        assert info["fields"][0]["source_column"] == "event_date"
        assert info["fields"][1]["source_column"] == "category"


class TestGetPartitionStats:
    """Test get_partition_stats."""

    def test_unpartitioned_table(self, test_catalog):
        stats = get_partition_stats(test_catalog, "health")
        assert not stats["is_partitioned"]
        assert stats["message"] == "Table is not partitioned"

    def test_partitioned_empty_table(self, test_catalog):
        stats = get_partition_stats(test_catalog, "expenses")
        assert stats["is_partitioned"]
        assert stats["partitions"] == []

    def test_partitioned_with_data(self, test_catalog):
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "date": "2025-01-15", "amount": 10.0},
            {"id": 2, "date": "2025-02-20", "amount": 20.0},
        ])

        stats = get_partition_stats(test_catalog, "expenses")
        assert stats["is_partitioned"]
        assert stats["total_partitions"] >= 1
        assert len(stats["partitions"]) >= 1
        # Each partition should have at least one file
        for p in stats["partitions"]:
            assert p["files"] >= 1
            assert p["size_bytes"] > 0

    def test_nonexistent_table_raises(self, test_catalog):
        with pytest.raises(ValueError, match="not found"):
            get_partition_stats(test_catalog, "nonexistent")

    def test_with_namespace(self, test_catalog):
        insert_rows(test_catalog, "expenses", [{"id": 1, "date": "2025-01-15", "amount": 10.0}])
        stats = get_partition_stats(test_catalog, "default.expenses")
        assert stats["table"] == "default.expenses"

    def test_identity_partition_stats(self, test_catalog):
        create_table(
            test_catalog, "events",
            {"id": "long", "category": "string", "value": "double"},
            partitions=["identity(category)"],
        )

        insert_rows(test_catalog, "events", [
            {"id": 1, "category": "A", "value": 10.0},
            {"id": 2, "category": "B", "value": 20.0},
            {"id": 3, "category": "A", "value": 30.0},
        ])

        stats = get_partition_stats(test_catalog, "events")
        assert stats["is_partitioned"]
        assert stats["total_partitions"] >= 1


class TestPartitioningWorkflow:
    """Test end-to-end partitioning workflows."""

    def test_create_insert_query_partitioned(self, test_catalog):
        """Full workflow: create partitioned table, insert data, query."""
        create_table(
            test_catalog, "events",
            {"id": "long", "event_date": "date", "category": "string", "value": "double"},
            partitions=["month(event_date)", "identity(category)"],
        )

        insert_rows(test_catalog, "events", [
            {"id": 1, "event_date": "2025-01-15", "category": "sales", "value": 100.0},
            {"id": 2, "event_date": "2025-01-20", "category": "sales", "value": 200.0},
            {"id": 3, "event_date": "2025-02-10", "category": "marketing", "value": 50.0},
        ])

        # Verify data
        table = test_catalog.load_table("default.events")
        data = table.scan().to_arrow()
        assert data.num_rows == 3

        # Verify partitions
        info = get_partitions(test_catalog, "events")
        assert len(info["fields"]) == 2

        # Verify stats
        stats = get_partition_stats(test_catalog, "events")
        assert stats["is_partitioned"]
        assert stats["total_partitions"] >= 1

    def test_create_table_then_get_schema(self, test_catalog):
        """Created table schema is accessible."""
        from lakehouse.catalog import get_table_schema

        create_table(
            test_catalog, "events",
            {"id": "long", "name": "string", "event_date": "date"},
            partitions=["month(event_date)"],
        )

        schema = get_table_schema(test_catalog, "events")
        field_names = [f["name"] for f in schema["fields"]]
        assert "id" in field_names
        assert "name" in field_names
        assert "event_date" in field_names
