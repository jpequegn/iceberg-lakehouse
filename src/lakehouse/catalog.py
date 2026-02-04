"""Iceberg catalog management using PyIceberg."""

import os
from pathlib import Path
from typing import Optional

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    LongType,
    DoubleType,
    TimestampType,
    DateType,
    NestedField,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import MonthTransform
import pyarrow as pa


DEFAULT_WAREHOUSE = Path.home() / ".lakehouse" / "warehouse"
DEFAULT_CATALOG_DB = Path.home() / ".lakehouse" / "catalog.db"


def get_catalog(
    warehouse_path: Optional[Path] = None,
    catalog_db: Optional[Path] = None,
) -> Catalog:
    """Get or create the Iceberg catalog.

    Uses SQLite-backed catalog for simplicity (no external dependencies).
    """
    warehouse = warehouse_path or DEFAULT_WAREHOUSE
    catalog_path = catalog_db or DEFAULT_CATALOG_DB

    # Ensure directories exist
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog_path.parent.mkdir(parents=True, exist_ok=True)

    catalog = SqlCatalog(
        "lakehouse",
        **{
            "uri": f"sqlite:///{catalog_path}",
            "warehouse": f"file://{warehouse}",
        }
    )

    return catalog


def init_catalog(catalog: Catalog) -> None:
    """Initialize catalog with default namespace."""
    try:
        catalog.create_namespace("default")
    except Exception:
        # Namespace may already exist
        pass


def list_tables(catalog: Catalog, namespace: str = "default") -> list[str]:
    """List all tables in the catalog."""
    tables = catalog.list_tables(namespace)
    return [f"{ns}.{name}" for ns, name in tables]


def get_table_schema(catalog: Catalog, table_name: str) -> dict:
    """Get schema information for a table."""
    if "." not in table_name:
        table_name = f"default.{table_name}"

    table = catalog.load_table(table_name)
    schema = table.schema()

    return {
        "name": table_name,
        "fields": [
            {
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
                "doc": field.doc,
            }
            for field in schema.fields
        ],
        "partition_spec": str(table.spec()),
        "snapshots": len(table.history()),
    }


def create_sample_tables(catalog: Catalog) -> None:
    """Create sample tables for demonstration."""

    # Expenses table - all fields optional to match PyArrow defaults
    expenses_schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "date", DateType(), required=False),
        NestedField(3, "category", StringType(), required=False),
        NestedField(4, "description", StringType(), required=False),
        NestedField(5, "amount", DoubleType(), required=False),
        NestedField(6, "currency", StringType(), required=False),
    )

    expenses_partition = PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            transform=MonthTransform(),
            name="date_month",
        )
    )

    try:
        catalog.create_table(
            identifier="default.expenses",
            schema=expenses_schema,
            partition_spec=expenses_partition,
        )
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise

    # Health metrics table
    health_schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "timestamp", TimestampType(), required=False),
        NestedField(3, "metric_type", StringType(), required=False),
        NestedField(4, "value", DoubleType(), required=False),
        NestedField(5, "unit", StringType(), required=False),
        NestedField(6, "source", StringType(), required=False),
    )

    try:
        catalog.create_table(
            identifier="default.health",
            schema=health_schema,
        )
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise

    # Notes/media table
    notes_schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "created_at", TimestampType(), required=False),
        NestedField(3, "title", StringType(), required=False),
        NestedField(4, "content", StringType(), required=False),
        NestedField(5, "source", StringType(), required=False),
        NestedField(6, "tags", StringType(), required=False),
    )

    try:
        catalog.create_table(
            identifier="default.notes",
            schema=notes_schema,
        )
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise


def insert_sample_data(catalog: Catalog) -> None:
    """Insert sample data into tables."""
    import datetime

    # Sample expenses
    expenses_data = pa.table({
        "id": [1, 2, 3, 4, 5],
        "date": [
            datetime.date(2025, 12, 1),
            datetime.date(2025, 12, 3),
            datetime.date(2025, 12, 5),
            datetime.date(2025, 11, 15),
            datetime.date(2025, 11, 20),
        ],
        "category": ["groceries", "transport", "entertainment", "utilities", "groceries"],
        "description": [
            "Weekly shopping",
            "Uber ride",
            "Netflix subscription",
            "Electricity bill",
            "Coffee beans",
        ],
        "amount": [85.50, 24.00, 15.99, 120.00, 35.00],
        "currency": ["USD", "USD", "USD", "USD", "USD"],
    })

    try:
        table = catalog.load_table("default.expenses")
        table.append(expenses_data)
    except Exception:
        pass  # Table might not exist or data already inserted

    # Sample health data
    health_data = pa.table({
        "id": [1, 2, 3, 4],
        "timestamp": pa.array([
            datetime.datetime(2025, 12, 9, 8, 0),
            datetime.datetime(2025, 12, 9, 12, 0),
            datetime.datetime(2025, 12, 8, 8, 0),
            datetime.datetime(2025, 12, 8, 22, 0),
        ], type=pa.timestamp("us")),
        "metric_type": ["weight", "steps", "weight", "sleep_hours"],
        "value": [75.5, 8500.0, 75.3, 7.5],
        "unit": ["kg", "count", "kg", "hours"],
        "source": ["scale", "watch", "scale", "watch"],
    })

    try:
        table = catalog.load_table("default.health")
        table.append(health_data)
    except Exception:
        pass

    # Sample notes
    notes_data = pa.table({
        "id": [1, 2],
        "created_at": pa.array([
            datetime.datetime(2025, 12, 9, 10, 0),
            datetime.datetime(2025, 12, 8, 14, 30),
        ], type=pa.timestamp("us")),
        "title": ["Research notes: Iceberg", "Meeting notes"],
        "content": [
            "Apache Iceberg provides excellent time travel capabilities...",
            "Discussed Q1 roadmap with team. Key priorities: performance, reliability.",
        ],
        "source": ["manual", "transcription"],
        "tags": ['["research", "data"]', '["work", "planning"]'],
    })

    try:
        table = catalog.load_table("default.notes")
        table.append(notes_data)
    except Exception:
        pass
