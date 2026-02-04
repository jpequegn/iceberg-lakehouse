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


def insert_rows(
    catalog: Catalog,
    table_name: str,
    rows: list[dict],
) -> int:
    """Insert rows into an Iceberg table.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        rows: List of dictionaries, each representing a row

    Returns:
        Number of rows inserted

    Raises:
        ValueError: If table doesn't exist or rows are invalid
    """
    import datetime

    if not rows:
        return 0

    # Normalize table name
    if "." not in table_name:
        table_name = f"default.{table_name}"

    # Load table and get schema
    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    schema = table.schema()

    # Build column arrays from rows
    columns: dict[str, list] = {field.name: [] for field in schema.fields}

    for row in rows:
        for field in schema.fields:
            value = row.get(field.name)
            columns[field.name].append(value)

    # Convert to PyArrow arrays with proper types
    arrow_arrays = {}
    for field in schema.fields:
        values = columns[field.name]
        field_type = str(field.field_type)

        try:
            if field_type == "long":
                # Convert to int, handling None
                converted = [int(v) if v is not None else None for v in values]
                arrow_arrays[field.name] = pa.array(converted, type=pa.int64())

            elif field_type == "double":
                # Convert to float, handling None
                converted = [float(v) if v is not None else None for v in values]
                arrow_arrays[field.name] = pa.array(converted, type=pa.float64())

            elif field_type == "string":
                # Convert to string, handling None
                converted = [str(v) if v is not None else None for v in values]
                arrow_arrays[field.name] = pa.array(converted, type=pa.string())

            elif field_type == "date":
                # Parse date strings or pass through date objects
                converted = []
                for v in values:
                    if v is None:
                        converted.append(None)
                    elif isinstance(v, datetime.date):
                        converted.append(v)
                    elif isinstance(v, str):
                        # Parse ISO format date string
                        converted.append(datetime.date.fromisoformat(v))
                    else:
                        converted.append(None)
                arrow_arrays[field.name] = pa.array(converted, type=pa.date32())

            elif field_type.startswith("timestamp"):
                # Parse timestamp strings or pass through datetime objects
                converted = []
                for v in values:
                    if v is None:
                        converted.append(None)
                    elif isinstance(v, datetime.datetime):
                        converted.append(v)
                    elif isinstance(v, str):
                        # Parse ISO format timestamp string
                        converted.append(datetime.datetime.fromisoformat(v))
                    else:
                        converted.append(None)
                arrow_arrays[field.name] = pa.array(converted, type=pa.timestamp("us"))

            else:
                # Default: try as-is
                arrow_arrays[field.name] = pa.array(values)

        except Exception as e:
            raise ValueError(f"Error converting column '{field.name}': {e}")

    # Create Arrow table and append
    arrow_table = pa.table(arrow_arrays)
    table.append(arrow_table)

    return len(rows)


def update_rows(
    catalog: Catalog,
    table_name: str,
    filter_expr: str,
    updates: dict,
) -> int:
    """Update rows in an Iceberg table matching a filter.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        filter_expr: SQL WHERE clause (e.g., "id = 5" or "category = 'groceries'")
        updates: Dictionary of column names to new values

    Returns:
        Number of rows updated

    Raises:
        ValueError: If table doesn't exist or filter/updates are invalid
    """
    import datetime
    import duckdb

    if not filter_expr:
        raise ValueError("Filter expression is required for UPDATE operations")

    if not updates:
        raise ValueError("Updates dictionary cannot be empty")

    # Normalize table name
    if "." not in table_name:
        table_name = f"default.{table_name}"

    # Load table
    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    schema = table.schema()
    field_names = {field.name for field in schema.fields}

    # Validate update columns exist
    for col in updates.keys():
        if col not in field_names:
            raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")

    # Read all data from the table
    try:
        arrow_table = table.scan().to_arrow()
    except Exception:
        # Table might be empty
        return 0

    if arrow_table.num_rows == 0:
        return 0

    # Use DuckDB to identify matching rows and apply updates
    conn = duckdb.connect(":memory:")
    conn.register("source_table", arrow_table)

    # Count matching rows first
    count_result = conn.execute(f"SELECT COUNT(*) FROM source_table WHERE {filter_expr}").fetchone()
    match_count = count_result[0] if count_result else 0

    if match_count == 0:
        conn.close()
        return 0

    # Build the UPDATE-like SELECT query
    # We select all columns, replacing updated ones with new values for matching rows
    select_parts = []
    for field in schema.fields:
        col_name = field.name
        if col_name in updates:
            new_value = updates[col_name]
            # Format value for SQL
            if new_value is None:
                formatted_value = "NULL"
            elif isinstance(new_value, str):
                # Escape single quotes
                escaped = new_value.replace("'", "''")
                formatted_value = f"'{escaped}'"
            elif isinstance(new_value, (datetime.date, datetime.datetime)):
                formatted_value = f"'{new_value.isoformat()}'"
            else:
                formatted_value = str(new_value)

            # Use CASE to conditionally update
            select_parts.append(
                f"CASE WHEN {filter_expr} THEN {formatted_value} ELSE \"{col_name}\" END AS \"{col_name}\""
            )
        else:
            select_parts.append(f"\"{col_name}\"")

    select_sql = f"SELECT {', '.join(select_parts)} FROM source_table"

    # Execute and get updated table
    updated_arrow = conn.execute(select_sql).fetch_arrow_table()
    conn.close()

    # Overwrite the table with updated data
    table.overwrite(updated_arrow)

    return match_count


def delete_rows(
    catalog: Catalog,
    table_name: str,
    filter_expr: str,
) -> int:
    """Delete rows from an Iceberg table matching a filter.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        filter_expr: SQL WHERE clause (e.g., "id = 5" or "category = 'groceries'")

    Returns:
        Number of rows deleted

    Raises:
        ValueError: If table doesn't exist or filter is invalid
    """
    import duckdb

    if not filter_expr:
        raise ValueError("Filter expression is required for DELETE operations")

    # Normalize table name
    if "." not in table_name:
        table_name = f"default.{table_name}"

    # Load table
    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    # Read all data from the table
    try:
        arrow_table = table.scan().to_arrow()
    except Exception:
        # Table might be empty
        return 0

    if arrow_table.num_rows == 0:
        return 0

    # Use DuckDB to filter out matching rows
    conn = duckdb.connect(":memory:")
    conn.register("source_table", arrow_table)

    # Count matching rows first
    count_result = conn.execute(f"SELECT COUNT(*) FROM source_table WHERE {filter_expr}").fetchone()
    match_count = count_result[0] if count_result else 0

    if match_count == 0:
        conn.close()
        return 0

    # Select rows that DON'T match the filter (i.e., keep these)
    remaining_arrow = conn.execute(f"SELECT * FROM source_table WHERE NOT ({filter_expr})").fetch_arrow_table()
    conn.close()

    # Overwrite the table with remaining data
    table.overwrite(remaining_arrow)

    return match_count


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
