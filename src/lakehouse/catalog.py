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


def get_snapshots(catalog: Catalog, table_name: str) -> list[dict]:
    """List all snapshots for a table.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)

    Returns:
        List of snapshot dicts with id, timestamp, summary
    """
    import datetime

    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    snapshots = []
    for snapshot in table.snapshots():
        ts = datetime.datetime.fromtimestamp(
            snapshot.timestamp_ms / 1000, tz=datetime.timezone.utc
        )
        snapshots.append({
            "snapshot_id": snapshot.snapshot_id,
            "timestamp": ts.isoformat(),
            "parent_id": snapshot.parent_snapshot_id,
            "operation": snapshot.summary.operation.value if snapshot.summary else None,
            "summary": {k: v for k, v in (snapshot.summary.additional_properties.items() if snapshot.summary else [])},
        })

    return snapshots


def scan_as_of(
    catalog: Catalog,
    table_name: str,
    as_of: str,
) -> "pa.Table":
    """Scan a table at a specific point in time or snapshot.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        as_of: Either an ISO timestamp string or a snapshot ID (integer string)

    Returns:
        PyArrow table with data at that point in time
    """
    import datetime

    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    # Try to parse as snapshot ID first (pure integer)
    try:
        snapshot_id = int(as_of)
        snapshot = table.snapshot_by_id(snapshot_id)
        if snapshot is None:
            raise ValueError(f"Snapshot ID {snapshot_id} not found in table '{table_name}'")
        return table.scan(snapshot_id=snapshot_id).to_arrow()
    except ValueError as ve:
        if "not found" in str(ve):
            raise
        pass

    # Parse as ISO timestamp
    try:
        ts = datetime.datetime.fromisoformat(as_of)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        timestamp_ms = int(ts.timestamp() * 1000)
        snapshot = table.snapshot_as_of_timestamp(timestamp_ms)
        if snapshot is None:
            raise ValueError(f"No snapshot found at or before {as_of} in table '{table_name}'")
        return table.scan(snapshot_id=snapshot.snapshot_id).to_arrow()
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid as_of value '{as_of}': must be ISO timestamp or snapshot ID. {e}")


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


def rollback_table(
    catalog: Catalog,
    table_name: str,
    snapshot_id: int | None = None,
    timestamp: str | None = None,
) -> dict:
    """Rollback a table to a previous snapshot.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        snapshot_id: Snapshot ID to rollback to
        timestamp: ISO timestamp to rollback to (finds nearest snapshot at or before)

    Returns:
        Dict with rollback details

    Raises:
        ValueError: If neither snapshot_id nor timestamp provided, or snapshot not found
    """
    import datetime

    if not snapshot_id and not timestamp:
        raise ValueError("Either snapshot_id or timestamp must be provided")

    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    current = table.current_snapshot()
    if current is None:
        raise ValueError(f"Table '{table_name}' has no snapshots")

    # Resolve target snapshot
    if snapshot_id:
        target = table.snapshot_by_id(snapshot_id)
        if target is None:
            raise ValueError(f"Snapshot ID {snapshot_id} not found in table '{table_name}'")
    else:
        ts = datetime.datetime.fromisoformat(timestamp)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        timestamp_ms = int(ts.timestamp() * 1000)
        target = table.snapshot_as_of_timestamp(timestamp_ms)
        if target is None:
            raise ValueError(f"No snapshot found at or before {timestamp} in table '{table_name}'")

    if target.snapshot_id == current.snapshot_id:
        return {
            "snapshot_id": target.snapshot_id,
            "message": "Already at this snapshot, no rollback needed",
        }

    # Read data from the target snapshot and overwrite
    arrow_data = table.scan(snapshot_id=target.snapshot_id).to_arrow()
    table.overwrite(arrow_data)

    target_ts = datetime.datetime.fromtimestamp(
        target.timestamp_ms / 1000, tz=datetime.timezone.utc
    ).isoformat()

    return {
        "snapshot_id": target.snapshot_id,
        "timestamp": target_ts,
        "message": f"Rolled back to snapshot {target.snapshot_id} ({target_ts})",
    }


def expire_snapshots(
    catalog: Catalog,
    table_name: str,
    older_than: str | None = None,
    retain_last: int | None = None,
) -> dict:
    """Expire old snapshots from a table.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        older_than: ISO timestamp or duration string (e.g., '30d') — expire snapshots older than this
        retain_last: Minimum number of recent snapshots to keep

    Returns:
        Dict with expiration details
    """
    import datetime
    import re

    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    before_count = len(list(table.snapshots()))

    if not older_than and not retain_last:
        raise ValueError("Either older_than or retain_last must be provided")

    # If retain_last is set, compute the cutoff from snapshot timestamps
    # PyIceberg's ExpireSnapshots supports .older_than(datetime)
    expire_op = table.maintenance.expire_snapshots()

    if older_than:
        # Try duration format (e.g., '30d', '7d', '24h')
        duration_match = re.match(r'^(\d+)([dhm])$', older_than.strip())
        if duration_match:
            amount = int(duration_match.group(1))
            unit = duration_match.group(2)
            if unit == 'd':
                cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=amount)
            elif unit == 'h':
                cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=amount)
            elif unit == 'm':
                cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=amount)
            else:
                raise ValueError(f"Unsupported duration unit: {unit}")
        else:
            # Try ISO timestamp
            cutoff = datetime.datetime.fromisoformat(older_than)
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=datetime.timezone.utc)

        expire_op = expire_op.older_than(cutoff)

    if retain_last:
        # Manually determine which snapshots to keep
        # Get all snapshots sorted by timestamp descending
        all_snapshots = sorted(table.snapshots(), key=lambda s: s.timestamp_ms, reverse=True)
        keep_ids = {s.snapshot_id for s in all_snapshots[:retain_last]}

        # Expire specific IDs that are both older than cutoff and not in keep set
        for snapshot in all_snapshots:
            if snapshot.snapshot_id not in keep_ids:
                expire_op = expire_op.by_id(snapshot.snapshot_id)

        # If we only have retain_last without older_than, just commit what we have
        if not older_than:
            expire_op.commit()
            after_count = len(list(catalog.load_table(table_name).snapshots()))
            return {
                "expired": before_count - after_count,
                "remaining": after_count,
                "message": f"Expired {before_count - after_count} snapshot(s), retained last {retain_last}",
            }

    expire_op.commit()

    after_count = len(list(catalog.load_table(table_name).snapshots()))
    return {
        "expired": before_count - after_count,
        "remaining": after_count,
        "message": f"Expired {before_count - after_count} snapshot(s), {after_count} remaining",
    }


def execute_batch(
    catalog: Catalog,
    operations: list[dict],
) -> list[dict]:
    """Execute multiple write operations as a batch.

    Operations run sequentially. If any operation fails, previously completed
    operations are NOT rolled back (Iceberg doesn't support cross-table
    transactions). The result includes status for each operation.

    Args:
        catalog: The Iceberg catalog
        operations: List of operation dicts, each with:
            - action: 'insert', 'update', or 'delete'
            - table_name: target table
            - For insert: 'rows' (list of dicts)
            - For update: 'filter' (str) and 'updates' (dict)
            - For delete: 'filter' (str)

    Returns:
        List of result dicts with status for each operation
    """
    if not operations:
        raise ValueError("Operations list must not be empty")

    results = []
    for i, op in enumerate(operations):
        action = op.get("action")
        table_name = op.get("table_name")

        if not action:
            results.append({"index": i, "status": "error", "message": "Missing 'action'"})
            continue
        if not table_name:
            results.append({"index": i, "status": "error", "message": "Missing 'table_name'"})
            continue

        try:
            if action == "insert":
                rows = op.get("rows")
                if not rows:
                    results.append({"index": i, "status": "error", "message": "Missing 'rows' for insert"})
                    continue
                count = insert_rows(catalog, table_name, rows)
                results.append({
                    "index": i, "status": "ok", "action": "insert",
                    "table": table_name, "rows_affected": count,
                })

            elif action == "update":
                filter_expr = op.get("filter")
                updates = op.get("updates")
                if not filter_expr or not updates:
                    results.append({"index": i, "status": "error", "message": "Missing 'filter' or 'updates' for update"})
                    continue
                count = update_rows(catalog, table_name, filter_expr, updates)
                results.append({
                    "index": i, "status": "ok", "action": "update",
                    "table": table_name, "rows_affected": count,
                })

            elif action == "delete":
                filter_expr = op.get("filter")
                if not filter_expr:
                    results.append({"index": i, "status": "error", "message": "Missing 'filter' for delete"})
                    continue
                count = delete_rows(catalog, table_name, filter_expr)
                results.append({
                    "index": i, "status": "ok", "action": "delete",
                    "table": table_name, "rows_affected": count,
                })

            else:
                results.append({"index": i, "status": "error", "message": f"Unknown action '{action}'"})

        except Exception as e:
            results.append({
                "index": i, "status": "error", "action": action,
                "table": table_name, "message": str(e),
            })
            # Stop on first failure for safety
            for j in range(i + 1, len(operations)):
                results.append({"index": j, "status": "skipped", "message": "Skipped due to earlier failure"})
            break

    return results


TYPE_MAP = {
    "string": StringType,
    "long": LongType,
    "int": LongType,
    "integer": LongType,
    "double": DoubleType,
    "float": DoubleType,
    "timestamp": TimestampType,
    "date": DateType,
}


def alter_table(
    catalog: Catalog,
    table_name: str,
    operation: str,
    column_name: str,
    new_name: str | None = None,
    column_type: str | None = None,
) -> str:
    """Alter a table's schema.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        operation: One of 'add_column', 'drop_column', 'rename_column'
        column_name: Target column name
        new_name: New column name (for rename_column)
        column_type: Column type string (for add_column)

    Returns:
        Description of the change made

    Raises:
        ValueError: If parameters are invalid
    """
    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    if operation == "add_column":
        if not column_type:
            raise ValueError("column_type is required for add_column")

        type_key = column_type.lower().strip()
        if type_key not in TYPE_MAP:
            raise ValueError(
                f"Unsupported column type '{column_type}'. "
                f"Supported types: {', '.join(sorted(TYPE_MAP.keys()))}"
            )

        iceberg_type = TYPE_MAP[type_key]()

        with table.update_schema() as update:
            update.add_column(column_name, iceberg_type)

        return f"Added column '{column_name}' ({column_type}) to {table_name}"

    elif operation == "drop_column":
        with table.update_schema() as update:
            update.delete_column(column_name)

        return f"Dropped column '{column_name}' from {table_name}"

    elif operation == "rename_column":
        if not new_name:
            raise ValueError("new_name is required for rename_column")

        with table.update_schema() as update:
            update.rename_column(column_name, new_name)

        return f"Renamed column '{column_name}' to '{new_name}' in {table_name}"

    else:
        raise ValueError(
            f"Unknown operation '{operation}'. "
            f"Supported: add_column, drop_column, rename_column"
        )


def upsert_rows(
    catalog: Catalog,
    table_name: str,
    key_columns: list[str],
    rows: list[dict],
) -> dict[str, int]:
    """Upsert rows into an Iceberg table (insert or update on key match).

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        key_columns: Columns to match on for determining insert vs update
        rows: List of dictionaries, each representing a row

    Returns:
        Dict with 'inserted' and 'updated' counts

    Raises:
        ValueError: If table doesn't exist, key columns are invalid, or rows are empty
    """
    import datetime
    import duckdb

    if not rows:
        return {"inserted": 0, "updated": 0}

    if not key_columns:
        raise ValueError("key_columns must not be empty")

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

    # Validate key columns exist
    for col in key_columns:
        if col not in field_names:
            raise ValueError(f"Key column '{col}' does not exist in table '{table_name}'")

    # Build Arrow table from incoming rows using the same logic as insert_rows
    columns: dict[str, list] = {field.name: [] for field in schema.fields}
    for row in rows:
        for field in schema.fields:
            columns[field.name].append(row.get(field.name))

    arrow_arrays = {}
    for field in schema.fields:
        values = columns[field.name]
        field_type = str(field.field_type)
        try:
            if field_type == "long":
                converted = [int(v) if v is not None else None for v in values]
                arrow_arrays[field.name] = pa.array(converted, type=pa.int64())
            elif field_type == "double":
                converted = [float(v) if v is not None else None for v in values]
                arrow_arrays[field.name] = pa.array(converted, type=pa.float64())
            elif field_type == "string":
                converted = [str(v) if v is not None else None for v in values]
                arrow_arrays[field.name] = pa.array(converted, type=pa.string())
            elif field_type == "date":
                converted = []
                for v in values:
                    if v is None:
                        converted.append(None)
                    elif isinstance(v, datetime.date):
                        converted.append(v)
                    elif isinstance(v, str):
                        converted.append(datetime.date.fromisoformat(v))
                    else:
                        converted.append(None)
                arrow_arrays[field.name] = pa.array(converted, type=pa.date32())
            elif field_type.startswith("timestamp"):
                converted = []
                for v in values:
                    if v is None:
                        converted.append(None)
                    elif isinstance(v, datetime.datetime):
                        converted.append(v)
                    elif isinstance(v, str):
                        converted.append(datetime.datetime.fromisoformat(v))
                    else:
                        converted.append(None)
                arrow_arrays[field.name] = pa.array(converted, type=pa.timestamp("us"))
            else:
                arrow_arrays[field.name] = pa.array(values)
        except Exception as e:
            raise ValueError(f"Error converting column '{field.name}': {e}")

    new_arrow = pa.table(arrow_arrays)

    # Read existing data
    try:
        existing_arrow = table.scan().to_arrow()
    except Exception:
        existing_arrow = None

    if existing_arrow is None or existing_arrow.num_rows == 0:
        # No existing data - just insert everything
        table.append(new_arrow)
        return {"inserted": len(rows), "updated": 0}

    # Use DuckDB to merge
    conn = duckdb.connect(":memory:")
    conn.register("existing", existing_arrow)
    conn.register("incoming", new_arrow)

    # Build join condition
    join_cond = " AND ".join(
        f'existing."{col}" = incoming."{col}"' for col in key_columns
    )

    # Count how many incoming rows match existing rows
    count_sql = f'SELECT COUNT(*) FROM incoming JOIN existing ON {join_cond}'
    updated_count = conn.execute(count_sql).fetchone()[0]
    inserted_count = len(rows) - updated_count

    # Build merged result:
    # 1. For matched rows: take incoming values (update)
    # 2. For unmatched existing rows: keep as-is
    # 3. For unmatched incoming rows: insert
    col_names = [f'"{field.name}"' for field in schema.fields]
    col_list = ", ".join(col_names)

    # Unmatched existing rows (not in incoming)
    incoming_key_null_checks = " AND ".join(
        f'incoming."{col}" IS NULL' for col in key_columns
    )
    unmatched_existing_sql = (
        f"SELECT {', '.join(f'existing.{c}' for c in col_names)} "
        f"FROM existing LEFT JOIN incoming ON {join_cond} "
        f"WHERE {incoming_key_null_checks}"
    )

    # All incoming rows (they either update or insert)
    incoming_cols_sql = ", ".join(f'incoming.{c}' for c in col_names)

    merged_sql = f"{unmatched_existing_sql} UNION ALL SELECT {incoming_cols_sql} FROM incoming"

    merged_arrow = conn.execute(merged_sql).fetch_arrow_table()
    conn.close()

    # Overwrite the table with merged data
    table.overwrite(merged_arrow)

    return {"inserted": inserted_count, "updated": updated_count}


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
