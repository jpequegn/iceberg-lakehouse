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

    all_snapshots = sorted(table.snapshots(), key=lambda s: s.timestamp_ms, reverse=True)
    current = table.current_snapshot()

    # Determine which snapshot IDs to expire
    ids_to_expire = set()

    if older_than:
        # Parse cutoff timestamp
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
            cutoff = datetime.datetime.fromisoformat(older_than)
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=datetime.timezone.utc)

        cutoff_ms = int(cutoff.timestamp() * 1000)
        ids_to_expire = {s.snapshot_id for s in all_snapshots if s.timestamp_ms < cutoff_ms}

    if retain_last:
        keep_ids = {s.snapshot_id for s in all_snapshots[:retain_last]}
        if current:
            keep_ids.add(current.snapshot_id)
        retain_expire = {s.snapshot_id for s in all_snapshots if s.snapshot_id not in keep_ids}
        ids_to_expire = ids_to_expire | retain_expire if older_than else retain_expire

    # Never expire the current snapshot
    if current:
        ids_to_expire.discard(current.snapshot_id)

    if not ids_to_expire:
        msg = f"No snapshots to expire (retaining last {retain_last})" if retain_last else "No snapshots to expire"
        return {"expired": 0, "remaining": before_count, "message": msg}

    expire_op = table.maintenance.expire_snapshots()
    for sid in ids_to_expire:
        expire_op = expire_op.by_id(sid)
    expire_op.commit()

    after_count = len(list(catalog.load_table(table_name).snapshots()))
    expired = before_count - after_count
    if retain_last:
        msg = f"Expired {expired} snapshot(s), retained last {retain_last}"
    else:
        msg = f"Expired {expired} snapshot(s), {after_count} remaining"
    return {"expired": expired, "remaining": after_count, "message": msg}


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


def get_table_property(
    catalog: Catalog,
    table_name: str,
    key: str,
) -> str | None:
    """Get a property from an Iceberg table.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        key: Property key

    Returns:
        Property value or None if not set
    """
    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    return table.properties.get(key)


def set_table_property(
    catalog: Catalog,
    table_name: str,
    key: str,
    value: str,
) -> str:
    """Set a property on an Iceberg table.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        key: Property key
        value: Property value

    Returns:
        Description of the change made
    """
    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    with table.transaction() as tx:
        tx.set_properties({key: value})

    return f"Set '{key}' = '{value}' on {table_name}"


def remove_table_property(
    catalog: Catalog,
    table_name: str,
    key: str,
) -> str:
    """Remove a property from an Iceberg table.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        key: Property key to remove

    Returns:
        Description of the change made
    """
    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    if key not in table.properties:
        raise ValueError(f"Property '{key}' not found on {table_name}")

    with table.transaction() as tx:
        tx.remove_properties(key)

    return f"Removed '{key}' from {table_name}"


def import_file(
    catalog: Catalog,
    file_path: str | Path,
    table_name: str,
    *,
    file_format: str | None = None,
    if_exists: str = "fail",
    delimiter: str = ",",
    has_header: bool = True,
) -> dict:
    """Import data from a CSV or JSON file into an Iceberg table.

    Args:
        catalog: The Iceberg catalog
        file_path: Path to the file to import
        table_name: Target table name (with or without namespace)
        file_format: File format override ('csv', 'json', 'ndjson'). Auto-detected from extension if None.
        if_exists: What to do if the table already exists: 'fail', 'append', 'replace'
        delimiter: CSV delimiter character (default: ',')
        has_header: Whether CSV has a header row (default: True)

    Returns:
        Dict with import details: table, rows_imported, format

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If format is unsupported, table exists and if_exists='fail',
                     or schema is incompatible on append
    """
    import pyarrow.csv as pa_csv
    import pyarrow.json as pa_json

    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Auto-detect format from extension
    if file_format is None:
        ext = file_path.suffix.lower()
        format_map = {
            ".csv": "csv",
            ".tsv": "csv",
            ".json": "json",
            ".ndjson": "ndjson",
            ".jsonl": "ndjson",
        }
        file_format = format_map.get(ext)
        if file_format is None:
            raise ValueError(
                f"Cannot auto-detect format for extension '{ext}'. "
                f"Use --format to specify (csv, json, ndjson)."
            )
        if ext == ".tsv":
            delimiter = "\t"

    # Read the file into a PyArrow table
    if file_format == "csv":
        read_options = pa_csv.ReadOptions(autogenerate_column_names=not has_header)
        parse_options = pa_csv.ParseOptions(delimiter=delimiter)
        arrow_table = pa_csv.read_csv(
            str(file_path),
            read_options=read_options,
            parse_options=parse_options,
        )
    elif file_format == "json":
        # PyArrow read_json expects NDJSON, so handle JSON arrays ourselves
        import json as json_mod
        import tempfile

        raw = file_path.read_text()
        parsed = json_mod.loads(raw)
        if isinstance(parsed, list):
            # JSON array — convert to NDJSON in a temp file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as tmp:
                for obj in parsed:
                    tmp.write(json_mod.dumps(obj) + "\n")
                tmp_name = tmp.name
            try:
                arrow_table = pa_json.read_json(tmp_name)
            finally:
                Path(tmp_name).unlink(missing_ok=True)
        else:
            raise ValueError("JSON file must contain an array of objects.")
    elif file_format == "ndjson":
        arrow_table = pa_json.read_json(str(file_path))
    else:
        raise ValueError(
            f"Unsupported format '{file_format}'. Supported: csv, json, ndjson."
        )

    if arrow_table.num_rows == 0:
        raise ValueError("File contains no data rows.")

    # Normalize table name
    if "." not in table_name:
        table_name = f"default.{table_name}"

    # Check if table exists
    table_exists = True
    try:
        table = catalog.load_table(table_name)
    except Exception:
        table_exists = False

    if table_exists:
        if if_exists == "fail":
            raise ValueError(
                f"Table '{table_name}' already exists. "
                f"Use --if-exists append or --if-exists replace."
            )
        elif if_exists == "replace":
            table.overwrite(arrow_table)
        elif if_exists == "append":
            # Validate schema compatibility: imported columns must match table columns
            table_schema = table.schema()
            table_field_names = {f.name for f in table_schema.fields}
            import_field_names = set(arrow_table.column_names)

            missing_in_import = table_field_names - import_field_names
            extra_in_import = import_field_names - table_field_names

            if extra_in_import:
                raise ValueError(
                    f"Import file has columns not in table: {sorted(extra_in_import)}. "
                    f"Table columns: {sorted(table_field_names)}"
                )

            # Reorder and cast columns to match table schema
            cast_arrays = {}
            for field in table_schema.fields:
                if field.name in import_field_names:
                    col = arrow_table.column(field.name)
                    # Cast to the table's expected Arrow type
                    target_type = _iceberg_type_to_arrow(str(field.field_type))
                    if target_type and col.type != target_type:
                        col = col.cast(target_type)
                    cast_arrays[field.name] = col
                else:
                    # Missing column — fill with nulls
                    target_type = _iceberg_type_to_arrow(str(field.field_type)) or pa.string()
                    cast_arrays[field.name] = pa.array(
                        [None] * arrow_table.num_rows, type=target_type
                    )

            arrow_table = pa.table(cast_arrays)
            table.append(arrow_table)
        else:
            raise ValueError(f"Invalid if_exists value: '{if_exists}'. Use 'fail', 'append', or 'replace'.")
    else:
        # Create a new table from the imported schema
        iceberg_schema = _arrow_schema_to_iceberg(arrow_table.schema)
        catalog.create_table(identifier=table_name, schema=iceberg_schema)
        table = catalog.load_table(table_name)

        # Cast columns to match the Iceberg schema types
        cast_arrays = {}
        for field in table.schema().fields:
            col = arrow_table.column(field.name)
            target_type = _iceberg_type_to_arrow(str(field.field_type))
            if target_type and col.type != target_type:
                col = col.cast(target_type)
            cast_arrays[field.name] = col

        arrow_table = pa.table(cast_arrays)
        table.append(arrow_table)

    return {
        "table": table_name,
        "rows_imported": arrow_table.num_rows,
        "format": file_format,
    }


def _iceberg_type_to_arrow(type_str: str):
    """Convert an Iceberg type string to a PyArrow type."""
    mapping = {
        "long": pa.int64(),
        "double": pa.float64(),
        "string": pa.string(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "boolean": pa.bool_(),
        "int": pa.int32(),
        "float": pa.float32(),
    }
    return mapping.get(type_str)


def _arrow_schema_to_iceberg(arrow_schema) -> Schema:
    """Convert a PyArrow schema to an Iceberg Schema."""
    from pyiceberg.types import BooleanType

    arrow_to_iceberg = {
        pa.int8(): LongType,
        pa.int16(): LongType,
        pa.int32(): LongType,
        pa.int64(): LongType,
        pa.float16(): DoubleType,
        pa.float32(): DoubleType,
        pa.float64(): DoubleType,
        pa.string(): StringType,
        pa.large_string(): StringType,
        pa.utf8(): StringType,
        pa.large_utf8(): StringType,
        pa.bool_(): BooleanType,
        pa.date32(): DateType,
        pa.date64(): DateType,
    }

    fields = []
    for i, arrow_field in enumerate(arrow_schema):
        iceberg_type_cls = None

        # Direct match
        if arrow_field.type in arrow_to_iceberg:
            iceberg_type_cls = arrow_to_iceberg[arrow_field.type]
        # Timestamp types
        elif pa.types.is_timestamp(arrow_field.type):
            iceberg_type_cls = TimestampType
        # Default to string
        else:
            iceberg_type_cls = StringType

        fields.append(
            NestedField(
                field_id=i + 1,
                name=arrow_field.name,
                field_type=iceberg_type_cls(),
                required=False,
            )
        )

    return Schema(*fields)


def export_table(
    catalog: Catalog,
    table_name: str,
    output_path: str | Path | None = None,
    *,
    file_format: str | None = None,
    where: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> dict:
    """Export an Iceberg table to CSV, JSON, NDJSON, or Parquet.

    Args:
        catalog: The Iceberg catalog
        table_name: Source table name (with or without namespace)
        output_path: Output file path (default: <table>.<format>)
        file_format: Output format ('csv', 'json', 'ndjson', 'parquet').
                     Auto-detected from output_path extension if None.
        where: SQL WHERE clause for filtering rows
        columns: List of column names to include
        limit: Maximum number of rows to export

    Returns:
        Dict with export details: table, output, rows_exported, format

    Raises:
        ValueError: If table not found, format unknown, or columns invalid
    """
    import duckdb
    import pyarrow.csv as pa_csv
    import pyarrow.parquet as pq
    import json as json_mod

    # Normalize table name
    if "." not in table_name:
        table_name = f"default.{table_name}"

    short_name = table_name.split(".")[-1]

    # Load table
    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    # Resolve format
    if file_format is None and output_path is not None:
        ext = Path(output_path).suffix.lower()
        ext_map = {
            ".csv": "csv",
            ".tsv": "csv",
            ".json": "json",
            ".ndjson": "ndjson",
            ".jsonl": "ndjson",
            ".parquet": "parquet",
        }
        file_format = ext_map.get(ext)

    if file_format is None:
        file_format = "csv"

    if file_format not in ("csv", "json", "ndjson", "parquet"):
        raise ValueError(
            f"Unsupported format '{file_format}'. "
            f"Supported: csv, json, ndjson, parquet."
        )

    # Default output path
    if output_path is None:
        ext_for_format = {"csv": ".csv", "json": ".json", "ndjson": ".ndjson", "parquet": ".parquet"}
        output_path = Path(f"{short_name}{ext_for_format[file_format]}")
    else:
        output_path = Path(output_path)

    # Read table data
    try:
        arrow_table = table.scan().to_arrow()
    except Exception:
        arrow_table = pa.table({f.name: pa.array([], type=_iceberg_type_to_arrow(str(f.field_type)) or pa.string()) for f in table.schema().fields})

    # Apply WHERE filter via DuckDB
    if where:
        conn = duckdb.connect(":memory:")
        conn.register("source", arrow_table)
        arrow_table = conn.execute(f"SELECT * FROM source WHERE {where}").fetch_arrow_table()
        conn.close()

    # Apply column selection
    if columns:
        available = set(arrow_table.column_names)
        invalid = [c for c in columns if c not in available]
        if invalid:
            raise ValueError(
                f"Columns not found in table: {invalid}. "
                f"Available: {sorted(available)}"
            )
        arrow_table = arrow_table.select(columns)

    # Apply row limit
    if limit is not None and limit < arrow_table.num_rows:
        arrow_table = arrow_table.slice(0, limit)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write output
    rows_exported = arrow_table.num_rows

    if file_format == "csv":
        pa_csv.write_csv(arrow_table, str(output_path))
    elif file_format == "json":
        # Write as JSON array
        rows = arrow_table.to_pydict()
        records = []
        for i in range(arrow_table.num_rows):
            record = {}
            for col_name in arrow_table.column_names:
                val = rows[col_name][i]
                # Convert non-JSON-serializable types
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                record[col_name] = val
            records.append(record)
        output_path.write_text(json_mod.dumps(records, indent=2, default=str))
    elif file_format == "ndjson":
        rows = arrow_table.to_pydict()
        lines = []
        for i in range(arrow_table.num_rows):
            record = {}
            for col_name in arrow_table.column_names:
                val = rows[col_name][i]
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                record[col_name] = val
            lines.append(json_mod.dumps(record, default=str))
        output_path.write_text("\n".join(lines) + "\n")
    elif file_format == "parquet":
        pq.write_table(arrow_table, str(output_path))

    return {
        "table": table_name,
        "output": str(output_path),
        "rows_exported": rows_exported,
        "format": file_format,
        "size_bytes": output_path.stat().st_size,
    }


def profile_table(
    catalog: Catalog,
    table_name: str,
    columns: list[str] | None = None,
) -> dict:
    """Generate profiling statistics for an Iceberg table.

    Args:
        catalog: The Iceberg catalog
        table_name: Name of the table (with or without namespace)
        columns: Optional list of columns to profile (default: all)

    Returns:
        Dict with table name, row count, and per-column statistics

    Raises:
        ValueError: If table not found or columns invalid
    """
    import duckdb

    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    schema = table.schema()

    # Read data
    try:
        arrow_table = table.scan().to_arrow()
    except Exception:
        arrow_table = pa.table({
            f.name: pa.array([], type=_iceberg_type_to_arrow(str(f.field_type)) or pa.string())
            for f in schema.fields
        })

    row_count = arrow_table.num_rows

    # Determine which columns to profile
    all_field_names = [f.name for f in schema.fields]
    if columns:
        invalid = [c for c in columns if c not in set(all_field_names)]
        if invalid:
            raise ValueError(
                f"Columns not found: {invalid}. "
                f"Available: {sorted(all_field_names)}"
            )
        profile_fields = [f for f in schema.fields if f.name in columns]
    else:
        profile_fields = list(schema.fields)

    if row_count == 0:
        col_stats = {}
        for field in profile_fields:
            col_stats[field.name] = {
                "type": str(field.field_type),
                "nulls": 0,
                "unique": 0,
            }
        return {
            "table": table_name,
            "row_count": 0,
            "column_count": len(profile_fields),
            "columns": col_stats,
        }

    conn = duckdb.connect(":memory:")
    conn.register("data", arrow_table)

    col_stats = {}
    for field in profile_fields:
        col_name = field.name
        field_type = str(field.field_type)
        quoted = f'"{col_name}"'

        stats: dict = {"type": field_type}

        # Null count and unique count
        basic = conn.execute(
            f"SELECT COUNT(*) - COUNT({quoted}) AS nulls, "
            f"COUNT(DISTINCT {quoted}) AS uniq "
            f"FROM data"
        ).fetchone()
        stats["nulls"] = basic[0]
        stats["unique"] = basic[1]

        # Numeric stats
        if field_type in ("long", "double", "int", "float"):
            num = conn.execute(
                f"SELECT MIN({quoted}), MAX({quoted}), "
                f"AVG({quoted}), STDDEV({quoted}), "
                f"PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {quoted}), "
                f"PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {quoted}), "
                f"PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {quoted}) "
                f"FROM data"
            ).fetchone()
            stats["min"] = num[0]
            stats["max"] = num[1]
            stats["mean"] = round(num[2], 4) if num[2] is not None else None
            stats["std"] = round(num[3], 4) if num[3] is not None else None
            stats["p25"] = num[4]
            stats["p50"] = num[5]
            stats["p75"] = num[6]

        # String stats: top values
        elif field_type == "string":
            top_rows = conn.execute(
                f"SELECT {quoted}, COUNT(*) AS cnt "
                f"FROM data WHERE {quoted} IS NOT NULL "
                f"GROUP BY {quoted} ORDER BY cnt DESC LIMIT 10"
            ).fetchall()
            stats["top_values"] = {row[0]: row[1] for row in top_rows}

        # Date/timestamp: min and max
        elif field_type in ("date", "timestamp", "timestamptz"):
            minmax = conn.execute(
                f"SELECT MIN({quoted}), MAX({quoted}) FROM data"
            ).fetchone()
            stats["min"] = str(minmax[0]) if minmax[0] is not None else None
            stats["max"] = str(minmax[1]) if minmax[1] is not None else None

        col_stats[col_name] = stats

    conn.close()

    return {
        "table": table_name,
        "row_count": row_count,
        "column_count": len(profile_fields),
        "columns": col_stats,
    }


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
