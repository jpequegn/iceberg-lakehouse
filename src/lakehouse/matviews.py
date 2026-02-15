"""Materialized views — cached query results stored as Iceberg tables."""

import datetime
import json
import time
from pathlib import Path
from typing import Optional

import pandas as pd

DEFAULT_MATVIEW_PATH = Path.home() / ".lakehouse" / "materialized_views.json"
MV_PREFIX = "mv_"


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_MATVIEW_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_MATVIEW_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _backing_table_name(name: str, namespace: str = "default") -> str:
    return f"{namespace}.{MV_PREFIX}{name}"


def _get_source_snapshot_ids(catalog, sql: str) -> dict:
    """Try to identify source tables and their current snapshot IDs."""
    from .catalog import list_tables
    snapshots = {}
    all_tables = list_tables(catalog, namespace="*")
    sql_lower = sql.lower()
    for tbl in all_tables:
        short = tbl.split(".")[-1]
        if short.lower() in sql_lower:
            try:
                table = catalog.load_table(tbl)
                snap = table.current_snapshot()
                if snap:
                    snapshots[tbl] = snap.snapshot_id
            except Exception:
                pass
    return snapshots


def create_materialized_view(
    name: str,
    sql: str,
    engine,
    catalog,
    description: str = "",
    store_path: Optional[Path] = None,
) -> dict:
    """Create a materialized view by executing SQL and storing results.

    Args:
        name: View name
        sql: SQL SELECT query
        engine: QueryEngine instance
        catalog: Iceberg catalog
        description: Optional description
        store_path: Optional path to metadata store

    Returns:
        Dict with view details.
    """
    if not name or not name.strip():
        raise ValueError("View name must not be empty")
    if not sql or not sql.strip():
        raise ValueError("SQL must not be empty")

    store = _load_store(store_path)
    if name in store:
        raise ValueError(f"Materialized view '{name}' already exists")

    # Execute SQL to get results
    df = engine.execute(sql, max_rows=1_000_000)
    backing_table = _backing_table_name(name)

    # Infer column types and create backing table
    from .catalog import create_table, insert_rows
    type_map = {
        "int64": "long",
        "float64": "double",
        "object": "string",
        "bool": "boolean",
    }
    columns = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        columns[col] = type_map.get(dtype, "string")

    create_table(catalog, backing_table, columns)

    # Insert data
    if not df.empty:
        rows = df.to_dict(orient="records")
        insert_rows(catalog, backing_table, rows)

    # Get source snapshot IDs for freshness tracking
    source_snapshots = _get_source_snapshot_ids(catalog, sql)

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    store[name] = {
        "sql": sql,
        "description": description,
        "backing_table": backing_table,
        "created_at": now,
        "last_refreshed": now,
        "row_count": len(df),
        "source_snapshot_ids": source_snapshots,
    }
    _save_store(store, store_path)

    return {
        "name": name,
        "sql": sql,
        "description": description,
        "backing_table": backing_table,
        "row_count": len(df),
        "created_at": now,
        "message": f"Created materialized view '{name}' ({len(df)} rows)",
    }


def refresh_materialized_view(
    name: str,
    engine,
    catalog,
    store_path: Optional[Path] = None,
) -> dict:
    """Re-execute the view SQL and replace the backing table data."""
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"Materialized view '{name}' not found")

    entry = store[name]
    sql = entry["sql"]
    backing_table = entry["backing_table"]

    start = time.time()
    rows_before = entry.get("row_count", 0)

    # Re-execute SQL
    df = engine.execute(sql, max_rows=1_000_000)

    # Overwrite backing table
    from .catalog import delete_rows, insert_rows
    try:
        delete_rows(catalog, backing_table, "1=1")
    except Exception:
        pass

    if not df.empty:
        rows = df.to_dict(orient="records")
        insert_rows(catalog, backing_table, rows)

    duration_ms = int((time.time() - start) * 1000)

    # Update metadata
    source_snapshots = _get_source_snapshot_ids(catalog, sql)
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    entry["last_refreshed"] = now
    entry["row_count"] = len(df)
    entry["source_snapshot_ids"] = source_snapshots
    _save_store(store, store_path)

    return {
        "name": name,
        "rows_before": rows_before,
        "rows_after": len(df),
        "duration_ms": duration_ms,
        "last_refreshed": now,
        "message": f"Refreshed '{name}': {rows_before} → {len(df)} rows ({duration_ms}ms)",
    }


def list_materialized_views(
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all materialized views."""
    store = _load_store(store_path)
    result = []
    for name, entry in sorted(store.items()):
        result.append({
            "name": name,
            "sql": entry["sql"],
            "description": entry.get("description", ""),
            "backing_table": entry["backing_table"],
            "row_count": entry.get("row_count", 0),
            "created_at": entry.get("created_at", ""),
            "last_refreshed": entry.get("last_refreshed", ""),
        })
    return result


def drop_materialized_view(
    name: str,
    catalog,
    store_path: Optional[Path] = None,
) -> dict:
    """Drop a materialized view and its backing table."""
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"Materialized view '{name}' not found")

    backing_table = store[name]["backing_table"]

    # Drop backing table
    try:
        catalog.drop_table(backing_table)
    except Exception:
        pass  # Backing table may not exist

    del store[name]
    _save_store(store, store_path)

    return {
        "name": name,
        "message": f"Dropped materialized view '{name}'",
    }


def query_materialized_view(
    name: str,
    engine,
    max_rows: int = 1000,
    store_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Query the cached results of a materialized view (fast, no re-execution)."""
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"Materialized view '{name}' not found")

    backing_table = store[name]["backing_table"]
    short_name = backing_table.split(".")[-1]
    return engine.execute(f"SELECT * FROM {short_name}", max_rows=max_rows)


def check_materialized_view_freshness(
    name: str,
    catalog,
    store_path: Optional[Path] = None,
) -> dict:
    """Check if a materialized view is stale."""
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"Materialized view '{name}' not found")

    entry = store[name]
    stored_snapshots = entry.get("source_snapshot_ids", {})
    current_snapshots = _get_source_snapshot_ids(catalog, entry["sql"])

    stale = False
    changed_tables = []
    for tbl, snap_id in current_snapshots.items():
        stored_id = stored_snapshots.get(tbl)
        if stored_id != snap_id:
            stale = True
            changed_tables.append(tbl)

    return {
        "name": name,
        "stale": stale,
        "changed_tables": changed_tables,
        "last_refreshed": entry.get("last_refreshed", ""),
        "message": (
            f"'{name}' is stale ({len(changed_tables)} source table(s) changed)"
            if stale
            else f"'{name}' is fresh"
        ),
    }
