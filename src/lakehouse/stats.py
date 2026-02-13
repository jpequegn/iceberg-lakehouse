"""Table statistics cache for fast MCP access."""

import datetime
import json
from pathlib import Path
from typing import Optional

from pyiceberg.catalog import Catalog

DEFAULT_STATS_PATH = Path.home() / ".lakehouse" / "stats_cache.json"


def _load_cache(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_STATS_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_cache(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_STATS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def compute_table_stats(
    catalog: Catalog,
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Compute and cache comprehensive statistics for a table.

    Args:
        catalog: The Iceberg catalog
        table_name: Table name (with or without namespace)
        store_path: Optional path to stats cache file

    Returns:
        Dict with row_count, column_count, size_bytes, snapshot_count, etc.
    """
    import duckdb
    import pyarrow as pa

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
        arrow_table = None

    row_count = arrow_table.num_rows if arrow_table is not None else 0

    # Snapshot info
    snapshots = list(table.snapshots())
    snapshot_count = len(snapshots)
    current = table.current_snapshot()
    current_snapshot_id = current.snapshot_id if current else None

    last_modified = None
    if current:
        last_modified = datetime.datetime.fromtimestamp(
            current.timestamp_ms / 1000, tz=datetime.timezone.utc
        ).isoformat()

    # Data file stats
    file_count = 0
    total_size = 0
    try:
        for task in table.scan().plan_files():
            file_count += 1
            total_size += task.file.file_size_in_bytes
    except Exception:
        pass

    # Column stats
    columns = {}
    if row_count > 0 and arrow_table is not None:
        conn = duckdb.connect()
        conn.register("data", arrow_table)

        for field in schema.fields:
            col_name = field.name
            field_type = str(field.field_type)
            quoted = f'"{col_name}"'

            col_info = {"type": field_type}

            basic = conn.execute(
                f"SELECT COUNT(*) - COUNT({quoted}) AS nulls, "
                f"COUNT(DISTINCT {quoted}) AS uniq FROM data"
            ).fetchone()
            col_info["nulls"] = basic[0]
            col_info["unique"] = basic[1]

            if field_type in ("long", "double", "int", "float"):
                num = conn.execute(
                    f"SELECT MIN({quoted}), MAX({quoted}), AVG({quoted}) FROM data"
                ).fetchone()
                col_info["min"] = num[0]
                col_info["max"] = num[1]
                col_info["mean"] = round(num[2], 4) if num[2] is not None else None

            elif field_type in ("date", "timestamp", "timestamptz"):
                minmax = conn.execute(
                    f"SELECT MIN({quoted}), MAX({quoted}) FROM data"
                ).fetchone()
                col_info["min"] = str(minmax[0]) if minmax[0] is not None else None
                col_info["max"] = str(minmax[1]) if minmax[1] is not None else None

            columns[col_name] = col_info

        conn.close()
    else:
        for field in schema.fields:
            columns[field.name] = {
                "type": str(field.field_type),
                "nulls": 0,
                "unique": 0,
            }

    stats = {
        "row_count": row_count,
        "column_count": len(schema.fields),
        "size_bytes": total_size,
        "data_files": file_count,
        "snapshot_count": snapshot_count,
        "last_modified": last_modified,
        "cached_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "snapshot_id_at_cache": current_snapshot_id,
        "columns": columns,
    }

    # Save to cache
    cache = _load_cache(store_path)
    cache[table_name] = stats
    _save_cache(cache, store_path)

    return stats


def get_cached_stats(
    table_name: str,
    store_path: Optional[Path] = None,
) -> Optional[dict]:
    """Get cached stats for a table. Returns None if no cache."""
    if "." not in table_name:
        table_name = f"default.{table_name}"

    cache = _load_cache(store_path)
    return cache.get(table_name)


def get_all_cached_stats(
    store_path: Optional[Path] = None,
) -> dict:
    """Get cached stats for all tables."""
    return _load_cache(store_path)


def refresh_stats(
    catalog: Catalog,
    table_name: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Refresh stats for one table or all tables.

    Args:
        catalog: The Iceberg catalog
        table_name: Table to refresh (None = all tables)
        store_path: Optional path to stats cache

    Returns:
        Dict with tables refreshed and timing
    """
    from .catalog import list_tables

    start = datetime.datetime.now(datetime.timezone.utc)

    if table_name:
        compute_table_stats(catalog, table_name, store_path)
        tables_refreshed = [table_name if "." in table_name else f"default.{table_name}"]
    else:
        all_tables = list_tables(catalog, namespace="*")
        tables_refreshed = []
        for t in all_tables:
            compute_table_stats(catalog, t, store_path)
            tables_refreshed.append(t)

    elapsed = (datetime.datetime.now(datetime.timezone.utc) - start).total_seconds()

    return {
        "tables_refreshed": tables_refreshed,
        "count": len(tables_refreshed),
        "duration_seconds": round(elapsed, 3),
        "message": f"Refreshed stats for {len(tables_refreshed)} table(s) in {elapsed:.3f}s",
    }


def is_stats_stale(
    table_name: str,
    catalog: Optional[Catalog] = None,
    store_path: Optional[Path] = None,
) -> bool:
    """Check if cached stats are stale.

    Returns True if table has been modified since last cache, or no cache exists.
    """
    if "." not in table_name:
        table_name = f"default.{table_name}"

    cached = get_cached_stats(table_name, store_path)
    if cached is None:
        return True

    if catalog is None:
        return False

    try:
        table = catalog.load_table(table_name)
        current = table.current_snapshot()
        if current is None:
            return cached.get("snapshot_id_at_cache") is not None
        return current.snapshot_id != cached.get("snapshot_id_at_cache")
    except Exception:
        return True
