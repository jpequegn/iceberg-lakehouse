"""Table cloning and branching — zero-copy snapshots for safe experimentation."""

import datetime
import json
from pathlib import Path
from typing import Optional

import pyarrow as pa

DEFAULT_CLONES_PATH = Path.home() / ".lakehouse" / "clones.json"


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_CLONES_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_CLONES_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize_name(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def clone_table(
    catalog,
    source_table: str,
    target_table: str,
    as_of: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Clone a table (copies data from source to new target table).

    Args:
        catalog: Iceberg catalog
        source_table: Source table name
        target_table: Target table name (must not exist)
        as_of: Optional snapshot ID or timestamp for point-in-time clone
        store_path: Optional path to clones metadata store

    Returns:
        Dict with clone details.
    """
    source = _normalize_name(source_table)
    target = _normalize_name(target_table)

    # Load source table
    try:
        src_tbl = catalog.load_table(source)
    except Exception as e:
        raise ValueError(f"Source table '{source}' not found: {e}")

    # Verify target doesn't exist
    try:
        catalog.load_table(target)
        raise ValueError(f"Target table '{target}' already exists")
    except ValueError:
        raise
    except Exception:
        pass  # Table doesn't exist, good

    # Read source data (optionally at a specific point in time)
    source_snapshot_id = None
    if as_of:
        from .catalog import scan_as_of
        arrow_data = scan_as_of(catalog, source, as_of)
        # Determine which snapshot was used
        try:
            source_snapshot_id = int(as_of)
        except (ValueError, TypeError):
            # It was a timestamp — get the snapshot that was used
            ts = datetime.datetime.fromisoformat(as_of)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            timestamp_ms = int(ts.timestamp() * 1000)
            snapshot = src_tbl.snapshot_as_of_timestamp(timestamp_ms)
            source_snapshot_id = snapshot.snapshot_id if snapshot else None
    else:
        arrow_data = src_tbl.scan().to_arrow()
        current_snapshot = src_tbl.current_snapshot()
        source_snapshot_id = current_snapshot.snapshot_id if current_snapshot else None

    # Create target table with same schema
    schema = src_tbl.schema()
    catalog.create_table(identifier=target, schema=schema)

    # Write data to target
    target_tbl = catalog.load_table(target)
    if len(arrow_data) > 0:
        target_tbl.append(arrow_data)

    row_count = len(arrow_data)

    # Record clone metadata
    store = _load_store(store_path)
    store[target] = {
        "source_table": source,
        "source_snapshot_id": source_snapshot_id,
        "cloned_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "row_count": row_count,
        "as_of": as_of,
    }
    _save_store(store, store_path)

    return {
        "source": source,
        "target": target,
        "row_count": row_count,
        "source_snapshot_id": source_snapshot_id,
        "as_of": as_of,
        "message": f"Cloned {source} → {target} ({row_count} rows)",
    }


def list_clones(
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all active clones with their source tables."""
    store = _load_store(store_path)
    result = []
    for name, meta in sorted(store.items()):
        result.append({
            "clone": name,
            "source_table": meta["source_table"],
            "cloned_at": meta["cloned_at"],
            "row_count": meta["row_count"],
            "as_of": meta.get("as_of"),
        })
    return result


def promote_clone(
    catalog,
    clone_table_name: str,
    original_table: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Replace the original table's data with the clone's data.

    Reads all data from the clone and overwrites the original table.
    """
    clone_name = _normalize_name(clone_table_name)
    original = _normalize_name(original_table)

    # Load clone table
    try:
        clone_tbl = catalog.load_table(clone_name)
    except Exception as e:
        raise ValueError(f"Clone table '{clone_name}' not found: {e}")

    # Load original table
    try:
        orig_tbl = catalog.load_table(original)
    except Exception as e:
        raise ValueError(f"Original table '{original}' not found: {e}")

    # Read clone data
    clone_data = clone_tbl.scan().to_arrow()

    # Overwrite original: delete all then append
    from .catalog import delete_rows
    try:
        delete_rows(catalog, original, "1=1")
    except Exception:
        pass  # May fail if table is empty, that's ok

    if len(clone_data) > 0:
        orig_tbl = catalog.load_table(original)  # reload after delete
        orig_tbl.append(clone_data)

    row_count = len(clone_data)

    # Remove clone metadata
    store = _load_store(store_path)
    store.pop(clone_name, None)
    _save_store(store, store_path)

    return {
        "clone": clone_name,
        "original": original,
        "row_count": row_count,
        "message": f"Promoted {clone_name} → {original} ({row_count} rows)",
    }


def discard_clone(
    catalog,
    clone_table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Drop a cloned table and clean up metadata."""
    clone_name = _normalize_name(clone_table_name)

    # Try to drop the table
    try:
        catalog.drop_table(clone_name)
    except Exception as e:
        raise ValueError(f"Failed to drop clone '{clone_name}': {e}")

    # Remove from metadata
    store = _load_store(store_path)
    store.pop(clone_name, None)
    _save_store(store, store_path)

    return {
        "clone": clone_name,
        "message": f"Discarded clone {clone_name}",
    }
