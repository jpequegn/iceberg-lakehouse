"""Change data capture — row-level change tracking between snapshots."""

import csv
import datetime
import io
import json
from pathlib import Path
from typing import Optional

DEFAULT_CDC_PATH = Path.home() / ".lakehouse" / "cdc.json"


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def get_changes(
    catalog,
    table_name: str,
    from_snapshot: Optional[str] = None,
    to_snapshot: Optional[str] = None,
    key_columns: Optional[list[str]] = None,
) -> dict:
    """Compute row-level changes between two snapshots.

    Categorizes changes as INSERT, UPDATE, or DELETE.
    Updates are detected when key columns match but other columns differ.
    """
    import duckdb
    from .catalog import scan_as_of, get_snapshots, _resolve_snapshot_id

    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)

    # Resolve snapshots
    snapshots = get_snapshots(catalog, table_name)
    if not snapshots:
        return {
            "table": table_name,
            "changes": [],
            "summary": {"inserts": 0, "updates": 0, "deletes": 0},
            "from_snapshot": None,
            "to_snapshot": None,
            "message": "No snapshots available",
        }

    if from_snapshot is not None:
        from_arrow = scan_as_of(catalog, table_name, from_snapshot)
        from_id = _resolve_snapshot_id(table, from_snapshot)
    else:
        # Use the second-to-last snapshot (or empty if only one)
        if len(snapshots) < 2:
            from_arrow = None
            from_id = None
        else:
            from_id = snapshots[-2]["snapshot_id"]
            from_arrow = scan_as_of(catalog, table_name, str(from_id))

    if to_snapshot is not None:
        to_arrow = scan_as_of(catalog, table_name, to_snapshot)
        to_id = _resolve_snapshot_id(table, to_snapshot)
    else:
        to_arrow = table.scan().to_arrow()
        current = table.current_snapshot()
        to_id = current.snapshot_id if current else None

    # Same snapshot — no changes
    if from_id == to_id:
        return {
            "table": table_name,
            "changes": [],
            "summary": {"inserts": 0, "updates": 0, "deletes": 0},
            "from_snapshot": from_id,
            "to_snapshot": to_id,
            "message": "No changes (same snapshot)",
        }

    columns = [field.name for field in to_arrow.schema]
    col_list = ", ".join(f'"{c}"' for c in columns)

    conn = duckdb.connect()

    if from_arrow is None or from_arrow.num_rows == 0:
        # All rows in to_snapshot are inserts
        conn.register("to_tbl", to_arrow)
        rows = conn.execute(f"SELECT {col_list} FROM to_tbl").fetchall()
        conn.close()
        changes = [
            {"type": "INSERT", "row": dict(zip(columns, row))}
            for row in rows
        ]
        return {
            "table": table_name,
            "changes": changes,
            "summary": {"inserts": len(changes), "updates": 0, "deletes": 0},
            "from_snapshot": from_id,
            "to_snapshot": to_id,
            "message": f"{len(changes)} inserts",
        }

    conn.register("from_tbl", from_arrow)
    conn.register("to_tbl", to_arrow)

    # Rows in to but not in from = potentially inserts or updates
    added = conn.execute(
        f"SELECT {col_list} FROM to_tbl EXCEPT SELECT {col_list} FROM from_tbl"
    ).fetchall()
    added_rows = [dict(zip(columns, row)) for row in added]

    # Rows in from but not in to = potentially deletes or updates
    removed = conn.execute(
        f"SELECT {col_list} FROM from_tbl EXCEPT SELECT {col_list} FROM to_tbl"
    ).fetchall()
    removed_rows = [dict(zip(columns, row)) for row in removed]

    conn.close()

    # Detect updates using key columns
    keys = key_columns or ([columns[0]] if columns else [])
    changes = _classify_changes(added_rows, removed_rows, keys, columns)

    inserts = sum(1 for c in changes if c["type"] == "INSERT")
    updates = sum(1 for c in changes if c["type"] == "UPDATE")
    deletes = sum(1 for c in changes if c["type"] == "DELETE")

    parts = []
    if inserts:
        parts.append(f"{inserts} inserts")
    if updates:
        parts.append(f"{updates} updates")
    if deletes:
        parts.append(f"{deletes} deletes")

    return {
        "table": table_name,
        "changes": changes,
        "summary": {"inserts": inserts, "updates": updates, "deletes": deletes},
        "from_snapshot": from_id,
        "to_snapshot": to_id,
        "message": ", ".join(parts) if parts else "No changes",
    }


def _classify_changes(
    added_rows: list[dict],
    removed_rows: list[dict],
    keys: list[str],
    columns: list[str],
) -> list[dict]:
    """Classify raw added/removed rows into INSERT, UPDATE, DELETE."""
    changes = []

    # Index removed rows by key for fast lookup
    removed_by_key = {}
    for row in removed_rows:
        key = tuple(row.get(k) for k in keys)
        removed_by_key[key] = row

    matched_keys = set()

    for row in added_rows:
        key = tuple(row.get(k) for k in keys)
        if key in removed_by_key:
            # Key exists in both — this is an UPDATE
            old_row = removed_by_key[key]
            changed_cols = [c for c in columns if c not in keys and row.get(c) != old_row.get(c)]
            changes.append({
                "type": "UPDATE",
                "key": dict(zip(keys, key)),
                "before": old_row,
                "after": row,
                "changed_columns": changed_cols,
            })
            matched_keys.add(key)
        else:
            # New key — this is an INSERT
            changes.append({"type": "INSERT", "row": row})

    # Remaining removed rows are DELETEs
    for row in removed_rows:
        key = tuple(row.get(k) for k in keys)
        if key not in matched_keys:
            changes.append({"type": "DELETE", "row": row})

    return changes


def get_change_log(
    catalog,
    table_name: str,
    limit: int = 100,
    key_columns: Optional[list[str]] = None,
) -> list[dict]:
    """Get a chronological log of changes across all snapshots."""
    from .catalog import get_snapshots

    table_name = _normalize(table_name)
    snapshots = get_snapshots(catalog, table_name)

    if len(snapshots) < 2:
        return []

    log = []
    pairs = list(zip(snapshots[:-1], snapshots[1:]))
    # Most recent first
    for old_snap, new_snap in reversed(pairs[-limit:]):
        try:
            result = get_changes(
                catalog, table_name,
                from_snapshot=str(old_snap["snapshot_id"]),
                to_snapshot=str(new_snap["snapshot_id"]),
                key_columns=key_columns,
            )
            log.append({
                "from_snapshot": old_snap["snapshot_id"],
                "to_snapshot": new_snap["snapshot_id"],
                "timestamp": new_snap["timestamp"],
                "operation": new_snap.get("operation", "unknown"),
                "summary": result["summary"],
                "change_count": sum(result["summary"].values()),
            })
        except Exception:
            continue

    return log


def get_change_summary(
    catalog,
    table_name: str,
    from_snapshot: Optional[str] = None,
    to_snapshot: Optional[str] = None,
    key_columns: Optional[list[str]] = None,
) -> dict:
    """Get summary statistics for changes between two snapshots."""
    table_name = _normalize(table_name)
    result = get_changes(
        catalog, table_name,
        from_snapshot=from_snapshot,
        to_snapshot=to_snapshot,
        key_columns=key_columns,
    )

    # Compute affected columns
    affected_columns = set()
    for change in result["changes"]:
        if change["type"] == "UPDATE":
            affected_columns.update(change.get("changed_columns", []))
        elif change["type"] == "INSERT":
            affected_columns.update(change["row"].keys())
        elif change["type"] == "DELETE":
            affected_columns.update(change["row"].keys())

    total = sum(result["summary"].values())

    return {
        "table": table_name,
        "from_snapshot": result["from_snapshot"],
        "to_snapshot": result["to_snapshot"],
        "inserts": result["summary"]["inserts"],
        "updates": result["summary"]["updates"],
        "deletes": result["summary"]["deletes"],
        "total_changes": total,
        "affected_columns": sorted(affected_columns),
        "message": f"Change summary: {result['message']}",
    }


def export_changes(
    catalog,
    table_name: str,
    from_snapshot: str,
    to_snapshot: str,
    format: str = "json",
    key_columns: Optional[list[str]] = None,
) -> str:
    """Export CDC data to JSON or CSV string."""
    table_name = _normalize(table_name)
    result = get_changes(
        catalog, table_name,
        from_snapshot=from_snapshot,
        to_snapshot=to_snapshot,
        key_columns=key_columns,
    )

    if format == "json":
        export = {
            "table": table_name,
            "from_snapshot": result["from_snapshot"],
            "to_snapshot": result["to_snapshot"],
            "exported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "summary": result["summary"],
            "changes": result["changes"],
        }
        return json.dumps(export, indent=2, default=str)

    elif format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)

        # Collect all possible columns
        all_cols = set()
        for change in result["changes"]:
            if change["type"] in ("INSERT", "DELETE"):
                all_cols.update(change["row"].keys())
            elif change["type"] == "UPDATE":
                all_cols.update(change["after"].keys())
        all_cols = sorted(all_cols)

        writer.writerow(["change_type"] + all_cols)
        for change in result["changes"]:
            if change["type"] == "INSERT":
                writer.writerow(["INSERT"] + [change["row"].get(c, "") for c in all_cols])
            elif change["type"] == "DELETE":
                writer.writerow(["DELETE"] + [change["row"].get(c, "") for c in all_cols])
            elif change["type"] == "UPDATE":
                writer.writerow(["UPDATE_BEFORE"] + [change["before"].get(c, "") for c in all_cols])
                writer.writerow(["UPDATE_AFTER"] + [change["after"].get(c, "") for c in all_cols])

        return output.getvalue()

    else:
        raise ValueError(f"Unsupported format: {format}. Use 'json' or 'csv'.")


def replay_changes(
    catalog,
    changes: list[dict],
    target_table: str,
) -> dict:
    """Apply captured changes to a target table."""
    from .catalog import insert_rows, update_rows, delete_rows

    target_table = _normalize(target_table)
    applied = {"inserts": 0, "updates": 0, "deletes": 0, "errors": []}

    for change in changes:
        try:
            if change["type"] == "INSERT":
                insert_rows(catalog, target_table, [change["row"]])
                applied["inserts"] += 1
            elif change["type"] == "DELETE":
                row = change["row"]
                # Build filter from all columns
                filters = []
                for k, v in row.items():
                    if v is None:
                        filters.append(f'"{k}" IS NULL')
                    elif isinstance(v, str):
                        escaped = v.replace("'", "''")
                        filters.append(f'"{k}" = \'{escaped}\'')
                    else:
                        filters.append(f'"{k}" = {v}')
                filter_expr = " AND ".join(filters)
                delete_rows(catalog, target_table, filter_expr)
                applied["deletes"] += 1
            elif change["type"] == "UPDATE":
                key = change.get("key", {})
                after = change.get("after", {})
                if key:
                    filters = []
                    for k, v in key.items():
                        if v is None:
                            filters.append(f'"{k}" IS NULL')
                        elif isinstance(v, str):
                            escaped = v.replace("'", "''")
                            filters.append(f'"{k}" = \'{escaped}\'')
                        else:
                            filters.append(f'"{k}" = {v}')
                    filter_expr = " AND ".join(filters)
                    # Update to after values (exclude key columns)
                    updates = {c: v for c, v in after.items() if c not in key}
                    if updates:
                        update_rows(catalog, target_table, filter_expr, updates)
                        applied["updates"] += 1
        except Exception as e:
            applied["errors"].append(f"{change['type']}: {str(e)}")

    total = applied["inserts"] + applied["updates"] + applied["deletes"]
    return {
        "target_table": target_table,
        "applied": applied,
        "total_applied": total,
        "errors": len(applied["errors"]),
        "message": f"Replayed {total} changes to '{target_table}' ({len(applied['errors'])} errors)",
    }
