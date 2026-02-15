"""Schema evolution tracking — history, diff, migration, and compatibility checks."""

import datetime
from typing import Optional


def _normalize_table(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def _schema_to_dict(schema) -> dict:
    """Convert a PyIceberg Schema to a serializable dict."""
    return {
        "schema_id": schema.schema_id,
        "fields": [
            {
                "field_id": field.field_id,
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
                "doc": field.doc,
            }
            for field in schema.fields
        ],
    }


def get_schema_history(catalog, table_name: str) -> list[dict]:
    """Get the full schema history for a table across snapshots.

    Returns:
        List of dicts with schema_id, snapshot_id, timestamp, fields, and changes.
    """
    table_name = _normalize_table(table_name)

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    metadata = table.metadata

    # Build schema lookup by ID
    schemas_by_id = {}
    for schema in metadata.schemas:
        schemas_by_id[schema.schema_id] = schema

    # Walk snapshots in order to find schema changes
    history = []
    seen_schema_ids = set()

    snapshots = sorted(metadata.snapshots, key=lambda s: s.timestamp_ms)

    for snap in snapshots:
        schema_id = snap.schema_id
        if schema_id is None:
            schema_id = 0

        schema = schemas_by_id.get(schema_id)
        if schema is None:
            continue

        timestamp = datetime.datetime.fromtimestamp(
            snap.timestamp_ms / 1000, tz=datetime.timezone.utc
        ).isoformat()

        is_new_schema = schema_id not in seen_schema_ids
        seen_schema_ids.add(schema_id)

        # Compute change summary if this is a new schema version
        change_summary = None
        if is_new_schema and len(history) > 0:
            prev_schema = schemas_by_id.get(history[-1]["schema_id"])
            if prev_schema:
                diff = _compute_diff(prev_schema, schema)
                change_summary = _summarize_diff(diff)

        entry = {
            "schema_id": schema_id,
            "snapshot_id": snap.snapshot_id,
            "timestamp": timestamp,
            "fields": [
                {
                    "field_id": f.field_id,
                    "name": f.name,
                    "type": str(f.field_type),
                    "required": f.required,
                }
                for f in schema.fields
            ],
            "is_schema_change": is_new_schema and len(history) > 0,
            "change_summary": change_summary,
        }
        if is_new_schema:
            history.append(entry)

    # If no snapshots yet, return at least the current schema
    if not history and schemas_by_id:
        current = schemas_by_id.get(metadata.current_schema_id, next(iter(schemas_by_id.values())))
        history.append({
            "schema_id": current.schema_id,
            "snapshot_id": None,
            "timestamp": None,
            "fields": [
                {
                    "field_id": f.field_id,
                    "name": f.name,
                    "type": str(f.field_type),
                    "required": f.required,
                }
                for f in current.fields
            ],
            "is_schema_change": False,
            "change_summary": None,
        })

    return history


def _compute_diff(old_schema, new_schema) -> dict:
    """Compute the diff between two PyIceberg Schema objects."""
    old_fields = {f.field_id: f for f in old_schema.fields}
    new_fields = {f.field_id: f for f in new_schema.fields}

    old_ids = set(old_fields.keys())
    new_ids = set(new_fields.keys())

    added = []
    for fid in sorted(new_ids - old_ids):
        f = new_fields[fid]
        added.append({
            "field_id": fid,
            "name": f.name,
            "type": str(f.field_type),
        })

    dropped = []
    for fid in sorted(old_ids - new_ids):
        f = old_fields[fid]
        dropped.append({
            "field_id": fid,
            "name": f.name,
            "type": str(f.field_type),
        })

    renamed = []
    type_changes = []
    for fid in sorted(old_ids & new_ids):
        old_f = old_fields[fid]
        new_f = new_fields[fid]
        if old_f.name != new_f.name:
            renamed.append({
                "field_id": fid,
                "old_name": old_f.name,
                "new_name": new_f.name,
            })
        if str(old_f.field_type) != str(new_f.field_type):
            type_changes.append({
                "field_id": fid,
                "name": new_f.name,
                "old_type": str(old_f.field_type),
                "new_type": str(new_f.field_type),
            })

    return {
        "added_columns": added,
        "dropped_columns": dropped,
        "renamed_columns": renamed,
        "type_changes": type_changes,
    }


def _summarize_diff(diff: dict) -> str:
    """Generate a human-readable summary of a schema diff."""
    parts = []
    if diff["added_columns"]:
        names = [c["name"] for c in diff["added_columns"]]
        parts.append(f"added {', '.join(names)}")
    if diff["dropped_columns"]:
        names = [c["name"] for c in diff["dropped_columns"]]
        parts.append(f"dropped {', '.join(names)}")
    if diff["renamed_columns"]:
        renames = [f"{c['old_name']}→{c['new_name']}" for c in diff["renamed_columns"]]
        parts.append(f"renamed {', '.join(renames)}")
    if diff["type_changes"]:
        changes = [f"{c['name']}: {c['old_type']}→{c['new_type']}" for c in diff["type_changes"]]
        parts.append(f"type changed {', '.join(changes)}")
    return "; ".join(parts) if parts else "no changes"


def schema_diff(
    catalog,
    table_name: str,
    from_snapshot: Optional[int] = None,
    to_snapshot: Optional[int] = None,
) -> dict:
    """Compare schemas between two snapshots.

    If no snapshots specified, compares second-to-last schema with current.

    Returns:
        Dict with added_columns, dropped_columns, renamed_columns, type_changes.
    """
    table_name = _normalize_table(table_name)

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    metadata = table.metadata
    schemas_by_id = {s.schema_id: s for s in metadata.schemas}
    snapshots_by_id = {s.snapshot_id: s for s in metadata.snapshots}

    if from_snapshot and to_snapshot:
        from_snap = snapshots_by_id.get(from_snapshot)
        to_snap = snapshots_by_id.get(to_snapshot)
        if not from_snap:
            raise ValueError(f"Snapshot {from_snapshot} not found")
        if not to_snap:
            raise ValueError(f"Snapshot {to_snapshot} not found")
        from_schema_id = from_snap.schema_id if from_snap.schema_id is not None else 0
        to_schema_id = to_snap.schema_id if to_snap.schema_id is not None else 0
    else:
        # Use the last two distinct schemas
        ordered_snapshots = sorted(metadata.snapshots, key=lambda s: s.timestamp_ms)
        seen_schemas = []
        for snap in ordered_snapshots:
            sid = snap.schema_id if snap.schema_id is not None else 0
            if not seen_schemas or seen_schemas[-1] != sid:
                seen_schemas.append(sid)

        if len(seen_schemas) < 2:
            # Only one schema version — return empty diff
            current = schemas_by_id.get(metadata.current_schema_id)
            return {
                "table": table_name,
                "from_schema_id": current.schema_id if current else 0,
                "to_schema_id": current.schema_id if current else 0,
                "added_columns": [],
                "dropped_columns": [],
                "renamed_columns": [],
                "type_changes": [],
                "summary": "no changes",
            }

        from_schema_id = seen_schemas[-2]
        to_schema_id = seen_schemas[-1]

    old_schema = schemas_by_id.get(from_schema_id)
    new_schema = schemas_by_id.get(to_schema_id)

    if not old_schema or not new_schema:
        raise ValueError("Could not resolve schemas for the specified snapshots")

    diff = _compute_diff(old_schema, new_schema)

    return {
        "table": table_name,
        "from_schema_id": from_schema_id,
        "to_schema_id": to_schema_id,
        **diff,
        "summary": _summarize_diff(diff),
    }


def generate_migration(
    catalog,
    table_name: str,
    from_snapshot: Optional[int] = None,
    to_snapshot: Optional[int] = None,
) -> dict:
    """Generate migration steps (alter_table operations) between two schema versions.

    Returns:
        Dict with steps list, each containing operation, column_name, and params.
    """
    diff = schema_diff(catalog, table_name, from_snapshot, to_snapshot)

    steps = []

    for col in diff["added_columns"]:
        steps.append({
            "operation": "add_column",
            "column_name": col["name"],
            "column_type": col["type"],
        })

    for col in diff["dropped_columns"]:
        steps.append({
            "operation": "drop_column",
            "column_name": col["name"],
        })

    for col in diff["renamed_columns"]:
        steps.append({
            "operation": "rename_column",
            "column_name": col["old_name"],
            "new_name": col["new_name"],
        })

    return {
        "table": diff["table"],
        "from_schema_id": diff["from_schema_id"],
        "to_schema_id": diff["to_schema_id"],
        "steps": steps,
        "step_count": len(steps),
        "message": f"Migration for '{diff['table']}': {len(steps)} step(s)",
    }


def check_schema_compatibility(
    catalog,
    table_name: str,
    proposed_changes: list[dict],
) -> dict:
    """Check if proposed schema changes are backward-compatible.

    Args:
        catalog: Iceberg catalog
        table_name: Table name
        proposed_changes: List of dicts with 'op' (add_column, drop_column, rename_column)
            and relevant params (column, type, new_name).

    Returns:
        Dict with compatible (bool), warnings, breaking_changes.
    """
    table_name = _normalize_table(table_name)

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    schema = table.schema()
    existing_fields = {f.name: f for f in schema.fields}

    warnings = []
    breaking_changes = []

    for change in proposed_changes:
        op = change.get("op")

        if op == "drop_column":
            col = change.get("column")
            if col and col in existing_fields:
                field = existing_fields[col]
                if field.required:
                    breaking_changes.append(
                        f"Dropping required column '{col}' is a breaking change"
                    )
                else:
                    warnings.append(
                        f"Dropping optional column '{col}' — downstream consumers may depend on it"
                    )
            elif col and col not in existing_fields:
                warnings.append(f"Column '{col}' does not exist")

        elif op == "rename_column":
            col = change.get("column")
            if col and col in existing_fields:
                warnings.append(
                    f"Renaming '{col}' to '{change.get('new_name')}' may break downstream consumers"
                )
            elif col and col not in existing_fields:
                warnings.append(f"Column '{col}' does not exist")

        elif op == "add_column":
            col = change.get("column")
            if col and col in existing_fields:
                warnings.append(f"Column '{col}' already exists")

        else:
            warnings.append(f"Unknown operation: {op}")

    compatible = len(breaking_changes) == 0

    return {
        "table": table_name,
        "compatible": compatible,
        "warnings": warnings,
        "breaking_changes": breaking_changes,
        "proposed_changes": len(proposed_changes),
        "message": (
            f"Schema changes are {'compatible' if compatible else 'NOT compatible'} "
            f"({len(breaking_changes)} breaking, {len(warnings)} warnings)"
        ),
    }
