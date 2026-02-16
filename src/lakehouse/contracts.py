"""Data contracts — define and enforce producer/consumer contracts."""

import copy
import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_CONTRACTS_PATH = Path.home() / ".lakehouse" / "contracts.json"
MAX_HISTORY = 50

TYPE_MAP = {
    "BooleanType": "boolean",
    "IntegerType": "int",
    "LongType": "long",
    "FloatType": "float",
    "DoubleType": "double",
    "StringType": "string",
    "DateType": "date",
    "TimestampType": "timestamp",
    "TimestamptzType": "timestamptz",
}


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_CONTRACTS_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_CONTRACTS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def create_contract(
    table_name: str,
    contract: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Create a contract for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name in store:
        raise ValueError(f"Contract already exists for '{table_name}'. Use update_contract to modify.")

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    entry = {
        "schema": contract.get("schema", {}),
        "quality": contract.get("quality", {}),
        "freshness": contract.get("freshness", {}),
        "constraints": contract.get("constraints", []),
        "owner": contract.get("owner", ""),
        "description": contract.get("description", ""),
        "status": "active",
        "version": 1,
        "created_at": now,
        "updated_at": now,
        "_history": [],
    }

    store[table_name] = entry
    _save_store(store, store_path)

    return {
        "table": table_name,
        "version": 1,
        "message": f"Created contract for '{table_name}'",
    }


def get_contract(
    table_name: str,
    store_path: Optional[Path] = None,
) -> Optional[dict]:
    """Get the active contract for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return None

    # Return without internal fields
    result = {k: v for k, v in entry.items() if not k.startswith("_")}
    result["table"] = table_name
    return result


def list_contracts(
    namespace: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all contracts, optionally filtered by namespace."""
    store = _load_store(store_path)
    result = []

    for table_name, entry in store.items():
        if namespace:
            ns = table_name.split(".")[0] if "." in table_name else "default"
            if ns != namespace:
                continue
        result.append({
            "table": table_name,
            "owner": entry.get("owner", ""),
            "status": entry.get("status", "active"),
            "version": entry.get("version", 1),
            "description": entry.get("description", ""),
            "created_at": entry.get("created_at", ""),
            "updated_at": entry.get("updated_at", ""),
        })

    return result


def update_contract(
    table_name: str,
    updates: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Update specific fields of a contract (partial update)."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store:
        raise ValueError(f"No contract found for '{table_name}'")

    entry = store[table_name]

    # Snapshot current state into history
    snapshot = {k: v for k, v in entry.items() if not k.startswith("_")}
    snapshot["snapshot_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    entry.setdefault("_history", []).append(snapshot)
    entry["_history"] = entry["_history"][-MAX_HISTORY:]

    # Apply updates
    updatable_fields = {"schema", "quality", "freshness", "constraints", "owner", "description"}
    for key, value in updates.items():
        if key in updatable_fields:
            entry[key] = value

    entry["version"] = entry.get("version", 1) + 1
    entry["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    _save_store(store, store_path)

    return {
        "table": table_name,
        "version": entry["version"],
        "message": f"Updated contract for '{table_name}' (v{entry['version']})",
    }


def remove_contract(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a contract."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store:
        return {"table": table_name, "message": f"No contract found for '{table_name}'"}

    del store[table_name]
    _save_store(store, store_path)

    return {"table": table_name, "message": f"Removed contract for '{table_name}'"}


def get_contract_summary(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Summary: contract terms vs current table state."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return {"table": table_name, "has_contract": False, "message": f"No contract for '{table_name}'"}

    # Schema comparison
    table = catalog.load_table(table_name)
    actual_schema = {}
    for field in table.schema().fields:
        type_name = type(field.field_type).__name__
        actual_schema[field.name] = {
            "type": TYPE_MAP.get(type_name, "string"),
            "nullable": not field.required,
        }

    contract_schema = entry.get("schema", {})
    schema_match = True
    schema_issues = []
    for col_name, col_def in contract_schema.items():
        if col_name not in actual_schema:
            schema_match = False
            schema_issues.append(f"Missing column: {col_name}")
        else:
            actual = actual_schema[col_name]
            if col_def.get("type") and actual["type"] != col_def["type"]:
                schema_match = False
                schema_issues.append(f"Type mismatch on '{col_name}': expected {col_def['type']}, got {actual['type']}")
            if "nullable" in col_def and actual["nullable"] != col_def["nullable"]:
                schema_issues.append(f"Nullable mismatch on '{col_name}': expected {col_def['nullable']}, got {actual['nullable']}")

    # Extra columns in table not in contract
    for col_name in actual_schema:
        if col_name not in contract_schema and contract_schema:
            schema_issues.append(f"Extra column not in contract: {col_name}")

    # Quality check
    quality_check = None
    quality_terms = entry.get("quality", {})
    if quality_terms.get("min_score") is not None:
        try:
            from .quality import compute_quality_score
            score_result = compute_quality_score(catalog, table_name)
            current_score = score_result.get("overall_score", 0)
            quality_check = {
                "min_score": quality_terms["min_score"],
                "current_score": current_score,
                "passing": current_score >= quality_terms["min_score"],
            }
        except Exception:
            quality_check = {"error": "Could not compute quality score"}

    # Freshness check
    freshness_check = None
    freshness_terms = entry.get("freshness", {})
    if freshness_terms.get("max_age_hours") is not None:
        try:
            from .stats import compute_table_stats
            stats = compute_table_stats(catalog, table_name)
            last_modified = stats.get("last_modified")
            if last_modified:
                age_hours = (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(str(last_modified))).total_seconds() / 3600
                freshness_check = {
                    "max_age_hours": freshness_terms["max_age_hours"],
                    "current_age_hours": round(age_hours, 2),
                    "passing": age_hours <= freshness_terms["max_age_hours"],
                }
        except Exception:
            freshness_check = {"error": "Could not check freshness"}

    return {
        "table": table_name,
        "has_contract": True,
        "version": entry.get("version", 1),
        "status": entry.get("status", "active"),
        "schema_match": schema_match,
        "schema_issues": schema_issues,
        "quality_check": quality_check,
        "freshness_check": freshness_check,
        "constraint_count": len(entry.get("constraints", [])),
        "owner": entry.get("owner", ""),
        "message": f"Contract summary for '{table_name}': schema {'OK' if schema_match else 'DRIFT'}, {len(schema_issues)} issues",
    }


def validate_contract(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Validate current table state against its contract."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return {"table": table_name, "valid": True, "violations": [], "message": f"No contract for '{table_name}' — skipping"}

    violations = []

    # Schema validation
    table = catalog.load_table(table_name)
    actual_schema = {}
    for field in table.schema().fields:
        type_name = type(field.field_type).__name__
        actual_schema[field.name] = {
            "type": TYPE_MAP.get(type_name, "string"),
            "nullable": not field.required,
        }

    contract_schema = entry.get("schema", {})
    for col_name, col_def in contract_schema.items():
        if col_name not in actual_schema:
            violations.append({"type": "schema", "field": col_name, "rule": "exists", "message": f"Missing column: {col_name}"})
        else:
            actual = actual_schema[col_name]
            if col_def.get("type") and actual["type"] != col_def["type"]:
                violations.append({
                    "type": "schema", "field": col_name, "rule": "type",
                    "expected": col_def["type"], "actual": actual["type"],
                    "message": f"Type mismatch on '{col_name}': expected {col_def['type']}, got {actual['type']}",
                })

    # Constraint validation on actual data
    arrow = table.scan().to_arrow()
    if arrow.num_rows > 0:
        import duckdb
        conn = duckdb.connect()
        conn.register("tbl", arrow)

        for constraint in entry.get("constraints", []):
            col = constraint.get("column", "")
            rule = constraint.get("rule", "")
            try:
                cv = _validate_constraint(conn, col, rule, constraint)
                if cv:
                    violations.append(cv)
            except Exception:
                pass

        conn.close()

    valid = len(violations) == 0
    return {
        "table": table_name,
        "valid": valid,
        "violations": violations,
        "violation_count": len(violations),
        "message": f"Contract validation for '{table_name}': {'PASS' if valid else f'FAIL ({len(violations)} violations)'}",
    }


def validate_data_against_contract(
    table_name: str,
    rows: list[dict],
    store_path: Optional[Path] = None,
) -> dict:
    """Validate a batch of rows against the contract before writing."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return {"table": table_name, "valid": True, "violations": [], "message": "No contract — all rows accepted"}

    violations = []
    contract_schema = entry.get("schema", {})
    constraints = entry.get("constraints", [])

    for i, row in enumerate(rows):
        # Schema checks
        for col_name, col_def in contract_schema.items():
            if col_name not in row:
                violations.append({
                    "type": "schema", "field": col_name, "rule": "exists",
                    "row": i, "message": f"Row {i}: missing column '{col_name}'",
                })
            elif row[col_name] is None and col_def.get("nullable") is False:
                violations.append({
                    "type": "schema", "field": col_name, "rule": "nullable",
                    "row": i, "message": f"Row {i}: null value in non-nullable '{col_name}'",
                })

        # Constraint checks
        for constraint in constraints:
            col = constraint.get("column", "")
            rule = constraint.get("rule", "")
            value = row.get(col)

            if rule == "not_null" and value is None:
                violations.append({
                    "type": "constraint", "field": col, "rule": rule,
                    "row": i, "message": f"Row {i}: '{col}' is null (not_null constraint)",
                })
            elif rule == "range" and value is not None:
                min_val = constraint.get("min")
                max_val = constraint.get("max")
                if min_val is not None and value < min_val:
                    violations.append({
                        "type": "constraint", "field": col, "rule": rule,
                        "row": i, "expected": f"{min_val}-{max_val}", "actual": value,
                        "message": f"Row {i}: '{col}' value {value} below min {min_val}",
                    })
                if max_val is not None and value > max_val:
                    violations.append({
                        "type": "constraint", "field": col, "rule": rule,
                        "row": i, "expected": f"{min_val}-{max_val}", "actual": value,
                        "message": f"Row {i}: '{col}' value {value} above max {max_val}",
                    })
            elif rule == "enum" and value is not None:
                allowed = constraint.get("values", [])
                if value not in allowed:
                    violations.append({
                        "type": "constraint", "field": col, "rule": rule,
                        "row": i, "expected": allowed, "actual": value,
                        "message": f"Row {i}: '{col}' value '{value}' not in allowed values",
                    })
            elif rule == "regex" and value is not None:
                import re
                pattern = constraint.get("pattern", "")
                if not re.match(pattern, str(value)):
                    violations.append({
                        "type": "constraint", "field": col, "rule": rule,
                        "row": i, "expected": pattern, "actual": value,
                        "message": f"Row {i}: '{col}' value '{value}' doesn't match pattern '{pattern}'",
                    })

    valid = len(violations) == 0
    return {
        "table": table_name,
        "valid": valid,
        "rows_checked": len(rows),
        "violations": violations,
        "violation_count": len(violations),
        "message": f"Pre-write validation: {'PASS' if valid else f'FAIL ({len(violations)} violations)'} for {len(rows)} rows",
    }


def get_contract_violations(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get current violations for a table."""
    result = validate_contract(catalog, table_name, store_path=store_path)
    summary = get_contract_summary(catalog, table_name, store_path=store_path)

    violations = list(result.get("violations", []))

    # Add quality violation if applicable
    qc = summary.get("quality_check")
    if qc and not qc.get("passing", True) and "error" not in qc:
        violations.append({
            "type": "quality", "field": None, "rule": "min_score",
            "expected": qc["min_score"], "actual": qc["current_score"],
            "message": f"Quality score {qc['current_score']:.0f} below threshold {qc['min_score']}",
        })

    # Add freshness violation if applicable
    fc = summary.get("freshness_check")
    if fc and not fc.get("passing", True) and "error" not in fc:
        violations.append({
            "type": "freshness", "field": None, "rule": "max_age_hours",
            "expected": fc["max_age_hours"], "actual": fc["current_age_hours"],
            "message": f"Table age {fc['current_age_hours']:.1f}h exceeds max {fc['max_age_hours']}h",
        })

    return {
        "table": table_name,
        "violations": violations,
        "violation_count": len(violations),
        "message": f"Violations for '{table_name}': {len(violations)} found",
    }


def _validate_constraint(conn, column: str, rule: str, constraint: dict) -> Optional[dict]:
    """Validate a single constraint against table data in DuckDB."""
    if rule == "not_null":
        count = conn.execute(f'SELECT COUNT(*) FROM tbl WHERE "{column}" IS NULL').fetchone()[0]
        if count > 0:
            return {
                "type": "constraint", "field": column, "rule": "not_null",
                "message": f"'{column}': {count} null values violate not_null constraint",
                "null_count": count,
            }
    elif rule == "range":
        min_val = constraint.get("min")
        max_val = constraint.get("max")
        conditions = []
        if min_val is not None:
            conditions.append(f'"{column}" < {min_val}')
        if max_val is not None:
            conditions.append(f'"{column}" > {max_val}')
        if conditions:
            where = " OR ".join(conditions)
            count = conn.execute(f'SELECT COUNT(*) FROM tbl WHERE {where}').fetchone()[0]
            if count > 0:
                return {
                    "type": "constraint", "field": column, "rule": "range",
                    "expected": f"{min_val}-{max_val}", "violation_count": count,
                    "message": f"'{column}': {count} values outside range [{min_val}, {max_val}]",
                }
    elif rule == "enum":
        allowed = constraint.get("values", [])
        if allowed:
            placeholders = ", ".join(f"'{v}'" for v in allowed)
            count = conn.execute(f'SELECT COUNT(*) FROM tbl WHERE "{column}" NOT IN ({placeholders})').fetchone()[0]
            if count > 0:
                return {
                    "type": "constraint", "field": column, "rule": "enum",
                    "expected": allowed, "violation_count": count,
                    "message": f"'{column}': {count} values not in allowed set",
                }
    return None


def get_contract_history(
    table_name: str,
    limit: int = 20,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Get version history of a contract."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return []

    history = entry.get("_history", [])
    # Return most recent first
    result = list(reversed(history))[:limit]
    return result


def get_contract_version(
    table_name: str,
    version: int,
    store_path: Optional[Path] = None,
) -> Optional[dict]:
    """Get a specific historical version of a contract."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return None

    # Current version
    current_version = entry.get("version", 1)
    if version == current_version:
        return {k: v for k, v in entry.items() if not k.startswith("_")}

    # Search history
    for snapshot in entry.get("_history", []):
        if snapshot.get("version") == version:
            return snapshot

    return None


def diff_contract_versions(
    table_name: str,
    v1: int,
    v2: int,
    store_path: Optional[Path] = None,
) -> dict:
    """Diff two contract versions showing added/removed/changed fields."""
    table_name = _normalize(table_name)

    ver1 = get_contract_version(table_name, v1, store_path=store_path)
    ver2 = get_contract_version(table_name, v2, store_path=store_path)

    if ver1 is None:
        return {"error": f"Version {v1} not found for '{table_name}'"}
    if ver2 is None:
        return {"error": f"Version {v2} not found for '{table_name}'"}

    changes = []
    compare_fields = {"schema", "quality", "freshness", "constraints", "owner", "description"}

    for field in compare_fields:
        old_val = ver1.get(field)
        new_val = ver2.get(field)
        if old_val != new_val:
            changes.append({
                "field": field,
                "from": old_val,
                "to": new_val,
            })

    # Schema-level detail: added/removed columns
    schema_added = []
    schema_removed = []
    schema_changed = []
    s1 = ver1.get("schema", {})
    s2 = ver2.get("schema", {})
    for col in set(s2) - set(s1):
        schema_added.append(col)
    for col in set(s1) - set(s2):
        schema_removed.append(col)
    for col in set(s1) & set(s2):
        if s1[col] != s2[col]:
            schema_changed.append({"column": col, "from": s1[col], "to": s2[col]})

    return {
        "table": table_name,
        "v1": v1,
        "v2": v2,
        "changes": changes,
        "schema_added": schema_added,
        "schema_removed": schema_removed,
        "schema_changed": schema_changed,
        "change_count": len(changes),
        "message": f"Diff v{v1} → v{v2}: {len(changes)} field(s) changed",
    }


def deprecate_contract(
    table_name: str,
    reason: str,
    sunset_date: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Mark a contract as deprecated."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store:
        raise ValueError(f"No contract found for '{table_name}'")

    entry = store[table_name]
    entry["status"] = "deprecated"
    entry["deprecation_reason"] = reason
    if sunset_date:
        entry["sunset_date"] = sunset_date
    entry["deprecated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    _save_store(store, store_path)

    return {
        "table": table_name,
        "status": "deprecated",
        "reason": reason,
        "sunset_date": sunset_date,
        "message": f"Contract for '{table_name}' marked as deprecated",
    }


def get_contract_status(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get lifecycle status of a contract."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return {"table": table_name, "status": "not_found", "message": f"No contract for '{table_name}'"}

    status = entry.get("status", "active")
    result = {
        "table": table_name,
        "status": status,
        "version": entry.get("version", 1),
        "created_at": entry.get("created_at", ""),
        "updated_at": entry.get("updated_at", ""),
    }

    if status == "deprecated":
        result["deprecation_reason"] = entry.get("deprecation_reason", "")
        result["sunset_date"] = entry.get("sunset_date")
        result["deprecated_at"] = entry.get("deprecated_at", "")

    return result


MAX_COMPLIANCE_HISTORY = 100


def monitor_contract(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
    notification_store_path: Optional[Path] = None,
) -> dict:
    """Run a full compliance check and record the result."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return {"table": table_name, "checked": False, "message": f"No contract for '{table_name}'"}

    # Run full validation
    validation = validate_contract(catalog, table_name, store_path=store_path)
    summary = get_contract_summary(catalog, table_name, store_path=store_path)

    violations = list(validation.get("violations", []))

    # Quality check
    qc = summary.get("quality_check")
    quality_passing = True
    if qc and "error" not in qc and not qc.get("passing", True):
        quality_passing = False
        violations.append({
            "type": "quality", "rule": "min_score",
            "message": f"Quality score {qc['current_score']:.0f} below threshold {qc['min_score']}",
        })

    # Freshness check
    fc = summary.get("freshness_check")
    freshness_passing = True
    if fc and "error" not in fc and not fc.get("passing", True):
        freshness_passing = False
        violations.append({
            "type": "freshness", "rule": "max_age_hours",
            "message": f"Table age {fc['current_age_hours']:.1f}h exceeds max {fc['max_age_hours']}h",
        })

    passed = len(violations) == 0

    # Record in compliance history
    record = {
        "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "passed": passed,
        "violation_count": len(violations),
        "violations": violations,
    }
    entry.setdefault("_compliance_history", []).append(record)
    entry["_compliance_history"] = entry["_compliance_history"][-MAX_COMPLIANCE_HISTORY:]
    _save_store(store, store_path)

    # Fire notification event on violations
    if not passed:
        try:
            from .notifications import fire_event
            fire_event(
                table_name, "contract_violation",
                {"violation_count": len(violations), "violations": violations[:5]},
                store_path=notification_store_path,
            )
        except Exception:
            pass  # Notifications are best-effort

    return {
        "table": table_name,
        "checked": True,
        "passed": passed,
        "violation_count": len(violations),
        "violations": violations,
        "message": f"Compliance check for '{table_name}': {'PASS' if passed else f'FAIL ({len(violations)} violations)'}",
    }


def get_compliance_history(
    table_name: str,
    limit: int = 20,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Get history of compliance check results."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return []

    history = entry.get("_compliance_history", [])
    return list(reversed(history))[:limit]


def get_compliance_score(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Compute a 0-100 compliance score."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return {"table": table_name, "score": None, "message": f"No contract for '{table_name}'"}

    # Schema match ratio
    contract_schema = entry.get("schema", {})
    if contract_schema:
        table = catalog.load_table(table_name)
        actual_cols = {f.name for f in table.schema().fields}
        actual_types = {}
        for f in table.schema().fields:
            actual_types[f.name] = TYPE_MAP.get(type(f.field_type).__name__, "string")

        matching = 0
        for col, col_def in contract_schema.items():
            if col in actual_cols:
                if not col_def.get("type") or actual_types.get(col) == col_def["type"]:
                    matching += 1
        schema_ratio = matching / len(contract_schema) if contract_schema else 1.0
    else:
        schema_ratio = 1.0

    # Constraint pass ratio
    constraints = entry.get("constraints", [])
    if constraints:
        validation = validate_contract(catalog, table_name, store_path=store_path)
        constraint_violations = [v for v in validation.get("violations", []) if v.get("type") == "constraint"]
        constraint_ratio = max(0.0, 1.0 - len(constraint_violations) / len(constraints))
    else:
        constraint_ratio = 1.0

    # Quality ratio
    quality_config = entry.get("quality", {})
    if quality_config.get("min_score"):
        try:
            from .quality import compute_quality_score
            qr = compute_quality_score(catalog, table_name)
            current_score = qr.get("score", 100)
            quality_ratio = min(1.0, current_score / quality_config["min_score"])
        except Exception:
            quality_ratio = 1.0
    else:
        quality_ratio = 1.0

    # Freshness ratio
    freshness_config = entry.get("freshness", {})
    if freshness_config.get("max_age_hours"):
        try:
            from .sla import check_sla
            sla_result = check_sla(catalog, table_name)
            age_hours = sla_result.get("age_hours", 0)
            max_hours = freshness_config["max_age_hours"]
            freshness_ratio = min(1.0, max(0.0, 1.0 - (age_hours - max_hours) / max_hours)) if age_hours > max_hours else 1.0
        except Exception:
            freshness_ratio = 1.0
    else:
        freshness_ratio = 1.0

    score = (schema_ratio * 0.3 + constraint_ratio * 0.3 + quality_ratio * 0.2 + freshness_ratio * 0.2) * 100
    score = round(score, 1)

    return {
        "table": table_name,
        "score": score,
        "schema_ratio": round(schema_ratio, 3),
        "constraint_ratio": round(constraint_ratio, 3),
        "quality_ratio": round(quality_ratio, 3),
        "freshness_ratio": round(freshness_ratio, 3),
        "message": f"Compliance score for '{table_name}': {score}/100",
    }


def add_consumer(
    table_name: str,
    consumer_name: str,
    contact: Optional[str] = None,
    usage: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Register a consumer of a table's contract."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store:
        raise ValueError(f"No contract found for '{table_name}'")

    entry = store[table_name]
    consumers = entry.setdefault("consumers", [])

    # Check for duplicates
    if any(c["name"] == consumer_name for c in consumers):
        return {"table": table_name, "message": f"Consumer '{consumer_name}' already registered"}

    consumer = {"name": consumer_name, "added_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    if contact:
        consumer["contact"] = contact
    if usage:
        consumer["usage"] = usage
    consumers.append(consumer)

    _save_store(store, store_path)
    return {"table": table_name, "consumer": consumer_name, "message": f"Consumer '{consumer_name}' registered for '{table_name}'"}


def add_producer(
    table_name: str,
    producer_name: str,
    contact: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Register the producer (data owner) for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store:
        raise ValueError(f"No contract found for '{table_name}'")

    entry = store[table_name]
    producer = {"name": producer_name, "added_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    if contact:
        producer["contact"] = contact
    entry["producer"] = producer

    _save_store(store, store_path)
    return {"table": table_name, "producer": producer_name, "message": f"Producer '{producer_name}' set for '{table_name}'"}


def list_consumers(
    table_name: str,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all registered consumers for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return []

    return entry.get("consumers", [])


def list_producers(
    table_name: str,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List the producer(s) for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    entry = store.get(table_name)

    if entry is None:
        return []

    producer = entry.get("producer")
    return [producer] if producer else []


def remove_consumer(
    table_name: str,
    consumer_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a consumer from a table's contract."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store:
        return {"table": table_name, "message": f"No contract found for '{table_name}'"}

    entry = store[table_name]
    consumers = entry.get("consumers", [])
    original_count = len(consumers)
    entry["consumers"] = [c for c in consumers if c["name"] != consumer_name]

    if len(entry["consumers"]) == original_count:
        return {"table": table_name, "message": f"Consumer '{consumer_name}' not found"}

    _save_store(store, store_path)
    return {"table": table_name, "message": f"Consumer '{consumer_name}' removed from '{table_name}'"}


def get_contract_coverage(
    catalog,
    store_path: Optional[Path] = None,
) -> dict:
    """Scan all tables and report contract coverage."""
    from .catalog import list_tables

    all_tables = list_tables(catalog, namespace="*")
    store = _load_store(store_path)

    contracted = []
    uncovered = []

    for t in all_tables:
        if t in store:
            contracted.append(t)
        else:
            uncovered.append(t)

    total = len(all_tables)
    coverage_pct = round((len(contracted) / total * 100), 1) if total > 0 else 0.0

    return {
        "total_tables": total,
        "contracted": len(contracted),
        "uncovered_count": len(uncovered),
        "coverage_pct": coverage_pct,
        "contracted_tables": contracted,
        "uncovered_tables": uncovered,
        "message": f"Contract coverage: {len(contracted)}/{total} tables ({coverage_pct}%)",
    }


ENUM_MAX_DISTINCT = 20


def generate_contract(
    catalog,
    table_name: str,
    strict: bool = False,
    store_path: Optional[Path] = None,
) -> dict:
    """Analyze a table and generate a draft contract, saving it."""
    contract = preview_contract(catalog, table_name, strict=strict)
    create_contract(table_name, contract, store_path=store_path)
    return {"table": _normalize(table_name), "contract": contract, "message": f"Generated contract for '{_normalize(table_name)}'"}


def preview_contract(
    catalog,
    table_name: str,
    strict: bool = False,
) -> dict:
    """Generate a draft contract without saving it."""
    from .catalog import profile_table

    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)
    profile = profile_table(catalog, table_name)
    row_count = profile.get("row_count", 0)

    # Infer schema
    schema = {}
    for field in table.schema().fields:
        type_name = TYPE_MAP.get(type(field.field_type).__name__, "string")
        col_stats = profile.get("columns", {}).get(field.name, {})
        null_count = col_stats.get("nulls", 0)

        if strict:
            nullable = null_count > 0 if row_count > 0 else True
        else:
            nullable = not field.required

        schema[field.name] = {"type": type_name, "nullable": nullable}

    # Infer constraints
    constraints = []
    for field in table.schema().fields:
        col_stats = profile.get("columns", {}).get(field.name, {})
        null_count = col_stats.get("nulls", 0)
        unique_count = col_stats.get("unique", 0)
        col_type = col_stats.get("type", "")

        # Not-null constraint
        if row_count > 0 and null_count == 0:
            constraints.append({"column": field.name, "rule": "not_null"})
        elif strict and row_count > 0 and null_count / row_count < 0.01:
            constraints.append({"column": field.name, "rule": "not_null"})

        # Range constraint for numeric
        if col_type in ("long", "double", "int", "float") and "min" in col_stats and "max" in col_stats:
            min_val = col_stats["min"]
            max_val = col_stats["max"]
            if min_val is not None and max_val is not None:
                if strict:
                    constraints.append({"column": field.name, "rule": "range", "min": min_val, "max": max_val})
                else:
                    # Add 10% buffer
                    span = max_val - min_val if max_val != min_val else abs(max_val) * 0.1 or 1
                    constraints.append({
                        "column": field.name, "rule": "range",
                        "min": round(min_val - span * 0.1, 4),
                        "max": round(max_val + span * 0.1, 4),
                    })

        # Enum constraint for low-cardinality strings
        if col_type == "string" and 0 < unique_count <= ENUM_MAX_DISTINCT:
            top_values = col_stats.get("top_values", {})
            if top_values:
                constraints.append({"column": field.name, "rule": "enum", "values": list(top_values.keys())})

    contract = {
        "schema": schema,
        "constraints": constraints,
        "description": f"Auto-generated contract for '{table_name}'",
    }

    # Quality baseline
    try:
        from .quality import compute_quality_score
        qr = compute_quality_score(catalog, table_name)
        score = qr.get("score", 100)
        threshold = int(score + 5) if strict else max(int(score - 10), 0)
        contract["quality"] = {"min_score": threshold}
    except Exception:
        pass

    return contract


def apply_generated_contract(
    catalog,
    table_name: str,
    contract: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Apply a generated/modified contract (create or update)."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name in store:
        # Update existing
        updatable = {k: v for k, v in contract.items() if k in {"schema", "quality", "freshness", "constraints", "owner", "description"}}
        return update_contract(table_name, updatable, store_path=store_path)
    else:
        return create_contract(table_name, contract, store_path=store_path)


def dry_run_contract(
    catalog,
    table_name: str,
    contract: dict,
) -> dict:
    """Test a proposed contract against existing table data without saving."""
    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)

    violations = []

    # Schema checks
    actual_schema = {}
    for field in table.schema().fields:
        type_name = TYPE_MAP.get(type(field.field_type).__name__, "string")
        actual_schema[field.name] = {"type": type_name, "nullable": not field.required}

    contract_schema = contract.get("schema", {})
    for col_name, col_def in contract_schema.items():
        if col_name not in actual_schema:
            violations.append({"type": "schema", "field": col_name, "rule": "exists", "message": f"Missing column: {col_name}"})
        elif col_def.get("type") and actual_schema[col_name]["type"] != col_def["type"]:
            violations.append({
                "type": "schema", "field": col_name, "rule": "type",
                "message": f"Type mismatch on '{col_name}': expected {col_def['type']}, got {actual_schema[col_name]['type']}",
            })

    # Constraint checks on data
    arrow = table.scan().to_arrow()
    if arrow.num_rows > 0:
        import duckdb
        conn = duckdb.connect()
        conn.register("tbl", arrow)

        for constraint in contract.get("constraints", []):
            col = constraint.get("column", "")
            rule = constraint.get("rule", "")
            try:
                cv = _validate_constraint(conn, col, rule, constraint)
                if cv:
                    violations.append(cv)
            except Exception:
                pass

        conn.close()

    valid = len(violations) == 0
    return {
        "table": table_name,
        "valid": valid,
        "violations": violations,
        "violation_count": len(violations),
        "message": f"Contract test for '{table_name}': {'PASS' if valid else f'FAIL ({len(violations)} violations)'}",
    }


def dry_run_migration(
    catalog,
    table_name: str,
    to_contract: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Simulate migration from current contract to a new one."""
    table_name = _normalize(table_name)

    # Get current violations
    current_result = validate_contract(catalog, table_name, store_path=store_path)
    current_violations = current_result.get("violations", [])

    # Get new violations
    new_result = dry_run_contract(catalog, table_name, to_contract)
    new_violations = new_result.get("violations", [])

    # Diff: find new violations (in new but not in current)
    current_keys = {(v.get("field", ""), v.get("rule", "")) for v in current_violations}
    new_keys = {(v.get("field", ""), v.get("rule", "")) for v in new_violations}

    introduced = [v for v in new_violations if (v.get("field", ""), v.get("rule", "")) not in current_keys]
    resolved = [v for v in current_violations if (v.get("field", ""), v.get("rule", "")) not in new_keys]

    return {
        "table": table_name,
        "current_violations": len(current_violations),
        "new_violations": len(new_violations),
        "introduced": introduced,
        "introduced_count": len(introduced),
        "resolved": resolved,
        "resolved_count": len(resolved),
        "safe_to_migrate": len(introduced) == 0,
        "message": f"Migration test: {len(introduced)} new violations, {len(resolved)} resolved",
    }


def dry_run_report(
    catalog,
    table_name: str,
    contract: dict,
) -> dict:
    """Comprehensive test report with per-column pass rates."""
    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()
    row_count = arrow.num_rows

    # Schema compatibility
    actual_schema = {}
    for field in table.schema().fields:
        type_name = TYPE_MAP.get(type(field.field_type).__name__, "string")
        actual_schema[field.name] = {"type": type_name, "nullable": not field.required}

    contract_schema = contract.get("schema", {})
    schema_issues = []
    for col_name, col_def in contract_schema.items():
        if col_name not in actual_schema:
            schema_issues.append(f"Missing column: {col_name}")
        elif col_def.get("type") and actual_schema[col_name]["type"] != col_def["type"]:
            schema_issues.append(f"Type mismatch on '{col_name}'")

    schema_compatible = len(schema_issues) == 0

    # Per-constraint pass rates
    constraint_results = []
    if row_count > 0:
        import duckdb
        conn = duckdb.connect()
        conn.register("tbl", arrow)

        for constraint in contract.get("constraints", []):
            col = constraint.get("column", "")
            rule = constraint.get("rule", "")
            cv = None
            try:
                cv = _validate_constraint(conn, col, rule, constraint)
            except Exception:
                pass

            if cv:
                violation_count = cv.get("null_count") or cv.get("violation_count") or 1
                pass_rate = round((1 - violation_count / row_count) * 100, 1) if row_count > 0 else 100.0
                constraint_results.append({
                    "column": col, "rule": rule,
                    "pass_rate": pass_rate, "violations": violation_count,
                })
            else:
                constraint_results.append({
                    "column": col, "rule": rule,
                    "pass_rate": 100.0, "violations": 0,
                })

        conn.close()

    overall_pass = schema_compatible and all(cr["violations"] == 0 for cr in constraint_results)

    return {
        "table": table_name,
        "row_count": row_count,
        "schema_compatible": schema_compatible,
        "schema_issues": schema_issues,
        "constraint_results": constraint_results,
        "overall_pass": overall_pass,
        "message": f"Test report for '{table_name}': {'PASS' if overall_pass else 'FAIL'}",
    }
