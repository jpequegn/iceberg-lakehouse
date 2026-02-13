"""Data validation rules for lakehouse tables."""

import datetime
import json
import re
import uuid
from pathlib import Path
from typing import Optional


DEFAULT_VALIDATION_PATH = Path.home() / ".lakehouse" / "validation.json"


class ValidationError(Exception):
    """Raised when data fails validation."""

    def __init__(self, failures: list[dict]):
        self.failures = failures
        messages = [f["message"] for f in failures]
        super().__init__(f"Validation failed: {'; '.join(messages)}")


def _load_rules(store_path: Optional[Path] = None) -> dict:
    """Load validation rules from disk."""
    path = store_path or DEFAULT_VALIDATION_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_rules(data: dict, store_path: Optional[Path] = None) -> None:
    """Save validation rules to disk."""
    path = store_path or DEFAULT_VALIDATION_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def add_validation_rule(
    table_name: str,
    rule: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Add a validation rule to a table.

    Args:
        table_name: Table name
        rule: Rule dict with 'type' and type-specific fields

    Returns:
        Dict with rule details including generated ID
    """
    rule_type = rule.get("type")
    valid_types = {"not_null", "unique", "range", "regex", "expression"}
    if rule_type not in valid_types:
        raise ValueError(f"Invalid rule type '{rule_type}'. Must be one of: {', '.join(sorted(valid_types))}")

    # Validate rule-specific fields
    if rule_type == "not_null":
        if not rule.get("column"):
            raise ValueError("not_null rule requires 'column'")
    elif rule_type == "unique":
        cols = rule.get("columns")
        if not cols or not isinstance(cols, list):
            raise ValueError("unique rule requires 'columns' (list)")
    elif rule_type == "range":
        if not rule.get("column"):
            raise ValueError("range rule requires 'column'")
        if "min" not in rule and "max" not in rule:
            raise ValueError("range rule requires at least 'min' or 'max'")
    elif rule_type == "regex":
        if not rule.get("column"):
            raise ValueError("regex rule requires 'column'")
        if not rule.get("pattern"):
            raise ValueError("regex rule requires 'pattern'")
        # Validate pattern compiles
        try:
            re.compile(rule["pattern"])
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
    elif rule_type == "expression":
        if not rule.get("sql"):
            raise ValueError("expression rule requires 'sql'")

    rule_id = uuid.uuid4().hex[:8]
    stored_rule = {
        "id": rule_id,
        **rule,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    store = _load_rules(store_path)
    store.setdefault(table_name, []).append(stored_rule)
    _save_rules(store, store_path)

    return {
        **stored_rule,
        "message": f"Added {rule_type} rule '{rule_id}' to {table_name}",
    }


def list_validation_rules(
    table_name: str,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all validation rules for a table."""
    store = _load_rules(store_path)
    return store.get(table_name, [])


def remove_validation_rule(
    table_name: str,
    rule_id: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a validation rule by ID."""
    store = _load_rules(store_path)
    rules = store.get(table_name, [])

    for i, rule in enumerate(rules):
        if rule["id"] == rule_id:
            removed = rules.pop(i)
            if not rules:
                del store[table_name]
            _save_rules(store, store_path)
            return {
                "id": rule_id,
                "type": removed["type"],
                "message": f"Removed rule '{rule_id}' from {table_name}",
            }

    raise ValueError(f"Rule '{rule_id}' not found for table '{table_name}'")


def validate_rows(
    rows: list[dict],
    rules: list[dict],
    existing_data: Optional[list[dict]] = None,
) -> dict:
    """Validate rows against a list of rules.

    Args:
        rows: Rows to validate
        rules: Validation rules to check
        existing_data: Existing table rows (needed for unique checks)

    Returns:
        Dict with 'valid' bool, 'failures' list, and 'checked' count
    """
    if not rules:
        return {"valid": True, "failures": [], "checked": len(rows)}

    failures = []

    for rule in rules:
        rule_type = rule["type"]

        if rule_type == "not_null":
            col = rule["column"]
            for i, row in enumerate(rows):
                val = row.get(col)
                if val is None:
                    failures.append({
                        "rule_id": rule["id"],
                        "rule_type": "not_null",
                        "row_index": i,
                        "column": col,
                        "message": f"Column '{col}' must not be null (row {i})",
                    })

        elif rule_type == "range":
            col = rule["column"]
            min_val = rule.get("min")
            max_val = rule.get("max")
            for i, row in enumerate(rows):
                val = row.get(col)
                if val is None:
                    continue
                try:
                    num_val = float(val)
                except (TypeError, ValueError):
                    continue
                if min_val is not None and num_val < float(min_val):
                    failures.append({
                        "rule_id": rule["id"],
                        "rule_type": "range",
                        "row_index": i,
                        "column": col,
                        "message": f"Column '{col}' value {num_val} is below minimum {min_val} (row {i})",
                    })
                if max_val is not None and num_val > float(max_val):
                    failures.append({
                        "rule_id": rule["id"],
                        "rule_type": "range",
                        "row_index": i,
                        "column": col,
                        "message": f"Column '{col}' value {num_val} is above maximum {max_val} (row {i})",
                    })

        elif rule_type == "regex":
            col = rule["column"]
            pattern = re.compile(rule["pattern"])
            for i, row in enumerate(rows):
                val = row.get(col)
                if val is None:
                    continue
                if not pattern.match(str(val)):
                    failures.append({
                        "rule_id": rule["id"],
                        "rule_type": "regex",
                        "row_index": i,
                        "column": col,
                        "message": f"Column '{col}' value '{val}' does not match pattern '{rule['pattern']}' (row {i})",
                    })

        elif rule_type == "expression":
            import duckdb
            sql_expr = rule["sql"]
            conn = duckdb.connect()
            try:
                conn.register("candidate", _rows_to_duckdb(rows))
                failing = conn.execute(
                    f"SELECT rowid FROM (SELECT row_number() OVER () - 1 AS rowid, * FROM candidate) WHERE NOT ({sql_expr})"
                ).fetchall()
                for (row_idx,) in failing:
                    failures.append({
                        "rule_id": rule["id"],
                        "rule_type": "expression",
                        "row_index": int(row_idx),
                        "message": f"Row {int(row_idx)} failed expression: {sql_expr}",
                    })
            except Exception as e:
                failures.append({
                    "rule_id": rule["id"],
                    "rule_type": "expression",
                    "row_index": -1,
                    "message": f"Expression rule error: {e}",
                })
            finally:
                conn.close()

        elif rule_type == "unique":
            cols = rule["columns"]
            # Check within the new rows
            seen = {}
            for i, row in enumerate(rows):
                key = tuple(row.get(c) for c in cols)
                if key in seen:
                    failures.append({
                        "rule_id": rule["id"],
                        "rule_type": "unique",
                        "row_index": i,
                        "columns": cols,
                        "message": f"Duplicate value for columns {cols} at row {i} (same as row {seen[key]})",
                    })
                else:
                    seen[key] = i

            # Check against existing data
            if existing_data:
                existing_keys = {tuple(row.get(c) for c in cols) for row in existing_data}
                for i, row in enumerate(rows):
                    key = tuple(row.get(c) for c in cols)
                    if key in existing_keys:
                        failures.append({
                            "rule_id": rule["id"],
                            "rule_type": "unique",
                            "row_index": i,
                            "columns": cols,
                            "message": f"Value for columns {cols} at row {i} already exists in table",
                        })

    return {
        "valid": len(failures) == 0,
        "failures": failures,
        "checked": len(rows),
    }


def _rows_to_duckdb(rows: list[dict]):
    """Convert row dicts to a format DuckDB can consume."""
    import pyarrow as pa

    if not rows:
        return pa.table({})

    # Collect all columns
    columns = {}
    for row in rows:
        for k, v in row.items():
            if k not in columns:
                columns[k] = []

    for row in rows:
        for k in columns:
            columns[k].append(row.get(k))

    return pa.table(columns)
