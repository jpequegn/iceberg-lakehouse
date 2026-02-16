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
