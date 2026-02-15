"""Data masking policies — column-level redaction and hashing at query time."""

import datetime
import hashlib
import json
from pathlib import Path
from typing import Optional

import pandas as pd

DEFAULT_MASKING_PATH = Path.home() / ".lakehouse" / "masking.json"

VALID_STRATEGIES = {"hash", "redact", "nullify", "truncate", "expression"}


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_MASKING_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_MASKING_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def add_masking_policy(
    table_name: str,
    column_name: str,
    strategy: str,
    options: Optional[dict] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Add a masking policy to a column.

    Args:
        table_name: Table name
        column_name: Column to mask
        strategy: One of 'hash', 'redact', 'nullify', 'truncate', 'expression'
        options: Strategy-specific options
    """
    if strategy not in VALID_STRATEGIES:
        raise ValueError(
            f"Invalid strategy '{strategy}'. "
            f"Must be one of: {', '.join(sorted(VALID_STRATEGIES))}"
        )

    if strategy == "expression" and (not options or "sql" not in options):
        raise ValueError("Expression strategy requires 'sql' in options")

    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store:
        store[table_name] = {}

    if column_name in store[table_name]:
        raise ValueError(
            f"Masking policy already exists for '{table_name}.{column_name}'. "
            "Remove it first to change the policy."
        )

    store[table_name][column_name] = {
        "strategy": strategy,
        "options": options or {},
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    _save_store(store, store_path)

    return {
        "table": table_name,
        "column": column_name,
        "strategy": strategy,
        "options": options or {},
        "message": f"Masking policy '{strategy}' added for '{table_name}.{column_name}'",
    }


def list_masking_policies(
    table_name: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List masking policies, optionally filtered by table."""
    store = _load_store(store_path)
    results = []

    tables = store
    if table_name:
        table_name = _normalize(table_name)
        tables = {table_name: store.get(table_name, {})}

    for tbl, columns in tables.items():
        for col, policy in columns.items():
            results.append({
                "table": tbl,
                "column": col,
                "strategy": policy["strategy"],
                "options": policy.get("options", {}),
            })

    return results


def remove_masking_policy(
    table_name: str,
    column_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a masking policy from a column."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name in store and column_name in store[table_name]:
        del store[table_name][column_name]
        if not store[table_name]:
            del store[table_name]
        _save_store(store, store_path)
        return {"table": table_name, "column": column_name, "message": f"Masking policy removed for '{table_name}.{column_name}'"}

    return {"table": table_name, "column": column_name, "message": f"No masking policy found for '{table_name}.{column_name}'"}


def _apply_mask(value, strategy: str, options: dict):
    """Apply a masking strategy to a single value."""
    if value is None or pd.isna(value):
        return value

    if strategy == "hash":
        return hashlib.sha256(str(value).encode()).hexdigest()[:16]

    elif strategy == "redact":
        return options.get("replacement", "***")

    elif strategy == "nullify":
        return None

    elif strategy == "truncate":
        length = options.get("length", 3)
        s = str(value)
        if len(s) <= length:
            return s
        return s[:length] + "***"

    return value


def query_with_masking(
    engine,
    sql: str,
    store_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Execute a query with masking policies applied to result columns."""
    df = engine.execute(sql)
    store = _load_store(store_path)

    # Build a flat lookup of column -> (strategy, options) across all tables
    column_policies = {}
    for tbl, columns in store.items():
        for col, policy in columns.items():
            column_policies[col] = (policy["strategy"], policy.get("options", {}))

    # Apply masking to matching columns
    for col in df.columns:
        if col in column_policies:
            strategy, options = column_policies[col]
            if strategy == "expression":
                # Expression masking handled via DuckDB
                try:
                    import duckdb
                    conn = duckdb.connect(":memory:")
                    conn.register("_mask_input", df)
                    sql_expr = options["sql"].replace("col", col)
                    result = conn.execute(
                        f"SELECT *, ({sql_expr}) AS _masked FROM _mask_input"
                    ).fetchdf()
                    df[col] = result["_masked"]
                    conn.close()
                except Exception:
                    df[col] = df[col].apply(lambda v: _apply_mask(v, "redact", {}))
            else:
                df[col] = df[col].apply(lambda v, s=strategy, o=options: _apply_mask(v, s, o))

    return df


def preview_masking(
    catalog,
    table_name: str,
    max_rows: int = 5,
    store_path: Optional[Path] = None,
) -> dict:
    """Preview a table with all masking policies applied."""
    table_name = _normalize(table_name)

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    arrow = table.scan().to_arrow()
    df = arrow.to_pandas().head(max_rows)

    original = df.copy()

    store = _load_store(store_path)
    policies = store.get(table_name, {})

    for col, policy in policies.items():
        if col in df.columns:
            strategy = policy["strategy"]
            options = policy.get("options", {})
            if strategy == "expression":
                try:
                    import duckdb
                    conn = duckdb.connect(":memory:")
                    conn.register("_mask_input", df)
                    sql_expr = options["sql"].replace("col", col)
                    result = conn.execute(
                        f"SELECT *, ({sql_expr}) AS _masked FROM _mask_input"
                    ).fetchdf()
                    df[col] = result["_masked"]
                    conn.close()
                except Exception:
                    df[col] = df[col].apply(lambda v: _apply_mask(v, "redact", {}))
            else:
                df[col] = df[col].apply(lambda v, s=strategy, o=options: _apply_mask(v, s, o))

    return {
        "table": table_name,
        "rows": max_rows,
        "original": original.to_dict(orient="records"),
        "masked": df.to_dict(orient="records"),
        "policies_applied": len(policies),
        "message": f"Preview of '{table_name}' with {len(policies)} masking policy/policies applied",
    }
