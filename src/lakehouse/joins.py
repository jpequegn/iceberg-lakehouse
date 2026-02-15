"""Cross-table joins and federated queries."""

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import pyarrow as pa

from .catalog import list_tables, get_table_schema, list_namespaces


def _register_all_tables(catalog, conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Register all catalog tables in DuckDB with both short and qualified names.

    Returns list of registered qualified table names.
    """
    registered = []
    for namespace in catalog.list_namespaces():
        ns_name = namespace[0] if isinstance(namespace, tuple) else namespace
        for table_id in catalog.list_tables(ns_name):
            table_name = table_id[1] if isinstance(table_id, tuple) else str(table_id)
            full_name = f"{ns_name}.{table_name}"

            try:
                table = catalog.load_table(full_name)
                arrow_table = table.scan().to_arrow()

                # Register with short name (for backward compat)
                conn.register(table_name, arrow_table)

                # Register with underscore-separated qualified name
                # DuckDB doesn't support dots in table names, use underscore
                qualified_alias = f"{ns_name}__{table_name}"
                conn.register(qualified_alias, arrow_table)

                registered.append(full_name)
            except Exception:
                pass

    return registered


def _resolve_namespace_refs(sql: str, catalog) -> str:
    """Replace namespace.table references with underscore-separated aliases.

    Converts 'default.expenses' → 'default__expenses' in SQL so DuckDB can resolve them.
    """
    all_tables = list_tables(catalog, namespace="*")
    for full_name in sorted(all_tables, key=len, reverse=True):
        # Replace namespace.table with namespace__table
        ns, tbl = full_name.split(".", 1)
        alias = f"{ns}__{tbl}"
        sql = sql.replace(full_name, alias)
    return sql


def execute_join(
    catalog,
    sql: str,
    engine=None,
    max_rows: int = 1000,
) -> dict:
    """Execute a cross-table join query with namespace-aware table resolution.

    Args:
        catalog: Iceberg catalog
        sql: SQL with optional namespace-qualified table references
        engine: Unused (kept for API compatibility)
        max_rows: Maximum rows to return

    Returns:
        Dict with columns, rows, row_count, and dataframe.
    """
    conn = duckdb.connect(":memory:")
    try:
        registered = _register_all_tables(catalog, conn)
        resolved_sql = _resolve_namespace_refs(sql, catalog)

        result = conn.execute(resolved_sql).fetchdf()
        if len(result) > max_rows:
            result = result.head(max_rows)

        return {
            "columns": list(result.columns),
            "row_count": len(result),
            "dataframe": result,
            "registered_tables": registered,
        }
    except Exception as e:
        raise ValueError(f"Join query failed: {e}")
    finally:
        conn.close()


def join_to_table(
    catalog,
    sql: str,
    target_table: str,
    engine=None,
    mode: str = "overwrite",
) -> dict:
    """Execute a join and write results to a new or existing table.

    Args:
        catalog: Iceberg catalog
        sql: Join SQL
        target_table: Name for the result table
        engine: Unused (kept for API compatibility)
        mode: 'overwrite' or 'append'

    Returns:
        Dict with rows written and target table info.
    """
    from .catalog import create_table, insert_rows, delete_rows

    if "." not in target_table:
        target_table = f"default.{target_table}"

    # Execute the join
    result = execute_join(catalog, sql)
    df = result["dataframe"]

    if df.empty:
        return {
            "target": target_table,
            "rows_written": 0,
            "mode": mode,
            "message": f"No rows to write to {target_table}",
        }

    # Infer column types from DataFrame
    type_map = {
        "int64": "long",
        "float64": "double",
        "object": "string",
        "bool": "boolean",
        "datetime64[ns]": "timestamp",
        "datetime64[us]": "timestamp",
    }
    columns = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        columns[col] = type_map.get(dtype, "string")

    # Create or prepare table
    try:
        catalog.load_table(target_table)
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        create_table(catalog, target_table, columns)
    elif mode == "overwrite":
        try:
            delete_rows(catalog, target_table, "1=1")
        except Exception:
            pass

    # Convert DataFrame to list of dicts and insert
    rows = df.to_dict(orient="records")
    insert_rows(catalog, target_table, rows)

    # Record lineage if available
    try:
        from .lineage import record_lineage
        # Extract source tables from SQL (simple heuristic)
        all_tables = list_tables(catalog, namespace="*")
        sources = []
        sql_lower = sql.lower()
        for tbl in all_tables:
            short = tbl.split(".")[-1]
            if short.lower() in sql_lower or tbl.lower() in sql_lower:
                if tbl != target_table:
                    sources.append(tbl)
        if sources:
            record_lineage(sources, target_table, operation="join", sql=sql)
    except Exception:
        pass  # Lineage is best-effort

    return {
        "target": target_table,
        "rows_written": len(rows),
        "columns": list(df.columns),
        "mode": mode,
        "message": f"Wrote {len(rows)} rows to {target_table} ({mode})",
    }


def suggest_joins(
    catalog,
    table_name: str,
) -> list[dict]:
    """Suggest possible joins by finding columns with matching names across tables.

    Args:
        catalog: Iceberg catalog
        table_name: Table to find join partners for

    Returns:
        List of suggested join conditions.
    """
    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        source_schema = get_table_schema(catalog, table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    source_columns = {f["name"]: f["type"] for f in source_schema["fields"]}
    all_tables = list_tables(catalog, namespace="*")

    suggestions = []
    for other_table in all_tables:
        if other_table == table_name:
            continue
        try:
            other_schema = get_table_schema(catalog, other_table)
        except Exception:
            continue

        other_columns = {f["name"]: f["type"] for f in other_schema["fields"]}

        # Find matching column names
        matching = set(source_columns.keys()) & set(other_columns.keys())
        for col in sorted(matching):
            suggestions.append({
                "table": other_table,
                "column": col,
                "source_type": source_columns[col],
                "target_type": other_columns[col],
                "join_sql": f"SELECT * FROM {table_name.split('.')[1]} a JOIN {other_table.split('.')[1]} b ON a.{col} = b.{col}",
            })

    return suggestions
