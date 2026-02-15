"""Data pipelines — multi-step SQL transformations."""

import datetime
import json
import time
from pathlib import Path
from typing import Optional

DEFAULT_PIPELINE_PATH = Path.home() / ".lakehouse" / "pipelines.json"


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_PIPELINE_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_PIPELINE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def create_pipeline(
    name: str,
    steps: list[dict],
    description: str = "",
    store_path: Optional[Path] = None,
) -> dict:
    """Create a named pipeline.

    Args:
        name: Pipeline name
        steps: List of dicts with 'sql' (required), 'target_table' (optional),
               'mode' (optional, 'overwrite' or 'append', default 'overwrite')
        description: Optional description
        store_path: Optional path to metadata store

    Returns:
        Dict with pipeline details.
    """
    if not name or not name.strip():
        raise ValueError("Pipeline name must not be empty")
    if not steps:
        raise ValueError("Steps must not be empty")

    for i, step in enumerate(steps):
        if not step.get("sql") or not step["sql"].strip():
            raise ValueError(f"Step {i} has empty SQL")
        mode = step.get("mode", "overwrite")
        if mode not in ("overwrite", "append"):
            raise ValueError(f"Step {i} has invalid mode '{mode}' (must be 'overwrite' or 'append')")

    store = _load_store(store_path)
    if name in store:
        raise ValueError(f"Pipeline '{name}' already exists")

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    store[name] = {
        "steps": steps,
        "description": description,
        "created_at": now,
        "last_run": None,
        "last_run_status": None,
    }
    _save_store(store, store_path)

    return {
        "name": name,
        "steps": len(steps),
        "description": description,
        "created_at": now,
        "message": f"Created pipeline '{name}' ({len(steps)} steps)",
    }


def get_pipeline(
    name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get pipeline definition."""
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"Pipeline '{name}' not found")

    entry = store[name]
    return {
        "name": name,
        "steps": entry["steps"],
        "description": entry.get("description", ""),
        "created_at": entry.get("created_at", ""),
        "last_run": entry.get("last_run"),
        "last_run_status": entry.get("last_run_status"),
    }


def list_pipelines(
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all pipelines."""
    store = _load_store(store_path)
    result = []
    for name, entry in sorted(store.items()):
        result.append({
            "name": name,
            "step_count": len(entry["steps"]),
            "description": entry.get("description", ""),
            "created_at": entry.get("created_at", ""),
            "last_run": entry.get("last_run"),
            "last_run_status": entry.get("last_run_status"),
        })
    return result


def run_pipeline(
    name: str,
    catalog,
    engine,
    dry_run: bool = False,
    store_path: Optional[Path] = None,
) -> dict:
    """Execute all steps in a pipeline sequentially.

    Args:
        name: Pipeline name
        catalog: Iceberg catalog
        engine: QueryEngine instance
        dry_run: If True, validate SQL without executing
        store_path: Optional path to metadata store

    Returns:
        Dict with per-step results.
    """
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"Pipeline '{name}' not found")

    entry = store[name]
    steps = entry["steps"]
    step_results = []
    overall_start = time.time()

    for i, step in enumerate(steps):
        sql = step["sql"]
        target_table = step.get("target_table")
        mode = step.get("mode", "overwrite")

        step_start = time.time()

        if dry_run:
            # Validate SQL syntax by preparing (not executing)
            try:
                # Use DuckDB explain to validate without executing
                import duckdb
                conn = duckdb.connect(":memory:")
                # Register tables for validation
                from .joins import _register_all_tables
                _register_all_tables(catalog, conn)
                # Resolve namespace references
                from .joins import _resolve_namespace_refs
                resolved_sql = _resolve_namespace_refs(sql, catalog)
                conn.execute(f"EXPLAIN {resolved_sql}")
                conn.close()
                step_results.append({
                    "step": i,
                    "sql": sql,
                    "target_table": target_table,
                    "status": "validated",
                    "duration_ms": int((time.time() - step_start) * 1000),
                })
            except Exception as e:
                conn.close()
                step_results.append({
                    "step": i,
                    "sql": sql,
                    "target_table": target_table,
                    "status": "error",
                    "error": str(e),
                    "duration_ms": int((time.time() - step_start) * 1000),
                })
                break
        else:
            try:
                if target_table:
                    # Execute and write results to target table
                    from .joins import execute_join
                    from .catalog import create_table, insert_rows, delete_rows, list_tables

                    result = execute_join(catalog, sql)
                    df = result["dataframe"]

                    if "." not in target_table:
                        target_table = f"default.{target_table}"

                    # Check if table exists
                    try:
                        catalog.load_table(target_table)
                        table_exists = True
                    except Exception:
                        table_exists = False

                    if not table_exists:
                        # Infer types and create
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
                        create_table(catalog, target_table, columns)
                    elif mode == "overwrite":
                        try:
                            delete_rows(catalog, target_table, "1=1")
                        except Exception:
                            pass

                    rows_written = 0
                    if not df.empty:
                        rows = df.to_dict(orient="records")
                        insert_rows(catalog, target_table, rows)
                        rows_written = len(rows)

                    # Log to audit
                    try:
                        from .audit import log_operation
                        log_operation(
                            target_table, "pipeline_step",
                            rows_affected=rows_written,
                            source="pipeline",
                            details={"pipeline": name, "step": i, "mode": mode},
                        )
                    except Exception:
                        pass

                    # Record lineage
                    try:
                        from .lineage import record_lineage
                        all_tables = list_tables(catalog, namespace="*")
                        sources = []
                        sql_lower = sql.lower()
                        for tbl in all_tables:
                            short = tbl.split(".")[-1]
                            if short.lower() in sql_lower or tbl.lower() in sql_lower:
                                if tbl != target_table:
                                    sources.append(tbl)
                        if sources:
                            record_lineage(sources, target_table, operation="pipeline", sql=sql)
                    except Exception:
                        pass

                    step_results.append({
                        "step": i,
                        "sql": sql,
                        "target_table": target_table,
                        "rows_affected": rows_written,
                        "status": "completed",
                        "duration_ms": int((time.time() - step_start) * 1000),
                    })
                else:
                    # Execute SQL directly (no target table)
                    df = engine.execute(sql, max_rows=1_000_000)
                    step_results.append({
                        "step": i,
                        "sql": sql,
                        "target_table": None,
                        "rows_affected": len(df),
                        "status": "completed",
                        "duration_ms": int((time.time() - step_start) * 1000),
                    })
            except Exception as e:
                step_results.append({
                    "step": i,
                    "sql": sql,
                    "target_table": target_table,
                    "status": "error",
                    "error": str(e),
                    "duration_ms": int((time.time() - step_start) * 1000),
                })
                # Failure stops the pipeline
                break

    total_ms = int((time.time() - overall_start) * 1000)

    # Update pipeline metadata
    if not dry_run:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        has_error = any(r["status"] == "error" for r in step_results)
        entry["last_run"] = now
        entry["last_run_status"] = "failed" if has_error else "completed"
        _save_store(store, store_path)

    completed = sum(1 for r in step_results if r["status"] in ("completed", "validated"))
    failed = sum(1 for r in step_results if r["status"] == "error")
    mode_str = " (dry run)" if dry_run else ""

    return {
        "name": name,
        "steps_total": len(steps),
        "steps_completed": completed,
        "steps_failed": failed,
        "step_results": step_results,
        "duration_ms": total_ms,
        "dry_run": dry_run,
        "message": f"Pipeline '{name}'{mode_str}: {completed}/{len(steps)} steps completed ({total_ms}ms)",
    }


def drop_pipeline(
    name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Drop a pipeline definition."""
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"Pipeline '{name}' not found")

    del store[name]
    _save_store(store, store_path)

    return {
        "name": name,
        "message": f"Dropped pipeline '{name}'",
    }
