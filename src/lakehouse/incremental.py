"""Incremental processing — watermark-based pipeline runs."""

import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_WATERMARK_PATH = Path.home() / ".lakehouse" / "watermarks.json"


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_WATERMARK_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_WATERMARK_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def set_watermark(
    pipeline_name: str,
    table_name: str,
    snapshot_id: int,
    rows_processed: int = 0,
    store_path: Optional[Path] = None,
) -> dict:
    """Record the last-processed snapshot ID for a pipeline/table pair."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if pipeline_name not in store:
        store[pipeline_name] = {}

    store[pipeline_name][table_name] = {
        "snapshot_id": snapshot_id,
        "processed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "rows_processed": rows_processed,
    }
    _save_store(store, store_path)

    return {
        "pipeline": pipeline_name,
        "table": table_name,
        "snapshot_id": snapshot_id,
        "message": f"Watermark set for '{pipeline_name}/{table_name}' at snapshot {snapshot_id}",
    }


def get_watermark(
    pipeline_name: str,
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get the last-processed snapshot ID."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    pipeline_data = store.get(pipeline_name, {})
    watermark = pipeline_data.get(table_name)

    if watermark is None:
        return {
            "pipeline": pipeline_name,
            "table": table_name,
            "snapshot_id": None,
            "message": f"No watermark for '{pipeline_name}/{table_name}'",
        }

    return {
        "pipeline": pipeline_name,
        "table": table_name,
        "snapshot_id": watermark["snapshot_id"],
        "processed_at": watermark.get("processed_at"),
        "rows_processed": watermark.get("rows_processed", 0),
        "message": f"Watermark for '{pipeline_name}/{table_name}': snapshot {watermark['snapshot_id']}",
    }


def list_watermarks(
    pipeline_name: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all watermarks, optionally filtered by pipeline."""
    store = _load_store(store_path)
    results = []

    pipelines = store
    if pipeline_name:
        pipelines = {pipeline_name: store.get(pipeline_name, {})}

    for pname, tables in pipelines.items():
        for tbl, wm in tables.items():
            results.append({
                "pipeline": pname,
                "table": tbl,
                "snapshot_id": wm["snapshot_id"],
                "processed_at": wm.get("processed_at"),
                "rows_processed": wm.get("rows_processed", 0),
            })

    return results


def reset_watermark(
    pipeline_name: str,
    table_name: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Reset watermark to force full reprocessing."""
    store = _load_store(store_path)

    if pipeline_name not in store:
        return {"pipeline": pipeline_name, "message": f"No watermarks found for pipeline '{pipeline_name}'"}

    if table_name:
        table_name = _normalize(table_name)
        if table_name in store[pipeline_name]:
            del store[pipeline_name][table_name]
            if not store[pipeline_name]:
                del store[pipeline_name]
            _save_store(store, store_path)
            return {"pipeline": pipeline_name, "table": table_name, "message": f"Watermark reset for '{pipeline_name}/{table_name}'"}
        return {"pipeline": pipeline_name, "table": table_name, "message": f"No watermark found for '{pipeline_name}/{table_name}'"}
    else:
        del store[pipeline_name]
        _save_store(store, store_path)
        return {"pipeline": pipeline_name, "message": f"All watermarks reset for pipeline '{pipeline_name}'"}


def get_incremental_data(
    catalog,
    table_name: str,
    pipeline_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get only new rows since the last watermark.

    Uses Iceberg snapshot-based reads to get rows added between
    the watermarked snapshot and the current snapshot.

    Returns:
        Dict with dataframe, row_count, from_snapshot, to_snapshot.
    """
    from .catalog import scan_as_of

    table_name = _normalize(table_name)

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    current_snap = table.current_snapshot()
    if current_snap is None:
        import pandas as pd
        return {
            "table": table_name,
            "pipeline": pipeline_name,
            "dataframe": pd.DataFrame(),
            "row_count": 0,
            "from_snapshot": None,
            "to_snapshot": None,
            "is_full": True,
            "message": f"No snapshots in '{table_name}' — nothing to process",
        }

    current_id = current_snap.snapshot_id
    watermark = get_watermark(pipeline_name, table_name, store_path=store_path)
    last_snapshot_id = watermark["snapshot_id"]

    if last_snapshot_id is not None and last_snapshot_id == current_id:
        import pandas as pd
        return {
            "table": table_name,
            "pipeline": pipeline_name,
            "dataframe": pd.DataFrame(),
            "row_count": 0,
            "from_snapshot": last_snapshot_id,
            "to_snapshot": current_id,
            "is_full": False,
            "message": f"No new data in '{table_name}' since snapshot {last_snapshot_id}",
        }

    if last_snapshot_id is None:
        # No watermark — full scan
        arrow = table.scan().to_arrow()
        df = arrow.to_pandas()
        return {
            "table": table_name,
            "pipeline": pipeline_name,
            "dataframe": df,
            "row_count": len(df),
            "from_snapshot": None,
            "to_snapshot": current_id,
            "is_full": True,
            "message": f"Full scan of '{table_name}': {len(df)} rows (no prior watermark)",
        }

    # Incremental: get data added between watermark snapshot and current
    # Read both snapshots and diff
    old_arrow = scan_as_of(catalog, table_name, str(last_snapshot_id))
    new_arrow = table.scan().to_arrow()

    import duckdb
    conn = duckdb.connect(":memory:")
    conn.register("old_data", old_arrow)
    conn.register("new_data", new_arrow)

    # Find rows in new that are not in old (added rows)
    columns = [f.name for f in table.schema().fields]
    col_list = ", ".join(columns)

    result = conn.execute(
        f"SELECT {col_list} FROM new_data EXCEPT SELECT {col_list} FROM old_data"
    ).fetchdf()
    conn.close()

    return {
        "table": table_name,
        "pipeline": pipeline_name,
        "dataframe": result,
        "row_count": len(result),
        "from_snapshot": last_snapshot_id,
        "to_snapshot": current_id,
        "is_full": False,
        "message": f"Incremental data for '{table_name}': {len(result)} new rows (snapshot {last_snapshot_id} → {current_id})",
    }


def run_pipeline_incremental(
    name: str,
    catalog,
    engine,
    store_path: Optional[Path] = None,
    watermark_path: Optional[Path] = None,
) -> dict:
    """Run a pipeline in incremental mode.

    For each step with a source table, reads only new data since the last watermark.
    Updates watermarks after successful completion.
    """
    from .pipelines import get_pipeline
    from .catalog import insert_rows, create_table

    pipeline = get_pipeline(name, store_path=store_path)
    if pipeline is None:
        raise ValueError(f"Pipeline '{name}' not found")

    steps = pipeline["steps"]
    step_results = []
    source_snapshots = {}  # Track current snapshot per source table

    for i, step in enumerate(steps):
        source = step.get("source_table")
        target = step.get("target_table")

        if source:
            source_tbl = _normalize(source)

            # Get incremental data
            inc = get_incremental_data(catalog, source_tbl, name, store_path=watermark_path)

            if inc["row_count"] == 0:
                step_results.append({
                    "step": i + 1,
                    "source": source_tbl,
                    "target": target,
                    "status": "skipped",
                    "rows": 0,
                    "message": "No new data to process",
                })
                continue

            # Track the snapshot for watermark update
            source_snapshots[source_tbl] = inc["to_snapshot"]

            # Register incremental data and run the SQL against it
            import duckdb
            conn = duckdb.connect(":memory:")
            short_name = source.split(".")[-1] if "." in source else source
            conn.register(short_name, inc["dataframe"])

            sql = step.get("sql", f"SELECT * FROM {short_name}")
            result_df = conn.execute(sql).fetchdf()
            conn.close()

            if target:
                target_tbl = _normalize(target)
                rows = result_df.to_dict(orient="records")
                if rows:
                    try:
                        catalog.load_table(target_tbl)
                    except Exception:
                        # Create target if it doesn't exist
                        columns = {col: "string" for col in result_df.columns}
                        create_table(catalog, target, columns=columns)
                    insert_rows(catalog, target_tbl, rows)

            step_results.append({
                "step": i + 1,
                "source": source_tbl,
                "target": target,
                "status": "success",
                "rows": len(result_df),
                "is_full": inc["is_full"],
                "message": f"Processed {len(result_df)} rows",
            })
        else:
            # Non-source steps run as-is
            try:
                sql = step.get("sql", "")
                if sql:
                    engine.execute(sql)
                step_results.append({
                    "step": i + 1,
                    "status": "success",
                    "message": "Executed SQL",
                })
            except Exception as e:
                step_results.append({
                    "step": i + 1,
                    "status": "failed",
                    "message": str(e),
                })
                return {
                    "pipeline": name,
                    "status": "failed",
                    "steps": step_results,
                    "message": f"Pipeline '{name}' failed at step {i + 1}: {e}",
                }

    # Update watermarks for all processed sources
    for tbl, snap_id in source_snapshots.items():
        if snap_id is not None:
            rows_total = sum(
                s.get("rows", 0) for s in step_results
                if s.get("source") == tbl and s.get("status") == "success"
            )
            set_watermark(name, tbl, snap_id, rows_processed=rows_total, store_path=watermark_path)

    total_rows = sum(s.get("rows", 0) for s in step_results)
    return {
        "pipeline": name,
        "status": "success",
        "steps": step_results,
        "total_rows": total_rows,
        "watermarks_updated": len(source_snapshots),
        "message": f"Pipeline '{name}' completed: {total_rows} rows processed across {len(step_results)} step(s)",
    }
