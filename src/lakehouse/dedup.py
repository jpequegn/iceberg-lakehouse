"""Data deduplication — detect and remove duplicate rows."""

from typing import Optional


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def find_duplicates(
    catalog,
    table_name: str,
    key_columns: Optional[list[str]] = None,
    limit: int = 100,
) -> dict:
    """Find duplicate rows based on key columns."""
    import duckdb

    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()

    if arrow.num_rows == 0:
        return {
            "table": table_name,
            "duplicates": [],
            "duplicate_count": 0,
            "message": "Table is empty",
        }

    columns = [f.name for f in arrow.schema]
    keys = key_columns or columns
    key_list = ", ".join(f'"{k}"' for k in keys)

    conn = duckdb.connect()
    conn.register("tbl", arrow)

    # Find duplicate groups
    query = f"""
        SELECT {key_list}, COUNT(*) as _dup_count
        FROM tbl
        GROUP BY {key_list}
        HAVING COUNT(*) > 1
        ORDER BY _dup_count DESC
        LIMIT {limit}
    """
    result = conn.execute(query).fetchall()
    result_cols = keys + ["_dup_count"]
    duplicates = [dict(zip(result_cols, row)) for row in result]

    total_dups = 0
    for d in duplicates:
        total_dups += d["_dup_count"] - 1  # Extra copies beyond the first

    conn.close()

    return {
        "table": table_name,
        "duplicates": duplicates,
        "duplicate_groups": len(duplicates),
        "duplicate_count": total_dups,
        "key_columns": keys,
        "message": f"Found {len(duplicates)} duplicate groups ({total_dups} extra rows) in '{table_name}'",
    }


def dedup_summary(
    catalog,
    table_name: str,
    key_columns: Optional[list[str]] = None,
) -> dict:
    """Get deduplication summary statistics."""
    import duckdb

    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()

    if arrow.num_rows == 0:
        return {
            "table": table_name,
            "total_rows": 0,
            "unique_rows": 0,
            "duplicate_rows": 0,
            "duplicate_pct": 0.0,
            "message": "Table is empty",
        }

    columns = [f.name for f in arrow.schema]
    keys = key_columns or columns
    key_list = ", ".join(f'"{k}"' for k in keys)

    conn = duckdb.connect()
    conn.register("tbl", arrow)

    total = arrow.num_rows
    unique = conn.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT {key_list} FROM tbl)").fetchone()[0]
    conn.close()

    duplicates = total - unique
    dup_pct = (duplicates / total * 100) if total > 0 else 0.0

    return {
        "table": table_name,
        "total_rows": total,
        "unique_rows": unique,
        "duplicate_rows": duplicates,
        "duplicate_pct": round(dup_pct, 2),
        "key_columns": keys,
        "message": f"Dedup summary for '{table_name}': {duplicates} duplicates ({dup_pct:.1f}%) out of {total} rows",
    }


def remove_duplicates(
    catalog,
    table_name: str,
    key_columns: Optional[list[str]] = None,
    keep: str = "first",
    dry_run: bool = True,
) -> dict:
    """Remove duplicates, keeping first or last occurrence."""
    import duckdb

    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()

    if arrow.num_rows == 0:
        return {
            "table": table_name,
            "removed": 0,
            "remaining": 0,
            "dry_run": dry_run,
            "message": "Table is empty",
        }

    columns = [f.name for f in arrow.schema]
    keys = key_columns or columns
    key_list = ", ".join(f'"{k}"' for k in keys)
    col_list = ", ".join(f'"{c}"' for c in columns)

    conn = duckdb.connect()
    conn.register("tbl", arrow)

    # Use ROW_NUMBER to identify duplicates
    if keep == "first":
        order = "ASC"
    elif keep == "last":
        order = "DESC"
    else:
        raise ValueError(f"keep must be 'first' or 'last', got '{keep}'")

    dedup_query = f"""
        SELECT {col_list} FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_list}) as _rn
            FROM tbl
        ) WHERE _rn = 1
    """
    deduped = conn.execute(dedup_query).fetch_arrow_table()
    conn.close()

    removed = arrow.num_rows - deduped.num_rows

    if dry_run:
        return {
            "table": table_name,
            "removed": removed,
            "remaining": deduped.num_rows,
            "dry_run": True,
            "keep": keep,
            "key_columns": keys,
            "message": f"Dry run: would remove {removed} duplicates from '{table_name}' ({deduped.num_rows} remaining)",
        }

    # Actually remove by rewriting the table
    from .catalog import insert_rows

    # Overwrite: delete all then insert deduped
    rows = deduped.to_pydict()
    row_list = [dict(zip(rows.keys(), vals)) for vals in zip(*rows.values())]

    # Use PyIceberg overwrite
    table.overwrite(deduped)

    return {
        "table": table_name,
        "removed": removed,
        "remaining": len(row_list),
        "dry_run": False,
        "keep": keep,
        "key_columns": keys,
        "message": f"Removed {removed} duplicates from '{table_name}' ({len(row_list)} remaining)",
    }


def dedup_report(
    catalog,
    table_name: str,
    key_columns: Optional[list[str]] = None,
) -> dict:
    """Generate a comprehensive dedup report with per-column uniqueness."""
    import duckdb
    from .stats import get_cached_stats, compute_table_stats

    table_name = _normalize(table_name)
    summary = dedup_summary(catalog, table_name, key_columns=key_columns)

    # Get per-column uniqueness
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()
    columns = [f.name for f in arrow.schema]

    conn = duckdb.connect()
    conn.register("tbl", arrow)

    column_analysis = []
    for col in columns:
        unique_count = conn.execute(f'SELECT COUNT(DISTINCT "{col}") FROM tbl').fetchone()[0]
        null_count = conn.execute(f'SELECT COUNT(*) FROM tbl WHERE "{col}" IS NULL').fetchone()[0]
        uniqueness = (unique_count / arrow.num_rows * 100) if arrow.num_rows > 0 else 0.0
        column_analysis.append({
            "column": col,
            "unique_values": unique_count,
            "null_count": null_count,
            "uniqueness_pct": round(uniqueness, 2),
            "good_dedup_key": uniqueness > 50,
        })

    conn.close()

    # Suggest dedup keys: columns with high uniqueness
    suggested_keys = [c["column"] for c in column_analysis if c["uniqueness_pct"] > 80]

    # Estimate space savings
    if summary["total_rows"] > 0:
        try:
            stats = compute_table_stats(catalog, table_name)
            total_size = stats.get("total_size_bytes", 0)
            savings = int(total_size * (summary["duplicate_rows"] / summary["total_rows"]))
        except Exception:
            savings = 0
    else:
        savings = 0

    return {
        **summary,
        "column_analysis": column_analysis,
        "suggested_keys": suggested_keys,
        "estimated_space_savings_bytes": savings,
        "message": f"Dedup report for '{table_name}': {summary['duplicate_rows']} duplicates, {len(suggested_keys)} suggested keys",
    }
