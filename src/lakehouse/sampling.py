"""Data sampling — random, stratified, and systematic sampling."""

from typing import Optional


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def random_sample(
    catalog,
    table_name: str,
    fraction: float = 0.1,
    seed: Optional[int] = None,
    limit: Optional[int] = None,
) -> dict:
    """Random sample of rows."""
    import duckdb

    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()

    if arrow.num_rows == 0:
        return {
            "table": table_name,
            "rows": [],
            "sample_size": 0,
            "total_rows": 0,
            "fraction": fraction,
            "message": "Table is empty",
        }

    conn = duckdb.connect()
    conn.register("tbl", arrow)

    if seed is not None:
        conn.execute(f"SELECT setseed({seed / 2**31})")

    pct = fraction * 100
    query = f"SELECT * FROM tbl USING SAMPLE {pct:.4f} PERCENT (bernoulli)"
    if limit:
        query += f" LIMIT {limit}"

    result = conn.execute(query).fetchall()
    columns = [f.name for f in arrow.schema]
    rows = [dict(zip(columns, row)) for row in result]
    conn.close()

    return {
        "table": table_name,
        "rows": rows,
        "sample_size": len(rows),
        "total_rows": arrow.num_rows,
        "fraction": fraction,
        "actual_fraction": round(len(rows) / arrow.num_rows, 4) if arrow.num_rows > 0 else 0,
        "message": f"Random sample: {len(rows)} rows from '{table_name}' ({len(rows)}/{arrow.num_rows})",
    }


def stratified_sample(
    catalog,
    table_name: str,
    column: str,
    fraction: float = 0.1,
    seed: Optional[int] = None,
) -> dict:
    """Stratified sample maintaining column distribution."""
    import duckdb
    import math

    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()

    if arrow.num_rows == 0:
        return {
            "table": table_name,
            "rows": [],
            "sample_size": 0,
            "total_rows": 0,
            "column": column,
            "fraction": fraction,
            "strata": {},
            "message": "Table is empty",
        }

    columns = [f.name for f in arrow.schema]
    col_list = ", ".join(f'"{c}"' for c in columns)

    conn = duckdb.connect()
    conn.register("tbl", arrow)

    if seed is not None:
        conn.execute(f"SELECT setseed({seed / 2**31})")

    # Get stratum counts
    strata_counts = conn.execute(
        f'SELECT "{column}", COUNT(*) as cnt FROM tbl GROUP BY "{column}" ORDER BY cnt DESC'
    ).fetchall()

    # For each stratum, sample proportionally (at least 1 row per stratum)
    all_rows = []
    strata = {}
    for stratum_val, stratum_count in strata_counts:
        sample_n = max(1, math.ceil(stratum_count * fraction))
        query = f"""
            SELECT {col_list} FROM tbl
            WHERE "{column}" = $1
            ORDER BY random()
            LIMIT {sample_n}
        """
        rows = conn.execute(query, [stratum_val]).fetchall()
        row_dicts = [dict(zip(columns, r)) for r in rows]
        all_rows.extend(row_dicts)
        strata[str(stratum_val)] = {"total": stratum_count, "sampled": len(row_dicts)}

    conn.close()

    return {
        "table": table_name,
        "rows": all_rows,
        "sample_size": len(all_rows),
        "total_rows": arrow.num_rows,
        "column": column,
        "fraction": fraction,
        "strata": strata,
        "message": f"Stratified sample by '{column}': {len(all_rows)} rows from '{table_name}'",
    }


def systematic_sample(
    catalog,
    table_name: str,
    every_nth: int = 10,
) -> dict:
    """Every Nth row for deterministic, evenly-spaced sampling."""
    import duckdb

    table_name = _normalize(table_name)
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()

    if arrow.num_rows == 0:
        return {
            "table": table_name,
            "rows": [],
            "sample_size": 0,
            "total_rows": 0,
            "every_nth": every_nth,
            "message": "Table is empty",
        }

    columns = [f.name for f in arrow.schema]
    col_list = ", ".join(f'"{c}"' for c in columns)

    conn = duckdb.connect()
    conn.register("tbl", arrow)

    query = f"""
        SELECT {col_list} FROM (
            SELECT *, ROW_NUMBER() OVER () as _rn FROM tbl
        ) WHERE _rn % {every_nth} = 1
    """
    result = conn.execute(query).fetchall()
    rows = [dict(zip(columns, r)) for r in result]
    conn.close()

    return {
        "table": table_name,
        "rows": rows,
        "sample_size": len(rows),
        "total_rows": arrow.num_rows,
        "every_nth": every_nth,
        "message": f"Systematic sample (every {every_nth}th): {len(rows)} rows from '{table_name}'",
    }


def sample_to_table(
    catalog,
    table_name: str,
    sample_name: str,
    method: str = "random",
    fraction: float = 0.1,
    column: Optional[str] = None,
    every_nth: int = 10,
    seed: Optional[int] = None,
) -> dict:
    """Materialize a sample as a new Iceberg table."""
    from .catalog import create_table, insert_rows

    if method == "random":
        sample = random_sample(catalog, table_name, fraction=fraction, seed=seed)
    elif method == "stratified":
        if not column:
            raise ValueError("Stratified sampling requires 'column' parameter")
        sample = stratified_sample(catalog, table_name, column, fraction=fraction, seed=seed)
    elif method == "systematic":
        sample = systematic_sample(catalog, table_name, every_nth=every_nth)
    else:
        raise ValueError(f"Unknown method '{method}'. Use: random, stratified, systematic")

    rows = sample["rows"]
    if not rows:
        return {
            "table": table_name,
            "sample_table": _normalize(sample_name),
            "rows_sampled": 0,
            "message": "No rows to materialize (empty sample)",
        }

    # Infer column types from source table
    source = catalog.load_table(_normalize(table_name))
    schema_fields = source.schema().fields
    columns_def = {}
    type_map = {
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
    for field in schema_fields:
        type_name = type(field.field_type).__name__
        columns_def[field.name] = type_map.get(type_name, "string")

    create_table(catalog, sample_name, columns=columns_def)
    insert_rows(catalog, _normalize(sample_name), rows)

    return {
        "table": table_name,
        "sample_table": _normalize(sample_name),
        "method": method,
        "rows_sampled": len(rows),
        "message": f"Materialized {len(rows)} rows into '{_normalize(sample_name)}' using {method} sampling",
    }


def get_sample_stats(
    catalog,
    table_name: str,
    sample_name: str,
) -> dict:
    """Compare sample statistics vs full table."""
    import duckdb

    table_name = _normalize(table_name)
    sample_name = _normalize(sample_name)

    full_tbl = catalog.load_table(table_name)
    full_arrow = full_tbl.scan().to_arrow()

    sample_tbl = catalog.load_table(sample_name)
    sample_arrow = sample_tbl.scan().to_arrow()

    conn = duckdb.connect()
    conn.register("full_tbl", full_arrow)
    conn.register("sample_tbl", sample_arrow)

    columns = [f.name for f in full_arrow.schema]
    comparison = []

    for col in columns:
        col_type = str(full_arrow.schema.field(col).type)
        if col_type in ("int32", "int64", "float", "double"):
            full_stats = conn.execute(
                f'SELECT AVG("{col}"), STDDEV("{col}"), MIN("{col}"), MAX("{col}") FROM full_tbl'
            ).fetchone()
            sample_stats = conn.execute(
                f'SELECT AVG("{col}"), STDDEV("{col}"), MIN("{col}"), MAX("{col}") FROM sample_tbl'
            ).fetchone()
            comparison.append({
                "column": col,
                "type": "numeric",
                "full": {"mean": full_stats[0], "stddev": full_stats[1], "min": full_stats[2], "max": full_stats[3]},
                "sample": {"mean": sample_stats[0], "stddev": sample_stats[1], "min": sample_stats[2], "max": sample_stats[3]},
            })
        else:
            full_freq = conn.execute(
                f'SELECT "{col}", COUNT(*) FROM full_tbl GROUP BY "{col}" ORDER BY COUNT(*) DESC LIMIT 10'
            ).fetchall()
            sample_freq = conn.execute(
                f'SELECT "{col}", COUNT(*) FROM sample_tbl GROUP BY "{col}" ORDER BY COUNT(*) DESC LIMIT 10'
            ).fetchall()
            comparison.append({
                "column": col,
                "type": "categorical",
                "full_top_values": {str(r[0]): r[1] for r in full_freq},
                "sample_top_values": {str(r[0]): r[1] for r in sample_freq},
            })

    conn.close()

    return {
        "table": table_name,
        "sample_table": sample_name,
        "full_rows": full_arrow.num_rows,
        "sample_rows": sample_arrow.num_rows,
        "coverage": round(sample_arrow.num_rows / full_arrow.num_rows * 100, 2) if full_arrow.num_rows > 0 else 0,
        "column_comparison": comparison,
        "message": f"Sample '{sample_name}' has {sample_arrow.num_rows}/{full_arrow.num_rows} rows ({sample_arrow.num_rows / full_arrow.num_rows * 100:.1f}% coverage)",
    }
