#!/usr/bin/env python3
"""Proof-of-concept: Vortex format integration via DuckDB extension.

Demonstrates:
1. Writing data as Vortex files via DuckDB
2. Reading Vortex files via DuckDB
3. Converting between Parquet and Vortex
4. Basic performance comparison
"""

import tempfile
import time
from pathlib import Path

import duckdb
import pyarrow as pa


def setup_duckdb_vortex():
    """Create a DuckDB connection with Vortex extension loaded."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL vortex")
    conn.execute("LOAD vortex")
    return conn


def create_sample_data(n_rows: int = 100_000) -> pa.Table:
    """Create sample Arrow table for benchmarking."""
    import random
    import datetime

    ids = list(range(n_rows))
    categories = [random.choice(["groceries", "transport", "entertainment", "utilities", "health"]) for _ in range(n_rows)]
    amounts = [round(random.uniform(1.0, 500.0), 2) for _ in range(n_rows)]
    currencies = [random.choice(["USD", "EUR", "GBP"]) for _ in range(n_rows)]
    descriptions = [f"Transaction {i} - {categories[i]}" for i in range(n_rows)]

    return pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "category": pa.array(categories, type=pa.string()),
        "description": pa.array(descriptions, type=pa.string()),
        "amount": pa.array(amounts, type=pa.float64()),
        "currency": pa.array(currencies, type=pa.string()),
    })


def benchmark_write(conn, arrow_table, output_dir: Path):
    """Benchmark writing data in Parquet vs Vortex format."""
    conn.register("bench_data", arrow_table)

    parquet_path = output_dir / "bench.parquet"
    vortex_path = output_dir / "bench.vortex"

    # Write Parquet
    start = time.perf_counter()
    conn.execute(f"COPY bench_data TO '{parquet_path}' (FORMAT parquet)")
    parquet_write_time = time.perf_counter() - start

    # Write Vortex
    start = time.perf_counter()
    conn.execute(f"COPY bench_data TO '{vortex_path}' (FORMAT vortex)")
    vortex_write_time = time.perf_counter() - start

    parquet_size = parquet_path.stat().st_size
    vortex_size = vortex_path.stat().st_size

    print(f"\n--- Write Benchmark ({arrow_table.num_rows:,} rows) ---")
    print(f"Parquet: {parquet_write_time:.3f}s, {parquet_size:,} bytes")
    print(f"Vortex:  {vortex_write_time:.3f}s, {vortex_size:,} bytes")
    print(f"Write speedup: {parquet_write_time / vortex_write_time:.2f}x")
    print(f"Size ratio:    {vortex_size / parquet_size:.2f}x")

    return parquet_path, vortex_path


def benchmark_read(conn, parquet_path: Path, vortex_path: Path):
    """Benchmark reading and scanning data in Parquet vs Vortex."""

    # Full scan
    start = time.perf_counter()
    conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
    parquet_scan_time = time.perf_counter() - start

    start = time.perf_counter()
    conn.execute(f"SELECT COUNT(*) FROM read_vortex('{vortex_path}')").fetchone()
    vortex_scan_time = time.perf_counter() - start

    print(f"\n--- Full Scan Benchmark ---")
    print(f"Parquet: {parquet_scan_time:.4f}s")
    print(f"Vortex:  {vortex_scan_time:.4f}s")
    print(f"Scan speedup: {parquet_scan_time / vortex_scan_time:.2f}x")

    # Filtered scan
    start = time.perf_counter()
    conn.execute(f"SELECT * FROM read_parquet('{parquet_path}') WHERE category = 'groceries' AND amount > 100").fetchall()
    parquet_filter_time = time.perf_counter() - start

    start = time.perf_counter()
    conn.execute(f"SELECT * FROM read_vortex('{vortex_path}') WHERE category = 'groceries' AND amount > 100").fetchall()
    vortex_filter_time = time.perf_counter() - start

    print(f"\n--- Filtered Scan Benchmark ---")
    print(f"Parquet: {parquet_filter_time:.4f}s")
    print(f"Vortex:  {vortex_filter_time:.4f}s")
    print(f"Filter speedup: {parquet_filter_time / vortex_filter_time:.2f}x")

    # Aggregation
    start = time.perf_counter()
    conn.execute(f"SELECT category, SUM(amount), COUNT(*) FROM read_parquet('{parquet_path}') GROUP BY category").fetchall()
    parquet_agg_time = time.perf_counter() - start

    start = time.perf_counter()
    conn.execute(f"SELECT category, SUM(amount), COUNT(*) FROM read_vortex('{vortex_path}') GROUP BY category").fetchall()
    vortex_agg_time = time.perf_counter() - start

    print(f"\n--- Aggregation Benchmark ---")
    print(f"Parquet: {parquet_agg_time:.4f}s")
    print(f"Vortex:  {vortex_agg_time:.4f}s")
    print(f"Agg speedup: {parquet_agg_time / vortex_agg_time:.2f}x")


def convert_parquet_to_vortex(conn, parquet_path: Path, vortex_path: Path):
    """Convert a Parquet file to Vortex format."""
    conn.execute(f"COPY (SELECT * FROM read_parquet('{parquet_path}')) TO '{vortex_path}' (FORMAT vortex)")
    print(f"\nConverted {parquet_path} -> {vortex_path}")
    print(f"  Parquet: {parquet_path.stat().st_size:,} bytes")
    print(f"  Vortex:  {vortex_path.stat().st_size:,} bytes")


def main():
    print("=== Vortex PoC: DuckDB Integration ===\n")

    try:
        conn = setup_duckdb_vortex()
        print("Vortex DuckDB extension loaded successfully.")
    except Exception as e:
        print(f"Failed to load Vortex extension: {e}")
        print("\nThis PoC requires DuckDB with Vortex extension support.")
        print("Install: INSTALL vortex; LOAD vortex;")
        return

    arrow_table = create_sample_data(100_000)
    print(f"Created sample data: {arrow_table.num_rows:,} rows, {arrow_table.num_columns} columns")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)

        # Benchmark write
        parquet_path, vortex_path = benchmark_write(conn, arrow_table, output_dir)

        # Benchmark read
        benchmark_read(conn, parquet_path, vortex_path)

        # Demonstrate conversion
        converted_path = output_dir / "converted.vortex"
        convert_parquet_to_vortex(conn, parquet_path, converted_path)

    conn.close()
    print("\n=== PoC Complete ===")


if __name__ == "__main__":
    main()
