"""Performance benchmarks comparing Parquet and Vortex formats.

Measures file size, write performance, read performance, and query performance
across different data sizes and types.

Usage:
    uv run python -m benchmarks.format_comparison
    uv run python -m benchmarks.format_comparison --rows 100000
    uv run python -m benchmarks.format_comparison --output results.md
"""

import argparse
import platform
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from lakehouse.vortex_io import write_vortex, read_vortex


# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

def generate_numeric_data(n: int) -> pa.Table:
    """Generate a table with mostly numeric columns."""
    import random
    random.seed(42)
    return pa.table({
        "id": pa.array(range(n), type=pa.int64()),
        "int_val": pa.array([random.randint(0, 1_000_000) for _ in range(n)], type=pa.int64()),
        "float_val": pa.array([random.random() * 1000 for _ in range(n)], type=pa.float64()),
        "small_int": pa.array([random.randint(0, 100) for _ in range(n)], type=pa.int64()),
    })


def generate_string_data(n: int) -> pa.Table:
    """Generate a table with string-heavy columns."""
    import random
    import string
    random.seed(42)

    words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
             "golf", "hotel", "india", "juliet", "kilo", "lima"]

    def random_text(min_words: int = 3, max_words: int = 15) -> str:
        return " ".join(random.choices(words, k=random.randint(min_words, max_words)))

    return pa.table({
        "id": pa.array(range(n), type=pa.int64()),
        "name": pa.array([random.choice(words) for _ in range(n)], type=pa.string()),
        "description": pa.array([random_text() for _ in range(n)], type=pa.string()),
        "category": pa.array([random.choice(["A", "B", "C", "D", "E"]) for _ in range(n)], type=pa.string()),
        "tags": pa.array([",".join(random.sample(words, k=3)) for _ in range(n)], type=pa.string()),
    })


def generate_mixed_data(n: int) -> pa.Table:
    """Generate a table with mixed column types (resembles real-world data)."""
    import random
    random.seed(42)

    categories = ["groceries", "transport", "entertainment", "utilities", "dining", "health"]
    currencies = ["USD", "EUR", "GBP", "JPY"]

    return pa.table({
        "id": pa.array(range(n), type=pa.int64()),
        "amount": pa.array([round(random.uniform(1.0, 5000.0), 2) for _ in range(n)], type=pa.float64()),
        "category": pa.array([random.choice(categories) for _ in range(n)], type=pa.string()),
        "currency": pa.array([random.choice(currencies) for _ in range(n)], type=pa.string()),
        "quantity": pa.array([random.randint(1, 100) for _ in range(n)], type=pa.int64()),
        "score": pa.array([round(random.uniform(0, 10), 1) for _ in range(n)], type=pa.float64()),
    })


DATA_GENERATORS = {
    "numeric": generate_numeric_data,
    "string": generate_string_data,
    "mixed": generate_mixed_data,
}


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def timed(fn: Callable, repeats: int = 3) -> tuple[float, any]:
    """Run fn `repeats` times and return (best_time_seconds, last_result)."""
    best = float("inf")
    result = None
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn()
        elapsed = time.perf_counter() - t0
        best = min(best, elapsed)
    return best, result


# ---------------------------------------------------------------------------
# Benchmark functions
# ---------------------------------------------------------------------------

class BenchmarkResult:
    """Container for benchmark results."""

    def __init__(self):
        self.rows: list[dict] = []

    def add(self, category: str, test: str, data_type: str, rows: int, **metrics):
        self.rows.append({
            "category": category,
            "test": test,
            "data_type": data_type,
            "rows": rows,
            **metrics,
        })

    def to_markdown(self) -> str:
        """Render results as markdown tables grouped by category."""
        if not self.rows:
            return "No results."

        lines = []
        categories = sorted(set(r["category"] for r in self.rows))

        for cat in categories:
            cat_rows = [r for r in self.rows if r["category"] == cat]
            lines.append(f"### {cat}\n")

            # Determine columns from first row (skip category)
            all_keys = list(cat_rows[0].keys())
            cols = [k for k in all_keys if k != "category"]

            # Header
            lines.append("| " + " | ".join(cols) + " |")
            lines.append("| " + " | ".join(["---"] * len(cols)) + " |")

            for r in cat_rows:
                vals = []
                for c in cols:
                    v = r.get(c, "")
                    if isinstance(v, float):
                        if v < 0.001:
                            vals.append(f"{v*1000:.2f}ms")
                        elif v < 1:
                            vals.append(f"{v*1000:.1f}ms")
                        else:
                            vals.append(f"{v:.3f}s")
                    elif isinstance(v, int) and v > 1024:
                        vals.append(f"{v:,}")
                    else:
                        vals.append(str(v))
                lines.append("| " + " | ".join(vals) + " |")

            lines.append("")

        return "\n".join(lines)


def benchmark_file_size(tmp_dir: Path, row_counts: list[int], results: BenchmarkResult):
    """Benchmark file sizes for Parquet vs Vortex."""
    for data_type, gen_fn in DATA_GENERATORS.items():
        for n in row_counts:
            table = gen_fn(n)

            pq_path = tmp_dir / f"size_{data_type}_{n}.parquet"
            vx_path = tmp_dir / f"size_{data_type}_{n}.vortex"

            pq.write_table(table, str(pq_path))
            write_vortex(table, vx_path)

            pq_size = pq_path.stat().st_size
            vx_size = vx_path.stat().st_size
            ratio = vx_size / pq_size if pq_size > 0 else 0

            results.add(
                "File Size",
                "size_comparison",
                data_type,
                n,
                parquet_bytes=pq_size,
                vortex_bytes=vx_size,
                ratio=round(ratio, 3),
            )


def benchmark_write(tmp_dir: Path, row_counts: list[int], results: BenchmarkResult):
    """Benchmark write performance for Parquet vs Vortex."""
    for data_type, gen_fn in DATA_GENERATORS.items():
        for n in row_counts:
            table = gen_fn(n)

            pq_path = tmp_dir / f"write_{data_type}_{n}.parquet"
            vx_path = tmp_dir / f"write_{data_type}_{n}.vortex"

            pq_time, _ = timed(lambda: pq.write_table(table, str(pq_path)))
            vx_time, _ = timed(lambda: write_vortex(table, vx_path))

            results.add(
                "Write Performance",
                "write",
                data_type,
                n,
                parquet_time=pq_time,
                vortex_time=vx_time,
                speedup=round(pq_time / vx_time, 2) if vx_time > 0 else 0,
            )


def benchmark_read(tmp_dir: Path, row_counts: list[int], results: BenchmarkResult):
    """Benchmark read performance (full scan) for Parquet vs Vortex."""
    for data_type, gen_fn in DATA_GENERATORS.items():
        for n in row_counts:
            table = gen_fn(n)

            pq_path = tmp_dir / f"read_{data_type}_{n}.parquet"
            vx_path = tmp_dir / f"read_{data_type}_{n}.vortex"

            pq.write_table(table, str(pq_path))
            write_vortex(table, vx_path)

            pq_time, _ = timed(lambda: pq.read_table(str(pq_path)))
            vx_time, _ = timed(lambda: read_vortex(vx_path))

            results.add(
                "Read Performance (Full Scan)",
                "full_scan",
                data_type,
                n,
                parquet_time=pq_time,
                vortex_time=vx_time,
                speedup=round(pq_time / vx_time, 2) if vx_time > 0 else 0,
            )


def _cast_string_views(table: pa.Table) -> pa.Table:
    """Cast string_view columns to large_string for DuckDB compatibility."""
    new_cols = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_large_string(field.type) or str(field.type) == "string_view":
            col = col.cast(pa.string())
        new_cols.append(col)
    return pa.table({table.schema.field(i).name: new_cols[i] for i in range(len(new_cols))})


def benchmark_query(tmp_dir: Path, row_counts: list[int], results: BenchmarkResult):
    """Benchmark DuckDB query performance on Parquet vs Vortex files."""
    queries = {
        "select_all": "SELECT * FROM data",
        "filtered": "SELECT * FROM data WHERE id < {half}",
        "aggregation": "SELECT count(*), sum(id) FROM data",
        "group_by": "SELECT {group_col}, count(*) FROM data GROUP BY {group_col}",
    }

    for data_type, gen_fn in DATA_GENERATORS.items():
        # Choose group column based on data type
        if data_type == "numeric":
            group_col = "small_int"
        elif data_type == "string":
            group_col = "category"
        else:
            group_col = "category"

        for n in row_counts:
            table = gen_fn(n)
            half = n // 2

            pq_path = tmp_dir / f"query_{data_type}_{n}.parquet"
            vx_path = tmp_dir / f"query_{data_type}_{n}.vortex"

            pq.write_table(table, str(pq_path))
            write_vortex(table, vx_path)

            for qname, sql_template in queries.items():
                sql = sql_template.format(half=half, group_col=group_col)

                def run_pq():
                    conn = duckdb.connect(":memory:")
                    conn.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{pq_path}')")
                    return conn.execute(sql).fetchdf()

                def run_vx():
                    conn = duckdb.connect(":memory:")
                    # Use Arrow bridge for Vortex (most portable)
                    arrow = _cast_string_views(read_vortex(vx_path))
                    conn.register("data", arrow)
                    return conn.execute(sql).fetchdf()

                pq_time, _ = timed(run_pq)
                vx_time, _ = timed(run_vx)

                results.add(
                    "Query Performance (DuckDB)",
                    qname,
                    data_type,
                    n,
                    parquet_time=pq_time,
                    vortex_time=vx_time,
                    speedup=round(pq_time / vx_time, 2) if vx_time > 0 else 0,
                )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_benchmarks(
    row_counts: list[int] | None = None,
    tmp_dir: Path | None = None,
) -> BenchmarkResult:
    """Run all benchmarks and return results.

    Args:
        row_counts: List of row counts to test. Default: [100, 1000, 10000]
        tmp_dir: Temporary directory for benchmark files.

    Returns:
        BenchmarkResult with all measurements.
    """
    import tempfile

    if row_counts is None:
        row_counts = [100, 1_000, 10_000]

    if tmp_dir is None:
        tmp_dir = Path(tempfile.mkdtemp(prefix="lakehouse_bench_"))
    tmp_dir.mkdir(parents=True, exist_ok=True)

    results = BenchmarkResult()

    benchmark_file_size(tmp_dir, row_counts, results)
    benchmark_write(tmp_dir, row_counts, results)
    benchmark_read(tmp_dir, row_counts, results)
    benchmark_query(tmp_dir, row_counts, results)

    return results


def system_info() -> str:
    """Get system information for benchmark context."""
    import sys
    lines = [
        f"- **Date**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"- **Python**: {sys.version.split()[0]}",
        f"- **Platform**: {platform.platform()}",
        f"- **Processor**: {platform.processor() or platform.machine()}",
        f"- **DuckDB**: {duckdb.__version__}",
        f"- **PyArrow**: {pa.__version__}",
    ]
    try:
        import vortex as vx
        lines.append(f"- **Vortex**: {vx.__version__}")
    except (ImportError, AttributeError):
        lines.append("- **Vortex**: (version unknown)")
    return "\n".join(lines)


def generate_report(results: BenchmarkResult, row_counts: list[int]) -> str:
    """Generate a full markdown report."""
    lines = [
        "# Parquet vs Vortex Performance Benchmarks",
        "",
        "## Environment",
        "",
        system_info(),
        "",
        f"## Test Configuration",
        "",
        f"- **Row counts**: {', '.join(f'{n:,}' for n in row_counts)}",
        f"- **Data types**: numeric, string, mixed",
        f"- **Timing**: best of 3 runs",
        "",
        "## Results",
        "",
        results.to_markdown(),
        "",
        "## Notes",
        "",
        "- **Speedup > 1.0** means Vortex is faster than Parquet",
        "- **Speedup < 1.0** means Parquet is faster than Vortex",
        "- **Ratio** for file size: Vortex size / Parquet size (< 1.0 = Vortex is smaller)",
        "- Vortex queries use Arrow bridge (read via vortex-data, register in DuckDB)",
        "- Parquet queries use DuckDB native `read_parquet()`",
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Run Parquet vs Vortex benchmarks")
    parser.add_argument(
        "--rows",
        type=str,
        default="100,1000,10000",
        help="Comma-separated row counts (default: 100,1000,10000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output markdown file (default: print to stdout)",
    )
    args = parser.parse_args()

    row_counts = [int(x.strip()) for x in args.rows.split(",")]

    print(f"Running benchmarks with row counts: {row_counts}")
    print(f"Data types: numeric, string, mixed")
    print()

    results = run_benchmarks(row_counts=row_counts)
    report = generate_report(results, row_counts)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report)
        print(f"Report written to {output_path}")
    else:
        print(report)


if __name__ == "__main__":
    main()
