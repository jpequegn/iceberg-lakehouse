"""Tests for the benchmark suite."""

import pytest

from benchmarks.format_comparison import (
    generate_numeric_data,
    generate_string_data,
    generate_mixed_data,
    BenchmarkResult,
    benchmark_file_size,
    benchmark_write,
    benchmark_read,
    benchmark_query,
    run_benchmarks,
    generate_report,
    system_info,
)


class TestDataGenerators:
    """Test synthetic data generators."""

    def test_numeric_data(self):
        """Test numeric data generation."""
        table = generate_numeric_data(100)
        assert table.num_rows == 100
        assert "id" in table.column_names
        assert "int_val" in table.column_names
        assert "float_val" in table.column_names

    def test_string_data(self):
        """Test string-heavy data generation."""
        table = generate_string_data(100)
        assert table.num_rows == 100
        assert "name" in table.column_names
        assert "description" in table.column_names
        assert "tags" in table.column_names

    def test_mixed_data(self):
        """Test mixed data generation."""
        table = generate_mixed_data(100)
        assert table.num_rows == 100
        assert "amount" in table.column_names
        assert "category" in table.column_names
        assert "currency" in table.column_names

    def test_deterministic(self):
        """Test that generators produce deterministic output."""
        t1 = generate_numeric_data(50)
        t2 = generate_numeric_data(50)
        assert t1.column("int_val").to_pylist() == t2.column("int_val").to_pylist()

    def test_various_sizes(self):
        """Test generators with different row counts."""
        for n in [1, 10, 1000]:
            assert generate_numeric_data(n).num_rows == n
            assert generate_string_data(n).num_rows == n
            assert generate_mixed_data(n).num_rows == n


class TestBenchmarkResult:
    """Test BenchmarkResult container."""

    def test_add_and_access(self):
        """Test adding and accessing results."""
        r = BenchmarkResult()
        r.add("File Size", "test1", "numeric", 100, parquet_bytes=1000, vortex_bytes=800)
        assert len(r.rows) == 1
        assert r.rows[0]["category"] == "File Size"
        assert r.rows[0]["parquet_bytes"] == 1000

    def test_to_markdown(self):
        """Test markdown rendering."""
        r = BenchmarkResult()
        r.add("File Size", "test1", "numeric", 100, parquet_bytes=1000, vortex_bytes=800)
        r.add("File Size", "test2", "string", 100, parquet_bytes=2000, vortex_bytes=1500)
        md = r.to_markdown()
        assert "File Size" in md
        assert "numeric" in md
        assert "string" in md

    def test_empty_results(self):
        """Test rendering empty results."""
        r = BenchmarkResult()
        assert r.to_markdown() == "No results."


class TestBenchmarkFunctions:
    """Test individual benchmark functions at small scale."""

    def test_file_size_benchmark(self, tmp_path):
        """Test file size comparison runs without error."""
        results = BenchmarkResult()
        benchmark_file_size(tmp_path, [50], results)
        # 3 data types * 1 row count = 3 results
        assert len(results.rows) == 3
        for r in results.rows:
            assert r["parquet_bytes"] > 0
            assert r["vortex_bytes"] > 0

    def test_write_benchmark(self, tmp_path):
        """Test write benchmark runs without error."""
        results = BenchmarkResult()
        benchmark_write(tmp_path, [50], results)
        assert len(results.rows) == 3
        for r in results.rows:
            assert r["parquet_time"] > 0
            assert r["vortex_time"] > 0

    def test_read_benchmark(self, tmp_path):
        """Test read benchmark runs without error."""
        results = BenchmarkResult()
        benchmark_read(tmp_path, [50], results)
        assert len(results.rows) == 3
        for r in results.rows:
            assert r["parquet_time"] > 0
            assert r["vortex_time"] > 0

    def test_query_benchmark(self, tmp_path):
        """Test query benchmark runs without error."""
        results = BenchmarkResult()
        benchmark_query(tmp_path, [50], results)
        # 3 data types * 1 row count * 4 queries = 12 results
        assert len(results.rows) == 12
        for r in results.rows:
            assert r["parquet_time"] > 0
            assert r["vortex_time"] > 0


class TestRunBenchmarks:
    """Test the full benchmark runner."""

    def test_run_small(self, tmp_path):
        """Test running all benchmarks at small scale."""
        results = run_benchmarks(row_counts=[50], tmp_dir=tmp_path)
        # 3 (size) + 3 (write) + 3 (read) + 12 (query) = 21
        assert len(results.rows) == 21

    def test_generate_report(self, tmp_path):
        """Test generating a full report."""
        results = run_benchmarks(row_counts=[50], tmp_dir=tmp_path)
        report = generate_report(results, [50])
        assert "# Parquet vs Vortex" in report
        assert "File Size" in report
        assert "Write Performance" in report
        assert "Read Performance" in report
        assert "Query Performance" in report

    def test_system_info(self):
        """Test system info generation."""
        info = system_info()
        assert "Python" in info
        assert "Platform" in info
        assert "DuckDB" in info
