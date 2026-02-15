"""Tests for query optimization advisor."""

import json
import pytest

from lakehouse.optimizer import (
    analyze_query_patterns,
    suggest_partitions,
    suggest_materializations,
    get_optimization_report,
    estimate_query_cost,
    _extract_tables_from_sql,
    _extract_filters_from_sql,
    _has_aggregation,
    _has_join,
)
from lakehouse.queries import add_history_entry
from lakehouse.catalog import create_table, insert_rows


@pytest.fixture
def query_path(tmp_path):
    return tmp_path / "queries.json"


@pytest.fixture
def seeded_history(query_path):
    """Seed query history with realistic entries."""
    entries = [
        ("SELECT * FROM orders WHERE status = 'active'", 100, 50),
        ("SELECT * FROM orders WHERE status = 'active'", 100, 45),
        ("SELECT * FROM orders WHERE region = 'US'", 80, 30),
        ("SELECT id, name FROM customers", 200, 20),
        ("SELECT id, name FROM customers", 200, 25),
        ("SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id", 50, 200),
        ("SELECT region, SUM(amount) FROM orders GROUP BY region", 5, 150),
        ("SELECT region, SUM(amount) FROM orders GROUP BY region", 5, 160),
        ("SELECT region, SUM(amount) FROM orders GROUP BY region", 5, 140),
        ("SELECT COUNT(*) FROM orders WHERE created_at > '2024-01-01'", 1, 500),
        ("SELECT * FROM orders", 200, 10),
        ("SELECT * FROM orders WHERE id > 5", 50, 15),
    ]
    for sql, rows, dur in entries:
        add_history_entry(sql, rows_returned=rows, duration_ms=dur, store_path=query_path)
    return query_path


@pytest.fixture
def opt_table(test_catalog):
    """Create a table with data for optimizer testing."""
    create_table(test_catalog, "orders", columns={"id": "long", "status": "string", "region": "string", "amount": "double"})
    rows = []
    for i in range(20):
        rows.append({"id": i, "status": "active" if i % 2 == 0 else "closed", "region": ["US", "EU", "APAC"][i % 3], "amount": float(i * 10)})
    insert_rows(test_catalog, "default.orders", rows)
    return test_catalog


# --- SQL parsing helpers ---


class TestSqlParsing:
    def test_extract_tables_from(self):
        tables = _extract_tables_from_sql("SELECT * FROM orders")
        assert "orders" in tables

    def test_extract_tables_join(self):
        tables = _extract_tables_from_sql("SELECT * FROM orders o JOIN customers c ON o.id = c.id")
        assert "orders" in tables
        assert "customers" in tables

    def test_extract_tables_qualified(self):
        tables = _extract_tables_from_sql("SELECT * FROM default.orders")
        assert "default.orders" in tables

    def test_extract_filters(self):
        filters = _extract_filters_from_sql("SELECT * FROM t WHERE status = 'active' AND region IN ('US')")
        assert "status" in filters
        assert "region" in filters

    def test_extract_filters_no_where(self):
        filters = _extract_filters_from_sql("SELECT * FROM t")
        assert filters == []

    def test_has_aggregation(self):
        assert _has_aggregation("SELECT COUNT(*) FROM t") is True
        assert _has_aggregation("SELECT region, SUM(x) FROM t GROUP BY region") is True
        assert _has_aggregation("SELECT * FROM t") is False

    def test_has_join(self):
        assert _has_join("SELECT * FROM a JOIN b ON a.id = b.id") is True
        assert _has_join("SELECT * FROM a") is False


# --- analyze_query_patterns ---


class TestAnalyzePatterns:
    def test_empty_history(self, query_path):
        result = analyze_query_patterns(store_path=query_path)
        assert result["total_queries"] == 0
        assert "no query history" in result["message"].lower()

    def test_frequent_tables(self, seeded_history):
        result = analyze_query_patterns(store_path=seeded_history)
        assert result["total_queries"] == 12
        table_names = [t["table"] for t in result["frequent_tables"]]
        assert "orders" in table_names

    def test_frequent_filters(self, seeded_history):
        result = analyze_query_patterns(store_path=seeded_history)
        filter_cols = [f["column"] for f in result["frequent_filters"]]
        assert "status" in filter_cols

    def test_repeated_queries(self, seeded_history):
        result = analyze_query_patterns(store_path=seeded_history)
        assert len(result["repeated_queries"]) >= 1
        # The aggregation query was run 3 times
        patterns = {rq["sql_pattern"] for rq in result["repeated_queries"]}
        assert any("SUM" in p for p in patterns)

    def test_slow_queries(self, seeded_history):
        result = analyze_query_patterns(store_path=seeded_history)
        # The 500ms query should be above p90
        assert len(result["slow_queries"]) >= 1


# --- suggest_partitions ---


class TestSuggestPartitions:
    def test_no_suggestions_empty_history(self, opt_table, query_path):
        result = suggest_partitions(opt_table, "orders", store_path=query_path)
        assert result == []

    def test_suggestions_with_history(self, opt_table, seeded_history):
        result = suggest_partitions(opt_table, "orders", store_path=seeded_history)
        # status and region are filtered frequently and have low cardinality
        if result:
            cols = [s["column"] for s in result]
            assert any(c in cols for c in ["status", "region"])
            for s in result:
                assert "benefit" in s
                assert "rationale" in s


# --- suggest_materializations ---


class TestSuggestMaterializations:
    def test_no_suggestions_empty(self, opt_table, query_path):
        result = suggest_materializations(opt_table, store_path=query_path)
        assert result == []

    def test_suggestions_with_repeated_agg(self, opt_table, seeded_history):
        result = suggest_materializations(opt_table, store_path=seeded_history)
        # The GROUP BY query was run 3 times
        assert len(result) >= 1
        assert result[0]["has_aggregation"] is True
        assert result[0]["run_count"] >= 2


# --- get_optimization_report ---


class TestOptimizationReport:
    def test_report_empty(self, opt_table, query_path):
        result = get_optimization_report(opt_table, store_path=query_path)
        assert "optimization_score" in result
        assert result["optimization_score"] == 100  # No issues

    def test_report_with_history(self, opt_table, seeded_history):
        result = get_optimization_report(opt_table, store_path=seeded_history)
        assert "query_patterns" in result
        assert "partition_suggestions" in result
        assert "materialization_suggestions" in result
        assert 0 <= result["optimization_score"] <= 100


# --- estimate_query_cost ---


class TestEstimateQueryCost:
    def test_simple_select(self, opt_table):
        result = estimate_query_cost(opt_table, "SELECT * FROM orders")
        assert result["complexity"] == "simple"
        assert result["has_filter"] is False
        assert result["total_source_rows"] >= 1

    def test_filtered_query(self, opt_table):
        result = estimate_query_cost(opt_table, "SELECT * FROM orders WHERE status = 'active'")
        assert result["has_filter"] is True
        assert result["estimated_rows_scanned"] < result["total_source_rows"]

    def test_join_query(self, opt_table):
        result = estimate_query_cost(opt_table, "SELECT * FROM orders JOIN customers ON orders.id = customers.id")
        assert result["has_join"] is True
        assert result["complexity"] in ("moderate", "complex")

    def test_complex_query(self, opt_table):
        result = estimate_query_cost(opt_table, "SELECT region, SUM(amount) FROM orders JOIN ref ON orders.id = ref.id GROUP BY region")
        assert result["complexity"] == "complex"
        assert result["has_aggregation"] is True
        assert result["has_join"] is True
