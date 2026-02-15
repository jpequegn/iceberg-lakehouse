"""Tests for data lineage tracking."""

import json
import pytest
from pathlib import Path

from lakehouse.lineage import (
    record_lineage,
    get_upstream,
    get_downstream,
    get_lineage_graph,
    remove_lineage,
    get_impact_analysis,
)


@pytest.fixture
def lineage_path(tmp_path):
    """Return a temporary lineage store path."""
    return tmp_path / "lineage.json"


# --- record_lineage ---


class TestRecordLineage:
    def test_basic(self, lineage_path):
        """Record a simple lineage edge."""
        result = record_lineage(["expenses"], "spending_report", store_path=lineage_path)
        assert result["sources"] == ["default.expenses"]
        assert result["target"] == "default.spending_report"
        assert result["operation"] == "manual"
        assert "recorded" in result["message"].lower() or "updated" in result["message"].lower()

    def test_multiple_sources(self, lineage_path):
        """Record lineage with multiple sources."""
        result = record_lineage(
            ["expenses", "categories"], "spending_report",
            operation="insert_from",
            store_path=lineage_path,
        )
        assert "default.categories" in result["sources"]
        assert "default.expenses" in result["sources"]

    def test_with_sql(self, lineage_path):
        """Record lineage with SQL."""
        sql = "INSERT INTO report SELECT * FROM expenses JOIN categories"
        result = record_lineage(
            ["expenses", "categories"], "report",
            operation="insert_from", sql=sql,
            store_path=lineage_path,
        )
        assert result["sql"] == sql

    def test_normalizes_names(self, lineage_path):
        """Bare names are auto-prefixed with default."""
        result = record_lineage(["expenses"], "report", store_path=lineage_path)
        assert result["sources"] == ["default.expenses"]
        assert result["target"] == "default.report"

    def test_qualified_names_preserved(self, lineage_path):
        """Qualified names are preserved as-is."""
        result = record_lineage(
            ["analytics.events"], "analytics.summary",
            store_path=lineage_path,
        )
        assert result["sources"] == ["analytics.events"]
        assert result["target"] == "analytics.summary"

    def test_duplicate_updates(self, lineage_path):
        """Recording same edge again updates it."""
        record_lineage(["a"], "b", operation="manual", store_path=lineage_path)
        result = record_lineage(["a"], "b", operation="pipeline", store_path=lineage_path)
        assert result["operation"] == "pipeline"
        assert "updated" in result["message"].lower()

        # Should still be one edge
        graph = get_lineage_graph(store_path=lineage_path)
        assert graph["edge_count"] == 1

    def test_deduplicates_sources(self, lineage_path):
        """Duplicate sources are deduplicated."""
        result = record_lineage(
            ["expenses", "expenses"], "report",
            store_path=lineage_path,
        )
        assert result["sources"] == ["default.expenses"]

    def test_sources_sorted(self, lineage_path):
        """Sources are sorted."""
        result = record_lineage(
            ["zebra", "alpha"], "report",
            store_path=lineage_path,
        )
        assert result["sources"] == ["default.alpha", "default.zebra"]

    def test_empty_sources_raises(self, lineage_path):
        """Empty sources list raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            record_lineage([], "report", store_path=lineage_path)

    def test_empty_target_raises(self, lineage_path):
        """Empty target raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            record_lineage(["a"], "", store_path=lineage_path)

    def test_has_timestamp(self, lineage_path):
        """Edge has a recorded_at timestamp."""
        result = record_lineage(["a"], "b", store_path=lineage_path)
        assert result["recorded_at"] is not None

    def test_persists(self, lineage_path):
        """Lineage persists to file."""
        record_lineage(["a"], "b", store_path=lineage_path)
        assert lineage_path.exists()
        data = json.loads(lineage_path.read_text())
        assert len(data["edges"]) == 1


# --- get_upstream ---


class TestGetUpstream:
    def test_direct(self, lineage_path):
        """Get direct upstream dependencies."""
        record_lineage(["expenses", "categories"], "report", store_path=lineage_path)
        deps = get_upstream("report", store_path=lineage_path, transitive=False)
        tables = [d["table"] for d in deps]
        assert "default.expenses" in tables
        assert "default.categories" in tables

    def test_transitive(self, lineage_path):
        """Get transitive upstream dependencies."""
        # raw_data → cleaned → report
        record_lineage(["raw_data"], "cleaned", store_path=lineage_path)
        record_lineage(["cleaned"], "report", store_path=lineage_path)
        deps = get_upstream("report", store_path=lineage_path)
        tables = [d["table"] for d in deps]
        assert "default.cleaned" in tables
        assert "default.raw_data" in tables

    def test_transitive_depth(self, lineage_path):
        """Transitive deps include correct depth."""
        record_lineage(["a"], "b", store_path=lineage_path)
        record_lineage(["b"], "c", store_path=lineage_path)
        record_lineage(["c"], "d", store_path=lineage_path)
        deps = get_upstream("d", store_path=lineage_path)
        depth_map = {d["table"]: d["depth"] for d in deps}
        assert depth_map["default.c"] == 1
        assert depth_map["default.b"] == 2
        assert depth_map["default.a"] == 3

    def test_no_deps(self, lineage_path):
        """Table with no upstream returns empty."""
        record_lineage(["a"], "b", store_path=lineage_path)
        deps = get_upstream("a", store_path=lineage_path)
        assert deps == []

    def test_cycle_detection(self, lineage_path):
        """Cycles don't cause infinite loops."""
        record_lineage(["a"], "b", store_path=lineage_path)
        record_lineage(["b"], "c", store_path=lineage_path)
        record_lineage(["c"], "a", store_path=lineage_path)  # cycle!
        # Should complete without hanging
        deps = get_upstream("a", store_path=lineage_path)
        tables = [d["table"] for d in deps]
        # Should find upstream of a via the cycle: c → a means c is upstream
        assert "default.c" in tables


# --- get_downstream ---


class TestGetDownstream:
    def test_direct(self, lineage_path):
        """Get direct downstream dependents."""
        record_lineage(["expenses"], "report", store_path=lineage_path)
        record_lineage(["expenses"], "summary", store_path=lineage_path)
        deps = get_downstream("expenses", store_path=lineage_path, transitive=False)
        tables = [d["table"] for d in deps]
        assert "default.report" in tables
        assert "default.summary" in tables

    def test_transitive(self, lineage_path):
        """Get transitive downstream dependents."""
        record_lineage(["a"], "b", store_path=lineage_path)
        record_lineage(["b"], "c", store_path=lineage_path)
        deps = get_downstream("a", store_path=lineage_path)
        tables = [d["table"] for d in deps]
        assert "default.b" in tables
        assert "default.c" in tables

    def test_no_deps(self, lineage_path):
        """Table with no downstream returns empty."""
        record_lineage(["a"], "b", store_path=lineage_path)
        deps = get_downstream("b", store_path=lineage_path)
        assert deps == []


# --- get_lineage_graph ---


class TestGetLineageGraph:
    def test_full_graph(self, lineage_path):
        """Full graph includes all nodes and edges."""
        record_lineage(["a"], "b", store_path=lineage_path)
        record_lineage(["b", "c"], "d", store_path=lineage_path)
        graph = get_lineage_graph(store_path=lineage_path)
        assert graph["node_count"] == 4
        assert graph["edge_count"] == 2
        assert "default.a" in graph["nodes"]
        assert "default.d" in graph["nodes"]

    def test_empty_graph(self, lineage_path):
        """Empty store returns empty graph."""
        graph = get_lineage_graph(store_path=lineage_path)
        assert graph["nodes"] == []
        assert graph["edges"] == []
        assert graph["node_count"] == 0
        assert graph["edge_count"] == 0

    def test_nodes_sorted(self, lineage_path):
        """Nodes are sorted."""
        record_lineage(["zebra"], "alpha", store_path=lineage_path)
        graph = get_lineage_graph(store_path=lineage_path)
        assert graph["nodes"] == ["default.alpha", "default.zebra"]


# --- remove_lineage ---


class TestRemoveLineage:
    def test_remove_existing(self, lineage_path):
        """Remove an existing edge."""
        record_lineage(["a"], "b", store_path=lineage_path)
        result = remove_lineage("a", "b", store_path=lineage_path)
        assert result["removed"] == 1
        graph = get_lineage_graph(store_path=lineage_path)
        assert graph["edge_count"] == 0

    def test_remove_nonexistent(self, lineage_path):
        """Remove a nonexistent edge."""
        result = remove_lineage("a", "b", store_path=lineage_path)
        assert result["removed"] == 0

    def test_remove_preserves_others(self, lineage_path):
        """Remove only the specified edge."""
        record_lineage(["a"], "b", store_path=lineage_path)
        record_lineage(["c"], "d", store_path=lineage_path)
        remove_lineage("a", "b", store_path=lineage_path)
        graph = get_lineage_graph(store_path=lineage_path)
        assert graph["edge_count"] == 1
        assert graph["edges"][0]["target"] == "default.d"


# --- get_impact_analysis ---


class TestImpactAnalysis:
    def test_with_downstream(self, lineage_path):
        """Impact analysis shows affected tables."""
        record_lineage(["a"], "b", store_path=lineage_path)
        record_lineage(["b"], "c", store_path=lineage_path)
        record_lineage(["a"], "d", store_path=lineage_path)
        result = get_impact_analysis("a", store_path=lineage_path)
        assert result["affected_count"] == 3
        assert "default.b" in result["affected_tables"]
        assert "default.c" in result["affected_tables"]
        assert "default.d" in result["affected_tables"]

    def test_no_downstream(self, lineage_path):
        """Impact analysis for leaf table."""
        record_lineage(["a"], "b", store_path=lineage_path)
        result = get_impact_analysis("b", store_path=lineage_path)
        assert result["affected_count"] == 0
        assert "no downstream" in result["message"].lower()

    def test_message(self, lineage_path):
        """Impact message includes count."""
        record_lineage(["a"], "b", store_path=lineage_path)
        result = get_impact_analysis("a", store_path=lineage_path)
        assert "1 table" in result["message"]


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, lineage_path):
        """Lineage store is valid JSON with expected structure."""
        record_lineage(
            ["expenses", "categories"], "report",
            operation="insert_from",
            sql="SELECT * FROM expenses JOIN categories",
            store_path=lineage_path,
        )
        data = json.loads(lineage_path.read_text())
        assert "edges" in data
        assert len(data["edges"]) == 1
        edge = data["edges"][0]
        assert edge["sources"] == ["default.categories", "default.expenses"]
        assert edge["target"] == "default.report"
        assert edge["operation"] == "insert_from"
        assert edge["sql"] is not None
        assert "recorded_at" in edge
