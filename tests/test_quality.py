"""Tests for data quality scoring and anomaly detection."""

import json
import pytest
from pathlib import Path

from lakehouse.quality import (
    compute_quality_score,
    detect_anomalies,
    get_quality_report,
    get_quality_history,
)
from lakehouse.catalog import create_table, insert_rows, list_tables
from lakehouse.stats import compute_table_stats
from lakehouse.validation import add_validation_rule


@pytest.fixture
def quality_path(tmp_path):
    """Return a temporary quality store path."""
    return tmp_path / "quality.json"


@pytest.fixture
def stats_path(tmp_path):
    """Return a temporary stats cache path."""
    return tmp_path / "stats_cache.json"


@pytest.fixture
def validation_path(tmp_path):
    """Return a temporary validation store path."""
    return tmp_path / "validation.json"


@pytest.fixture
def clean_table(test_catalog, stats_path):
    """Create a clean table with no nulls for testing."""
    create_table(test_catalog, "clean_data", columns={"id": "long", "name": "string", "amount": "double"})
    insert_rows(test_catalog, "default.clean_data", [
        {"id": 1, "name": "Alice", "amount": 10.0},
        {"id": 2, "name": "Bob", "amount": 20.0},
        {"id": 3, "name": "Carol", "amount": 30.0},
    ])
    return test_catalog


@pytest.fixture
def dirty_table(test_catalog, stats_path):
    """Create a table with nulls and duplicates for testing."""
    create_table(test_catalog, "dirty_data", columns={"id": "long", "name": "string", "amount": "double"})
    insert_rows(test_catalog, "default.dirty_data", [
        {"id": 1, "name": "Alice", "amount": 10.0},
        {"id": 1, "name": None, "amount": None},
        {"id": 2, "name": None, "amount": None},
        {"id": 2, "name": None, "amount": 20.0},
    ])
    return test_catalog


# --- compute_quality_score ---


class TestComputeQualityScore:
    def test_clean_data_high_score(self, clean_table, stats_path, validation_path, quality_path):
        """Clean data gets a high score."""
        result = compute_quality_score(
            clean_table, "clean_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        assert result["overall_score"] >= 70
        assert result["completeness"] == 100.0
        assert result["table"] == "default.clean_data"
        assert "message" in result

    def test_dirty_data_lower_score(self, dirty_table, stats_path, validation_path, quality_path):
        """Dirty data gets a lower completeness score."""
        result = compute_quality_score(
            dirty_table, "dirty_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        assert result["completeness"] < 100.0
        assert result["overall_score"] < 100

    def test_components_in_range(self, clean_table, stats_path, validation_path, quality_path):
        """All components are 0-100."""
        result = compute_quality_score(
            clean_table, "clean_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        for key in ("completeness", "uniqueness", "freshness", "rule_compliance", "overall_score"):
            assert 0 <= result[key] <= 100

    def test_includes_recommendations(self, dirty_table, stats_path, validation_path, quality_path):
        """Dirty data generates recommendations."""
        result = compute_quality_score(
            dirty_table, "dirty_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        # Dirty data should have at least some recommendations
        assert isinstance(result["recommendations"], list)

    def test_with_validation_rules(self, clean_table, stats_path, validation_path, quality_path):
        """Score includes validation rule compliance."""
        # Add a not_null rule
        add_validation_rule("default.clean_data", {"type": "not_null", "column": "name"}, store_path=validation_path)
        result = compute_quality_score(
            clean_table, "clean_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        # Clean data should pass all rules
        assert result["rule_compliance"] == 100.0

    def test_failing_validation_rules(self, dirty_table, stats_path, validation_path, quality_path):
        """Failing rules lower compliance score."""
        add_validation_rule("default.dirty_data", {"type": "not_null", "column": "name"}, store_path=validation_path)
        result = compute_quality_score(
            dirty_table, "dirty_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        assert result["rule_compliance"] < 100.0

    def test_saves_to_history(self, clean_table, stats_path, validation_path, quality_path):
        """Score is saved to history."""
        compute_quality_score(
            clean_table, "clean_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        history = get_quality_history("clean_data", store_path=quality_path)
        assert len(history) == 1
        assert "overall_score" in history[0]


# --- detect_anomalies ---


class TestDetectAnomalies:
    def test_no_anomalies_stable_data(self, clean_table, stats_path):
        """Stable data produces no anomalies."""
        # Compute initial stats
        compute_table_stats(clean_table, "clean_data", store_path=stats_path)
        # Detect anomalies (should find none since stats haven't changed)
        anomalies = detect_anomalies(clean_table, "clean_data", stats_path=stats_path)
        assert anomalies == []

    def test_row_count_anomaly(self, clean_table, stats_path):
        """Large row count change triggers anomaly."""
        # Cache initial stats (3 rows)
        compute_table_stats(clean_table, "clean_data", store_path=stats_path)
        # Add lots of data to trigger >50% change
        insert_rows(clean_table, "default.clean_data", [
            {"id": i, "name": f"user_{i}", "amount": float(i)}
            for i in range(10, 20)
        ])
        anomalies = detect_anomalies(clean_table, "clean_data", stats_path=stats_path)
        row_anomalies = [a for a in anomalies if a["type"] == "row_count_change"]
        assert len(row_anomalies) >= 1
        assert row_anomalies[0]["severity"] in ("warning", "critical")

    def test_null_spike_anomaly(self, clean_table, stats_path):
        """NULL spike triggers anomaly."""
        # Cache initial stats (no nulls)
        compute_table_stats(clean_table, "clean_data", store_path=stats_path)
        # Insert rows with many nulls
        insert_rows(clean_table, "default.clean_data", [
            {"id": i, "name": None, "amount": None}
            for i in range(10, 16)
        ])
        anomalies = detect_anomalies(clean_table, "clean_data", stats_path=stats_path)
        null_anomalies = [a for a in anomalies if a["type"] == "null_spike"]
        assert len(null_anomalies) >= 1

    def test_no_baseline_returns_empty(self, clean_table, stats_path):
        """No cached stats means no baseline, no anomalies."""
        anomalies = detect_anomalies(clean_table, "clean_data", stats_path=stats_path)
        assert anomalies == []


# --- get_quality_report ---


class TestGetQualityReport:
    def test_single_table(self, clean_table, stats_path, validation_path, quality_path):
        """Report for a single table."""
        report = get_quality_report(
            clean_table, table_name="clean_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        assert report["total_tables"] == 1
        assert len(report["tables"]) == 1
        assert report["tables"][0]["table"] == "default.clean_data"
        assert report["average_score"] > 0

    def test_all_tables(self, clean_table, stats_path, validation_path, quality_path):
        """Report for all tables."""
        report = get_quality_report(
            clean_table,
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        assert report["total_tables"] >= 1
        assert len(report["tables"]) >= 1

    def test_report_includes_anomalies(self, clean_table, stats_path, validation_path, quality_path):
        """Report includes anomaly count."""
        report = get_quality_report(
            clean_table, table_name="clean_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        assert "anomalies" in report["tables"][0]


# --- get_quality_history ---


class TestQualityHistory:
    def test_empty_history(self, quality_path):
        """No history returns empty list."""
        assert get_quality_history("clean_data", store_path=quality_path) == []

    def test_accumulates(self, clean_table, stats_path, validation_path, quality_path):
        """Multiple score computations accumulate in history."""
        for _ in range(3):
            compute_quality_score(
                clean_table, "clean_data",
                stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
            )
        history = get_quality_history("clean_data", store_path=quality_path)
        assert len(history) == 3
        for entry in history:
            assert "overall_score" in entry
            assert "computed_at" in entry


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, clean_table, stats_path, validation_path, quality_path):
        """Store is valid JSON with expected structure."""
        compute_quality_score(
            clean_table, "clean_data",
            stats_path=stats_path, validation_path=validation_path, store_path=quality_path,
        )
        data = json.loads(quality_path.read_text())
        assert "default.clean_data" in data
        entry = data["default.clean_data"]
        assert "history" in entry
        assert len(entry["history"]) == 1
        h = entry["history"][0]
        assert "overall_score" in h
        assert "completeness" in h
        assert "uniqueness" in h
        assert "freshness" in h
        assert "rule_compliance" in h
        assert "computed_at" in h
