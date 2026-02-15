"""Tests for table SLA monitoring."""

import json
import pytest

from lakehouse.sla import (
    set_sla,
    get_sla,
    list_slas,
    remove_sla,
    check_sla,
    get_sla_history,
)
from lakehouse.catalog import create_table, insert_rows


@pytest.fixture
def sla_path(tmp_path):
    return tmp_path / "slas.json"


@pytest.fixture
def quality_path(tmp_path):
    return tmp_path / "quality.json"


@pytest.fixture
def stats_path(tmp_path):
    return tmp_path / "stats_cache.json"


@pytest.fixture
def validation_path(tmp_path):
    return tmp_path / "validation.json"


@pytest.fixture
def sla_table(test_catalog):
    """Create a table with data for SLA testing."""
    create_table(test_catalog, "metrics", columns={"id": "long", "value": "double", "name": "string"})
    insert_rows(test_catalog, "default.metrics", [
        {"id": 1, "value": 10.0, "name": "a"},
        {"id": 2, "value": 20.0, "name": "b"},
        {"id": 3, "value": 30.0, "name": "c"},
    ])
    return test_catalog


@pytest.fixture
def dirty_table(test_catalog):
    """Create a table with nulls for SLA violation testing."""
    create_table(test_catalog, "dirty", columns={"id": "long", "name": "string"})
    insert_rows(test_catalog, "default.dirty", [
        {"id": 1, "name": None},
        {"id": 2, "name": None},
        {"id": 3, "name": "ok"},
    ])
    return test_catalog


# --- set/get SLA ---


class TestSetGetSla:
    def test_set_and_get(self, sla_path):
        """Set and retrieve an SLA."""
        result = set_sla("metrics", {"max_staleness_hours": 24, "min_quality_score": 80}, store_path=sla_path)
        assert result["table"] == "default.metrics"
        assert result["sla"]["max_staleness_hours"] == 24

        got = get_sla("metrics", store_path=sla_path)
        assert got["sla"]["min_quality_score"] == 80

    def test_get_nonexistent(self, sla_path):
        """Get a nonexistent SLA returns None."""
        result = get_sla("nope", store_path=sla_path)
        assert result["sla"] is None

    def test_empty_name_raises(self, sla_path):
        """Empty table name raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            set_sla("", {}, store_path=sla_path)

    def test_invalid_staleness_raises(self, sla_path):
        """Invalid staleness raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            set_sla("t", {"max_staleness_hours": -1}, store_path=sla_path)

    def test_invalid_quality_raises(self, sla_path):
        """Invalid quality score raises ValueError."""
        with pytest.raises(ValueError, match="between 0"):
            set_sla("t", {"min_quality_score": 200}, store_path=sla_path)


# --- list/remove SLA ---


class TestListRemoveSla:
    def test_list(self, sla_path):
        """List all SLAs."""
        set_sla("a", {"max_staleness_hours": 12}, store_path=sla_path)
        set_sla("b", {"min_row_count": 100}, store_path=sla_path)
        slas = list_slas(store_path=sla_path)
        assert len(slas) == 2

    def test_list_empty(self, sla_path):
        """Empty store returns empty list."""
        assert list_slas(store_path=sla_path) == []

    def test_remove(self, sla_path):
        """Remove an SLA."""
        set_sla("t", {"max_staleness_hours": 24}, store_path=sla_path)
        remove_sla("t", store_path=sla_path)
        assert get_sla("t", store_path=sla_path)["sla"] is None

    def test_remove_nonexistent(self, sla_path):
        """Remove nonexistent SLA is a no-op."""
        result = remove_sla("nope", store_path=sla_path)
        assert "no sla found" in result["message"].lower()


# --- check SLA ---


class TestCheckSla:
    def test_passing(self, sla_table, sla_path, quality_path, stats_path, validation_path):
        """Fresh table with good data passes SLA."""
        set_sla("metrics", {"max_staleness_hours": 24, "min_row_count": 1}, store_path=sla_path)
        result = check_sla(
            sla_table, "metrics",
            store_path=sla_path, quality_path=quality_path, stats_path=stats_path, validation_path=validation_path,
        )
        assert result["tables"][0]["status"] == "passing"
        assert result["passing"] == 1

    def test_row_count_violation(self, sla_table, sla_path, quality_path, stats_path, validation_path):
        """Row count below minimum triggers violation."""
        set_sla("metrics", {"min_row_count": 1000}, store_path=sla_path)
        result = check_sla(
            sla_table, "metrics",
            store_path=sla_path, quality_path=quality_path, stats_path=stats_path, validation_path=validation_path,
        )
        assert result["tables"][0]["status"] == "violation"
        assert any("row count" in v.lower() for v in result["tables"][0]["violations"])

    def test_null_pct_violation(self, dirty_table, sla_path, quality_path, stats_path, validation_path):
        """High null percentage triggers violation."""
        set_sla("dirty", {"max_null_pct": 10.0}, store_path=sla_path)
        result = check_sla(
            dirty_table, "dirty",
            store_path=sla_path, quality_path=quality_path, stats_path=stats_path, validation_path=validation_path,
        )
        assert result["tables"][0]["status"] == "violation"
        assert any("null" in v.lower() for v in result["tables"][0]["violations"])

    def test_check_all_tables(self, sla_table, sla_path, quality_path, stats_path, validation_path):
        """Check all tables with SLAs."""
        set_sla("metrics", {"min_row_count": 1}, store_path=sla_path)
        result = check_sla(
            sla_table,
            store_path=sla_path, quality_path=quality_path, stats_path=stats_path, validation_path=validation_path,
        )
        assert result["total"] >= 1

    def test_recommendations(self, sla_table, sla_path, quality_path, stats_path, validation_path):
        """Violations include recommendations."""
        set_sla("metrics", {"min_row_count": 1000}, store_path=sla_path)
        result = check_sla(
            sla_table, "metrics",
            store_path=sla_path, quality_path=quality_path, stats_path=stats_path, validation_path=validation_path,
        )
        assert len(result["tables"][0]["recommendations"]) >= 1


# --- SLA History ---


class TestSlaHistory:
    def test_history_accumulates(self, sla_table, sla_path, quality_path, stats_path, validation_path):
        """SLA check history accumulates."""
        set_sla("metrics", {"min_row_count": 1}, store_path=sla_path)
        for _ in range(3):
            check_sla(
                sla_table, "metrics",
                store_path=sla_path, quality_path=quality_path, stats_path=stats_path, validation_path=validation_path,
            )
        history = get_sla_history("metrics", store_path=sla_path)
        assert len(history) == 3
        for entry in history:
            assert "checked_at" in entry
            assert "status" in entry

    def test_empty_history(self, sla_path):
        """No history returns empty list."""
        assert get_sla_history("nope", store_path=sla_path) == []


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, sla_path):
        """Store is valid JSON with expected structure."""
        set_sla("t", {"max_staleness_hours": 24, "min_quality_score": 80}, store_path=sla_path)
        data = json.loads(sla_path.read_text())
        assert "default.t" in data
        entry = data["default.t"]
        assert "max_staleness_hours" in entry
        assert "min_quality_score" in entry
        assert "created_at" in entry
        assert "check_history" in entry
