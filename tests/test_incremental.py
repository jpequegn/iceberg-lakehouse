"""Tests for incremental processing (watermark-based pipeline runs)."""

import json
import pytest

from lakehouse.incremental import (
    set_watermark,
    get_watermark,
    list_watermarks,
    reset_watermark,
    get_incremental_data,
    run_pipeline_incremental,
)
from lakehouse.pipelines import create_pipeline
from lakehouse.catalog import create_table, insert_rows
from lakehouse.query import QueryEngine


@pytest.fixture
def wm_path(tmp_path):
    """Return a temporary watermark store path."""
    return tmp_path / "watermarks.json"


@pytest.fixture
def pipe_path(tmp_path):
    """Return a temporary pipeline store path."""
    return tmp_path / "pipelines.json"


@pytest.fixture
def inc_table(test_catalog):
    """Create a table with initial data (3 rows)."""
    create_table(test_catalog, "events", columns={"id": "long", "event": "string"})
    insert_rows(test_catalog, "default.events", [
        {"id": 1, "event": "click"},
        {"id": 2, "event": "view"},
        {"id": 3, "event": "purchase"},
    ])
    return test_catalog


# --- set/get watermark ---


class TestWatermark:
    def test_set_and_get(self, wm_path):
        """Set and retrieve a watermark."""
        set_watermark("etl", "events", 12345, rows_processed=100, store_path=wm_path)
        result = get_watermark("etl", "events", store_path=wm_path)
        assert result["snapshot_id"] == 12345
        assert result["rows_processed"] == 100

    def test_get_nonexistent(self, wm_path):
        """Get a nonexistent watermark returns None snapshot."""
        result = get_watermark("nope", "nope", store_path=wm_path)
        assert result["snapshot_id"] is None

    def test_overwrite(self, wm_path):
        """Setting a watermark again overwrites."""
        set_watermark("etl", "events", 111, store_path=wm_path)
        set_watermark("etl", "events", 222, store_path=wm_path)
        result = get_watermark("etl", "events", store_path=wm_path)
        assert result["snapshot_id"] == 222


# --- list_watermarks ---


class TestListWatermarks:
    def test_list_all(self, wm_path):
        """List all watermarks."""
        set_watermark("a", "t1", 1, store_path=wm_path)
        set_watermark("b", "t2", 2, store_path=wm_path)
        results = list_watermarks(store_path=wm_path)
        assert len(results) == 2

    def test_list_by_pipeline(self, wm_path):
        """List watermarks filtered by pipeline."""
        set_watermark("a", "t1", 1, store_path=wm_path)
        set_watermark("a", "t2", 2, store_path=wm_path)
        set_watermark("b", "t3", 3, store_path=wm_path)
        results = list_watermarks(pipeline_name="a", store_path=wm_path)
        assert len(results) == 2

    def test_list_empty(self, wm_path):
        """Empty store returns empty list."""
        assert list_watermarks(store_path=wm_path) == []


# --- reset_watermark ---


class TestResetWatermark:
    def test_reset_table(self, wm_path):
        """Reset watermark for a specific table."""
        set_watermark("etl", "events", 123, store_path=wm_path)
        reset_watermark("etl", table_name="events", store_path=wm_path)
        result = get_watermark("etl", "events", store_path=wm_path)
        assert result["snapshot_id"] is None

    def test_reset_all(self, wm_path):
        """Reset all watermarks for a pipeline."""
        set_watermark("etl", "t1", 1, store_path=wm_path)
        set_watermark("etl", "t2", 2, store_path=wm_path)
        reset_watermark("etl", store_path=wm_path)
        assert list_watermarks(pipeline_name="etl", store_path=wm_path) == []

    def test_reset_nonexistent(self, wm_path):
        """Resetting nonexistent pipeline is a no-op."""
        result = reset_watermark("nope", store_path=wm_path)
        assert "no watermarks" in result["message"].lower()


# --- get_incremental_data ---


class TestGetIncrementalData:
    def test_no_watermark_returns_all(self, inc_table, wm_path):
        """No watermark returns all rows (full scan)."""
        result = get_incremental_data(inc_table, "events", "etl", store_path=wm_path)
        assert result["row_count"] == 3
        assert result["is_full"] is True

    def test_incremental_returns_new_rows(self, inc_table, wm_path):
        """With watermark, returns only new rows."""
        # Get current snapshot and set as watermark
        table = inc_table.load_table("default.events")
        snap_id = table.current_snapshot().snapshot_id
        set_watermark("etl", "events", snap_id, store_path=wm_path)

        # Add new rows
        insert_rows(inc_table, "default.events", [
            {"id": 4, "event": "signup"},
            {"id": 5, "event": "logout"},
        ])

        result = get_incremental_data(inc_table, "events", "etl", store_path=wm_path)
        assert result["row_count"] == 2
        assert result["is_full"] is False

    def test_no_new_data(self, inc_table, wm_path):
        """Same snapshot returns 0 rows."""
        table = inc_table.load_table("default.events")
        snap_id = table.current_snapshot().snapshot_id
        set_watermark("etl", "events", snap_id, store_path=wm_path)

        result = get_incremental_data(inc_table, "events", "etl", store_path=wm_path)
        assert result["row_count"] == 0


# --- run_pipeline_incremental ---


class TestRunPipelineIncremental:
    def test_full_first_run(self, inc_table, wm_path, pipe_path):
        """First run processes all data."""
        create_pipeline(
            "etl",
            steps=[{"source_table": "events", "target_table": "events_copy", "sql": "SELECT * FROM events"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=inc_table)
        result = run_pipeline_incremental("etl", inc_table, engine, store_path=pipe_path, watermark_path=wm_path)
        assert result["status"] == "success"
        assert result["total_rows"] == 3
        # Watermark should be updated
        wm = get_watermark("etl", "events", store_path=wm_path)
        assert wm["snapshot_id"] is not None

    def test_incremental_second_run(self, inc_table, wm_path, pipe_path):
        """Second run processes only new data."""
        create_pipeline(
            "etl",
            steps=[{"source_table": "events", "target_table": "events_copy", "sql": "SELECT * FROM events"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=inc_table)

        # First run
        run_pipeline_incremental("etl", inc_table, engine, store_path=pipe_path, watermark_path=wm_path)

        # Add new data
        insert_rows(inc_table, "default.events", [{"id": 10, "event": "new_event"}])

        # Second run should only process new data
        result = run_pipeline_incremental("etl", inc_table, engine, store_path=pipe_path, watermark_path=wm_path)
        assert result["status"] == "success"
        assert result["total_rows"] == 1

    def test_skip_when_no_new_data(self, inc_table, wm_path, pipe_path):
        """Skip steps when no new data."""
        create_pipeline(
            "etl",
            steps=[{"source_table": "events", "target_table": "events_copy", "sql": "SELECT * FROM events"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=inc_table)

        # First run
        run_pipeline_incremental("etl", inc_table, engine, store_path=pipe_path, watermark_path=wm_path)

        # Second run without new data
        result = run_pipeline_incremental("etl", inc_table, engine, store_path=pipe_path, watermark_path=wm_path)
        assert result["steps"][0]["status"] == "skipped"
        assert result["total_rows"] == 0

    def test_nonexistent_pipeline_raises(self, inc_table, wm_path, pipe_path):
        """Nonexistent pipeline raises ValueError."""
        engine = QueryEngine(catalog=inc_table)
        with pytest.raises(ValueError, match="not found"):
            run_pipeline_incremental("nope", inc_table, engine, store_path=pipe_path, watermark_path=wm_path)


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, wm_path):
        """Store is valid JSON with expected structure."""
        set_watermark("etl", "events", 12345, rows_processed=100, store_path=wm_path)
        data = json.loads(wm_path.read_text())
        assert "etl" in data
        assert "default.events" in data["etl"]
        entry = data["etl"]["default.events"]
        assert entry["snapshot_id"] == 12345
        assert "processed_at" in entry
        assert entry["rows_processed"] == 100
