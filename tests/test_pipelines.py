"""Tests for data pipelines."""

import json
import pytest
from pathlib import Path

from lakehouse.pipelines import (
    create_pipeline,
    get_pipeline,
    list_pipelines,
    run_pipeline,
    drop_pipeline,
)
from lakehouse.catalog import create_table, insert_rows, list_tables
from lakehouse.query import QueryEngine


@pytest.fixture
def pipe_path(tmp_path):
    """Return a temporary pipelines store path."""
    return tmp_path / "pipelines.json"


@pytest.fixture
def pipe_data(test_catalog):
    """Create source tables with data for pipeline tests."""
    create_table(test_catalog, "raw_events", columns={"id": "long", "category": "string", "amount": "double"})
    insert_rows(test_catalog, "default.raw_events", [
        {"id": 1, "category": "food", "amount": 10.0},
        {"id": 2, "category": "food", "amount": 20.0},
        {"id": 3, "category": "travel", "amount": 30.0},
        {"id": 4, "category": "travel", "amount": 40.0},
    ])
    return test_catalog


# --- create_pipeline ---


class TestCreatePipeline:
    def test_basic(self, pipe_path):
        """Create a pipeline with steps."""
        result = create_pipeline(
            "etl", [{"sql": "SELECT 1 AS val", "target_table": "out"}],
            store_path=pipe_path,
        )
        assert result["name"] == "etl"
        assert result["steps"] == 1
        assert "created" in result["message"].lower()

    def test_with_description(self, pipe_path):
        """Create pipeline with description."""
        result = create_pipeline(
            "desc_pipe", [{"sql": "SELECT 1"}],
            description="My pipeline", store_path=pipe_path,
        )
        assert result["description"] == "My pipeline"

    def test_multiple_steps(self, pipe_path):
        """Create pipeline with multiple steps."""
        steps = [
            {"sql": "SELECT 1 AS val", "target_table": "step1"},
            {"sql": "SELECT * FROM step1", "target_table": "step2"},
        ]
        result = create_pipeline("multi", steps, store_path=pipe_path)
        assert result["steps"] == 2

    def test_duplicate_raises(self, pipe_path):
        """Duplicate name raises ValueError."""
        create_pipeline("dupe", [{"sql": "SELECT 1"}], store_path=pipe_path)
        with pytest.raises(ValueError, match="already exists"):
            create_pipeline("dupe", [{"sql": "SELECT 2"}], store_path=pipe_path)

    def test_empty_name_raises(self, pipe_path):
        """Empty name raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            create_pipeline("", [{"sql": "SELECT 1"}], store_path=pipe_path)

    def test_empty_steps_raises(self, pipe_path):
        """Empty steps raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            create_pipeline("test", [], store_path=pipe_path)

    def test_empty_sql_raises(self, pipe_path):
        """Step with empty SQL raises ValueError."""
        with pytest.raises(ValueError, match="empty SQL"):
            create_pipeline("test", [{"sql": ""}], store_path=pipe_path)

    def test_invalid_mode_raises(self, pipe_path):
        """Invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="invalid mode"):
            create_pipeline("test", [{"sql": "SELECT 1", "mode": "bad"}], store_path=pipe_path)


# --- get_pipeline ---


class TestGetPipeline:
    def test_basic(self, pipe_path):
        """Get a pipeline."""
        create_pipeline("get_me", [{"sql": "SELECT 1"}], description="desc", store_path=pipe_path)
        result = get_pipeline("get_me", store_path=pipe_path)
        assert result["name"] == "get_me"
        assert result["description"] == "desc"
        assert len(result["steps"]) == 1
        assert result["last_run"] is None

    def test_nonexistent_raises(self, pipe_path):
        """Nonexistent pipeline raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            get_pipeline("no_such", store_path=pipe_path)


# --- list_pipelines ---


class TestListPipelines:
    def test_empty(self, pipe_path):
        """Empty store returns empty list."""
        assert list_pipelines(store_path=pipe_path) == []

    def test_with_pipelines(self, pipe_path):
        """List created pipelines."""
        create_pipeline("p1", [{"sql": "SELECT 1"}], store_path=pipe_path)
        create_pipeline("p2", [{"sql": "SELECT 2"}], store_path=pipe_path)
        pipelines = list_pipelines(store_path=pipe_path)
        assert len(pipelines) == 2
        names = [p["name"] for p in pipelines]
        assert "p1" in names
        assert "p2" in names

    def test_includes_fields(self, pipe_path):
        """List includes expected fields."""
        create_pipeline("check", [{"sql": "SELECT 1"}], store_path=pipe_path)
        pipelines = list_pipelines(store_path=pipe_path)
        p = pipelines[0]
        assert "name" in p
        assert "step_count" in p
        assert "created_at" in p
        assert "last_run" in p
        assert "last_run_status" in p


# --- run_pipeline ---


class TestRunPipeline:
    def test_single_step(self, pipe_data, pipe_path):
        """Run a single-step pipeline."""
        create_pipeline(
            "single",
            [{"sql": "SELECT category, SUM(amount) AS total FROM raw_events GROUP BY category", "target_table": "summary"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        result = run_pipeline("single", pipe_data, engine, store_path=pipe_path)
        assert result["steps_completed"] == 1
        assert result["steps_failed"] == 0
        assert result["step_results"][0]["rows_affected"] == 2
        assert "completed" in result["message"].lower()

    def test_multi_step(self, pipe_data, pipe_path):
        """Run a multi-step pipeline."""
        steps = [
            {"sql": "SELECT * FROM raw_events WHERE category = 'food'", "target_table": "food_only"},
            {"sql": "SELECT * FROM raw_events WHERE category = 'travel'", "target_table": "travel_only"},
        ]
        create_pipeline("multi", steps, store_path=pipe_path)
        engine = QueryEngine(catalog=pipe_data)
        result = run_pipeline("multi", pipe_data, engine, store_path=pipe_path)
        assert result["steps_completed"] == 2
        assert result["steps_failed"] == 0
        assert result["step_results"][0]["rows_affected"] == 2
        assert result["step_results"][1]["rows_affected"] == 2

    def test_failure_stops_execution(self, pipe_data, pipe_path):
        """Step failure stops the pipeline."""
        steps = [
            {"sql": "SELECT * FROM nonexistent_table", "target_table": "out"},
            {"sql": "SELECT 1 AS val", "target_table": "out2"},
        ]
        create_pipeline("fail", steps, store_path=pipe_path)
        engine = QueryEngine(catalog=pipe_data)
        result = run_pipeline("fail", pipe_data, engine, store_path=pipe_path)
        assert result["steps_failed"] == 1
        assert result["steps_completed"] == 0
        # Second step should not have run
        assert len(result["step_results"]) == 1

    def test_updates_last_run(self, pipe_data, pipe_path):
        """Running a pipeline updates last_run metadata."""
        create_pipeline(
            "tracked",
            [{"sql": "SELECT * FROM raw_events", "target_table": "out"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        run_pipeline("tracked", pipe_data, engine, store_path=pipe_path)
        p = get_pipeline("tracked", store_path=pipe_path)
        assert p["last_run"] is not None
        assert p["last_run_status"] == "completed"

    def test_failed_status(self, pipe_data, pipe_path):
        """Failed pipeline records failed status."""
        create_pipeline(
            "will_fail",
            [{"sql": "SELECT * FROM nonexistent", "target_table": "out"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        run_pipeline("will_fail", pipe_data, engine, store_path=pipe_path)
        p = get_pipeline("will_fail", store_path=pipe_path)
        assert p["last_run_status"] == "failed"

    def test_nonexistent_raises(self, pipe_data, pipe_path):
        """Run nonexistent pipeline raises ValueError."""
        engine = QueryEngine(catalog=pipe_data)
        with pytest.raises(ValueError, match="not found"):
            run_pipeline("no_such", pipe_data, engine, store_path=pipe_path)

    def test_overwrite_mode(self, pipe_data, pipe_path):
        """Overwrite mode replaces existing data."""
        # Create target with initial data
        create_table(pipe_data, "overwrite_target", columns={"id": "long", "category": "string", "amount": "double"})
        insert_rows(pipe_data, "default.overwrite_target", [{"id": 99, "category": "old", "amount": 1.0}])

        create_pipeline(
            "overwrite",
            [{"sql": "SELECT * FROM raw_events WHERE category = 'food'", "target_table": "overwrite_target", "mode": "overwrite"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        result = run_pipeline("overwrite", pipe_data, engine, store_path=pipe_path)
        assert result["step_results"][0]["rows_affected"] == 2

        # Verify old data was replaced
        engine2 = QueryEngine(catalog=pipe_data)
        df = engine2.execute("SELECT * FROM overwrite_target")
        assert len(df) == 2
        assert "old" not in df["category"].values

    def test_append_mode(self, pipe_data, pipe_path):
        """Append mode adds to existing data."""
        create_table(pipe_data, "append_target", columns={"id": "long", "category": "string", "amount": "double"})
        insert_rows(pipe_data, "default.append_target", [{"id": 99, "category": "old", "amount": 1.0}])

        create_pipeline(
            "append",
            [{"sql": "SELECT * FROM raw_events WHERE category = 'food'", "target_table": "append_target", "mode": "append"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        run_pipeline("append", pipe_data, engine, store_path=pipe_path)

        engine2 = QueryEngine(catalog=pipe_data)
        df = engine2.execute("SELECT * FROM append_target")
        assert len(df) == 3  # 1 old + 2 new

    def test_no_target_table(self, pipe_data, pipe_path):
        """Step without target_table just executes SQL."""
        create_pipeline(
            "no_target",
            [{"sql": "SELECT * FROM raw_events"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        result = run_pipeline("no_target", pipe_data, engine, store_path=pipe_path)
        assert result["steps_completed"] == 1
        assert result["step_results"][0]["rows_affected"] == 4


# --- dry_run ---


class TestDryRun:
    def test_validates_without_executing(self, pipe_data, pipe_path):
        """Dry run validates SQL without creating tables."""
        create_pipeline(
            "dry",
            [{"sql": "SELECT * FROM raw_events", "target_table": "dry_out"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        result = run_pipeline("dry", pipe_data, engine, dry_run=True, store_path=pipe_path)
        assert result["dry_run"] is True
        assert result["step_results"][0]["status"] == "validated"

        # Target table should not exist
        tables = list_tables(pipe_data)
        assert "default.dry_out" not in tables

    def test_dry_run_catches_errors(self, pipe_data, pipe_path):
        """Dry run catches SQL errors."""
        create_pipeline(
            "dry_fail",
            [{"sql": "SELECT * FROM nonexistent_table", "target_table": "out"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        result = run_pipeline("dry_fail", pipe_data, engine, dry_run=True, store_path=pipe_path)
        assert result["step_results"][0]["status"] == "error"

    def test_dry_run_does_not_update_metadata(self, pipe_data, pipe_path):
        """Dry run does not update last_run metadata."""
        create_pipeline(
            "dry_meta",
            [{"sql": "SELECT * FROM raw_events"}],
            store_path=pipe_path,
        )
        engine = QueryEngine(catalog=pipe_data)
        run_pipeline("dry_meta", pipe_data, engine, dry_run=True, store_path=pipe_path)
        p = get_pipeline("dry_meta", store_path=pipe_path)
        assert p["last_run"] is None


# --- drop_pipeline ---


class TestDropPipeline:
    def test_drop(self, pipe_path):
        """Drop removes pipeline."""
        create_pipeline("to_drop", [{"sql": "SELECT 1"}], store_path=pipe_path)
        result = drop_pipeline("to_drop", store_path=pipe_path)
        assert "dropped" in result["message"].lower()
        assert list_pipelines(store_path=pipe_path) == []

    def test_drop_nonexistent_raises(self, pipe_path):
        """Drop nonexistent raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            drop_pipeline("no_such", store_path=pipe_path)


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, pipe_path):
        """Store is valid JSON with expected structure."""
        create_pipeline(
            "json_check",
            [{"sql": "SELECT 1 AS val", "target_table": "out", "mode": "overwrite"}],
            description="test",
            store_path=pipe_path,
        )
        data = json.loads(pipe_path.read_text())
        assert "json_check" in data
        entry = data["json_check"]
        assert entry["description"] == "test"
        assert len(entry["steps"]) == 1
        assert entry["steps"][0]["sql"] == "SELECT 1 AS val"
        assert entry["steps"][0]["target_table"] == "out"
        assert "created_at" in entry
        assert entry["last_run"] is None
