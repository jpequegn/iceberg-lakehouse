"""Tests for table cloning and branching."""

import json
import pytest
from pathlib import Path

from lakehouse.cloning import (
    clone_table,
    list_clones,
    promote_clone,
    discard_clone,
)
from lakehouse.catalog import (
    create_table,
    insert_rows,
    get_table_schema,
    list_tables,
    get_snapshots,
)
from lakehouse.query import QueryEngine


@pytest.fixture
def clones_path(tmp_path):
    """Return a temporary clones store path."""
    return tmp_path / "clones.json"


@pytest.fixture
def source_table(test_catalog):
    """Create a source table with data for cloning tests."""
    create_table(test_catalog, "clone_source", columns={"id": "long", "name": "string", "value": "double"})
    insert_rows(test_catalog, "default.clone_source", [
        {"id": 1, "name": "Alice", "value": 10.0},
        {"id": 2, "name": "Bob", "value": 20.0},
        {"id": 3, "name": "Charlie", "value": 30.0},
    ])
    return "clone_source"


# --- clone_table ---


class TestCloneTable:
    def test_basic_clone(self, test_catalog, source_table, clones_path):
        """Clone a table with data."""
        result = clone_table(test_catalog, source_table, "clone_target", store_path=clones_path)
        assert result["source"] == "default.clone_source"
        assert result["target"] == "default.clone_target"
        assert result["row_count"] == 3
        assert "cloned" in result["message"].lower()

    def test_clone_has_same_data(self, test_catalog, source_table, clones_path):
        """Clone has same row count and data."""
        clone_table(test_catalog, source_table, "clone_data_check", store_path=clones_path)
        engine = QueryEngine(catalog=test_catalog)
        original = engine.execute("SELECT * FROM clone_source ORDER BY id")
        cloned = engine.execute("SELECT * FROM clone_data_check ORDER BY id")
        assert len(original) == len(cloned)
        assert list(original["id"]) == list(cloned["id"])

    def test_clone_has_same_schema(self, test_catalog, source_table, clones_path):
        """Clone has same schema as source."""
        clone_table(test_catalog, source_table, "clone_schema_check", store_path=clones_path)
        src_schema = get_table_schema(test_catalog, "clone_source")
        tgt_schema = get_table_schema(test_catalog, "clone_schema_check")
        src_fields = [(f["name"], f["type"]) for f in src_schema["fields"]]
        tgt_fields = [(f["name"], f["type"]) for f in tgt_schema["fields"]]
        assert src_fields == tgt_fields

    def test_clone_is_independent(self, test_catalog, source_table, clones_path):
        """Inserting into clone doesn't affect original."""
        clone_table(test_catalog, source_table, "clone_indep", store_path=clones_path)
        insert_rows(test_catalog, "default.clone_indep", [
            {"id": 4, "name": "Diana", "value": 40.0},
        ])
        engine = QueryEngine(catalog=test_catalog)
        original = engine.execute("SELECT * FROM clone_source")
        cloned = engine.execute("SELECT * FROM clone_indep")
        assert len(original) == 3
        assert len(cloned) == 4

    def test_clone_from_snapshot(self, test_catalog, clones_path):
        """Clone from a historical snapshot."""
        create_table(test_catalog, "snap_source", columns={"id": "long", "val": "string"})
        insert_rows(test_catalog, "default.snap_source", [{"id": 1, "val": "v1"}])
        snapshots = get_snapshots(test_catalog, "snap_source")
        first_snap_id = str(snapshots[0]["snapshot_id"])

        # Add more data
        insert_rows(test_catalog, "default.snap_source", [{"id": 2, "val": "v2"}])

        # Clone from first snapshot
        result = clone_table(
            test_catalog, "snap_source", "snap_clone",
            as_of=first_snap_id, store_path=clones_path,
        )
        assert result["row_count"] == 1
        assert result["as_of"] == first_snap_id

    def test_clone_nonexistent_source_raises(self, test_catalog, clones_path):
        """Clone of non-existent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            clone_table(test_catalog, "no_such_table", "clone_out", store_path=clones_path)

    def test_clone_to_existing_name_raises(self, test_catalog, source_table, clones_path):
        """Clone to an existing table name raises error."""
        with pytest.raises(ValueError, match="already exists"):
            clone_table(test_catalog, source_table, source_table, store_path=clones_path)

    def test_clone_empty_table(self, test_catalog, clones_path):
        """Clone an empty table."""
        create_table(test_catalog, "empty_src", columns={"id": "long"})
        result = clone_table(test_catalog, "empty_src", "empty_clone", store_path=clones_path)
        assert result["row_count"] == 0

    def test_has_source_snapshot_id(self, test_catalog, source_table, clones_path):
        """Clone result includes source snapshot ID."""
        result = clone_table(test_catalog, source_table, "snap_id_check", store_path=clones_path)
        assert result["source_snapshot_id"] is not None


# --- list_clones ---


class TestListClones:
    def test_empty(self, clones_path):
        """List clones when none exist."""
        assert list_clones(store_path=clones_path) == []

    def test_with_clones(self, test_catalog, source_table, clones_path):
        """List active clones."""
        clone_table(test_catalog, source_table, "c1", store_path=clones_path)
        clone_table(test_catalog, source_table, "c2", store_path=clones_path)
        clones = list_clones(store_path=clones_path)
        assert len(clones) == 2
        names = [c["clone"] for c in clones]
        assert "default.c1" in names
        assert "default.c2" in names

    def test_includes_fields(self, test_catalog, source_table, clones_path):
        """Clone entries include expected fields."""
        clone_table(test_catalog, source_table, "c_fields", store_path=clones_path)
        clones = list_clones(store_path=clones_path)
        c = clones[0]
        assert "clone" in c
        assert "source_table" in c
        assert "cloned_at" in c
        assert "row_count" in c


# --- promote_clone ---


class TestPromoteClone:
    def test_promote(self, test_catalog, source_table, clones_path):
        """Promote clone replaces original data."""
        clone_table(test_catalog, source_table, "promo_clone", store_path=clones_path)
        # Modify clone
        insert_rows(test_catalog, "default.promo_clone", [
            {"id": 99, "name": "New", "value": 99.0},
        ])
        result = promote_clone(test_catalog, "promo_clone", source_table, store_path=clones_path)
        assert result["row_count"] == 4  # 3 original + 1 added
        assert "promoted" in result["message"].lower()

        # Original should now have 4 rows
        engine = QueryEngine(catalog=test_catalog)
        df = engine.execute("SELECT * FROM clone_source")
        assert len(df) == 4

    def test_promote_removes_from_clones_list(self, test_catalog, source_table, clones_path):
        """Promote removes the clone from metadata."""
        clone_table(test_catalog, source_table, "promo_rem", store_path=clones_path)
        promote_clone(test_catalog, "promo_rem", source_table, store_path=clones_path)
        clones = list_clones(store_path=clones_path)
        names = [c["clone"] for c in clones]
        assert "default.promo_rem" not in names

    def test_promote_nonexistent_clone_raises(self, test_catalog, clones_path):
        """Promote of nonexistent clone raises error."""
        with pytest.raises(ValueError, match="not found"):
            promote_clone(test_catalog, "no_such_clone", "expenses", store_path=clones_path)


# --- discard_clone ---


class TestDiscardClone:
    def test_discard(self, test_catalog, source_table, clones_path):
        """Discard removes clone table."""
        clone_table(test_catalog, source_table, "to_discard", store_path=clones_path)
        result = discard_clone(test_catalog, "to_discard", store_path=clones_path)
        assert "discarded" in result["message"].lower()

        # Table should be gone
        tables = list_tables(test_catalog)
        assert "default.to_discard" not in tables

    def test_discard_removes_from_metadata(self, test_catalog, source_table, clones_path):
        """Discard removes clone from metadata store."""
        clone_table(test_catalog, source_table, "disc_meta", store_path=clones_path)
        discard_clone(test_catalog, "disc_meta", store_path=clones_path)
        clones = list_clones(store_path=clones_path)
        assert len(clones) == 0

    def test_discard_nonexistent_raises(self, test_catalog, clones_path):
        """Discard of nonexistent table raises error."""
        with pytest.raises(ValueError, match="Failed"):
            discard_clone(test_catalog, "no_such", store_path=clones_path)


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, test_catalog, source_table, clones_path):
        """Clones store is valid JSON with expected structure."""
        clone_table(test_catalog, source_table, "json_check", store_path=clones_path)
        data = json.loads(clones_path.read_text())
        assert "default.json_check" in data
        entry = data["default.json_check"]
        assert entry["source_table"] == "default.clone_source"
        assert entry["row_count"] == 3
        assert "cloned_at" in entry
        assert "source_snapshot_id" in entry
