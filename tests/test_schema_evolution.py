"""Tests for schema evolution tracking."""

import pytest

from lakehouse.schema_evolution import (
    get_schema_history,
    schema_diff,
    generate_migration,
    check_schema_compatibility,
)
from lakehouse.catalog import create_table, insert_rows, alter_table


@pytest.fixture
def evo_table(test_catalog):
    """Create a table and evolve its schema."""
    create_table(test_catalog, "evo_test", columns={"id": "long", "name": "string"})
    insert_rows(test_catalog, "default.evo_test", [{"id": 1, "name": "Alice"}])
    return test_catalog


@pytest.fixture
def evolved_table(evo_table):
    """Create a table, add data, then add a column."""
    alter_table(evo_table, "evo_test", "add_column", "email", column_type="string")
    insert_rows(evo_table, "default.evo_test", [{"id": 2, "name": "Bob", "email": "bob@test.com"}])
    return evo_table


# --- get_schema_history ---


class TestGetSchemaHistory:
    def test_single_schema(self, evo_table):
        """Table with one schema version returns one entry."""
        history = get_schema_history(evo_table, "evo_test")
        assert len(history) >= 1
        assert history[0]["schema_id"] == 0
        field_names = [f["name"] for f in history[0]["fields"]]
        assert "id" in field_names
        assert "name" in field_names

    def test_evolved_schema(self, evolved_table):
        """Table with schema evolution shows multiple entries."""
        history = get_schema_history(evolved_table, "evo_test")
        assert len(history) >= 2
        # Last entry should have the email column
        last = history[-1]
        field_names = [f["name"] for f in last["fields"]]
        assert "email" in field_names

    def test_change_summary(self, evolved_table):
        """Schema change entry includes a summary."""
        history = get_schema_history(evolved_table, "evo_test")
        changes = [h for h in history if h["is_schema_change"]]
        assert len(changes) >= 1
        assert changes[0]["change_summary"] is not None
        assert "email" in changes[0]["change_summary"]

    def test_nonexistent_raises(self, evo_table):
        """Nonexistent table raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            get_schema_history(evo_table, "no_such_table")

    def test_includes_field_ids(self, evo_table):
        """History entries include field IDs."""
        history = get_schema_history(evo_table, "evo_test")
        for field in history[0]["fields"]:
            assert "field_id" in field


# --- schema_diff ---


class TestSchemaDiff:
    def test_no_changes(self, evo_table):
        """Single schema version returns empty diff."""
        diff = schema_diff(evo_table, "evo_test")
        assert diff["added_columns"] == []
        assert diff["dropped_columns"] == []
        assert diff["renamed_columns"] == []
        assert diff["type_changes"] == []
        assert "no changes" in diff["summary"]

    def test_added_column(self, evolved_table):
        """Diff detects added column."""
        diff = schema_diff(evolved_table, "evo_test")
        added_names = [c["name"] for c in diff["added_columns"]]
        assert "email" in added_names

    def test_dropped_column(self, evolved_table):
        """Diff detects dropped column."""
        # Drop a column to create another schema version
        alter_table(evolved_table, "evo_test", "drop_column", "name")
        insert_rows(evolved_table, "default.evo_test", [{"id": 3, "email": "c@test.com"}])
        diff = schema_diff(evolved_table, "evo_test")
        dropped_names = [c["name"] for c in diff["dropped_columns"]]
        assert "name" in dropped_names

    def test_between_specific_snapshots(self, evolved_table):
        """Diff between specific snapshots."""
        table = evolved_table.load_table("default.evo_test")
        snapshots = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms)
        first = snapshots[0].snapshot_id
        last = snapshots[-1].snapshot_id
        diff = schema_diff(evolved_table, "evo_test", from_snapshot=first, to_snapshot=last)
        assert diff["table"] == "default.evo_test"

    def test_nonexistent_raises(self, evo_table):
        """Nonexistent table raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            schema_diff(evo_table, "no_such")

    def test_invalid_snapshot_raises(self, evo_table):
        """Invalid snapshot ID raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            schema_diff(evo_table, "evo_test", from_snapshot=99999, to_snapshot=88888)


# --- generate_migration ---


class TestGenerateMigration:
    def test_empty_migration(self, evo_table):
        """No schema changes produces empty migration."""
        result = generate_migration(evo_table, "evo_test")
        assert result["step_count"] == 0
        assert result["steps"] == []

    def test_add_column_migration(self, evolved_table):
        """Migration includes add_column step."""
        result = generate_migration(evolved_table, "evo_test")
        add_steps = [s for s in result["steps"] if s["operation"] == "add_column"]
        assert len(add_steps) >= 1
        assert add_steps[0]["column_name"] == "email"
        assert add_steps[0]["column_type"] == "string"

    def test_drop_column_migration(self, evolved_table):
        """Migration includes drop_column step."""
        alter_table(evolved_table, "evo_test", "drop_column", "name")
        insert_rows(evolved_table, "default.evo_test", [{"id": 3, "email": "c@test.com"}])
        result = generate_migration(evolved_table, "evo_test")
        drop_steps = [s for s in result["steps"] if s["operation"] == "drop_column"]
        assert len(drop_steps) >= 1
        assert drop_steps[0]["column_name"] == "name"

    def test_message(self, evolved_table):
        """Migration result includes a message."""
        result = generate_migration(evolved_table, "evo_test")
        assert "migration" in result["message"].lower()


# --- check_schema_compatibility ---


class TestCheckSchemaCompatibility:
    def test_safe_add(self, evo_table):
        """Adding a column is compatible."""
        result = check_schema_compatibility(
            evo_table, "evo_test",
            [{"op": "add_column", "column": "email", "type": "string"}],
        )
        assert result["compatible"] is True
        assert result["breaking_changes"] == []

    def test_drop_column_warning(self, evo_table):
        """Dropping any column produces a warning (Iceberg v2 fields are optional by default)."""
        result = check_schema_compatibility(
            evo_table, "evo_test",
            [{"op": "drop_column", "column": "id"}],
        )
        assert result["compatible"] is True
        assert len(result["warnings"]) >= 1

    def test_drop_optional_warning(self, evolved_table):
        """Dropping an optional column produces a warning but is compatible."""
        result = check_schema_compatibility(
            evolved_table, "evo_test",
            [{"op": "drop_column", "column": "email"}],
        )
        assert result["compatible"] is True
        assert len(result["warnings"]) >= 1

    def test_rename_warning(self, evo_table):
        """Renaming a column produces a warning."""
        result = check_schema_compatibility(
            evo_table, "evo_test",
            [{"op": "rename_column", "column": "name", "new_name": "full_name"}],
        )
        assert result["compatible"] is True
        assert len(result["warnings"]) >= 1

    def test_duplicate_column_warning(self, evo_table):
        """Adding an existing column produces a warning."""
        result = check_schema_compatibility(
            evo_table, "evo_test",
            [{"op": "add_column", "column": "id", "type": "long"}],
        )
        assert len(result["warnings"]) >= 1

    def test_nonexistent_column_warning(self, evo_table):
        """Dropping a nonexistent column produces a warning."""
        result = check_schema_compatibility(
            evo_table, "evo_test",
            [{"op": "drop_column", "column": "nonexistent"}],
        )
        assert len(result["warnings"]) >= 1

    def test_nonexistent_table_raises(self, evo_table):
        """Nonexistent table raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            check_schema_compatibility(evo_table, "no_such", [])

    def test_message(self, evo_table):
        """Result includes a message."""
        result = check_schema_compatibility(
            evo_table, "evo_test",
            [{"op": "add_column", "column": "new_col", "type": "string"}],
        )
        assert "compatible" in result["message"].lower()
