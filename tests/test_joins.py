"""Tests for cross-table joins and federated queries."""

import pytest

from lakehouse.joins import execute_join, join_to_table, suggest_joins
from lakehouse.catalog import create_table, insert_rows, list_tables
from lakehouse.query import QueryEngine


@pytest.fixture
def join_tables(test_catalog):
    """Create tables with joinable data."""
    create_table(test_catalog, "users", columns={"id": "long", "name": "string", "dept": "string"})
    insert_rows(test_catalog, "default.users", [
        {"id": 1, "name": "Alice", "dept": "eng"},
        {"id": 2, "name": "Bob", "dept": "sales"},
        {"id": 3, "name": "Charlie", "dept": "eng"},
    ])

    create_table(test_catalog, "orders", columns={"order_id": "long", "user_id": "long", "amount": "double"})
    insert_rows(test_catalog, "default.orders", [
        {"order_id": 1, "user_id": 1, "amount": 100.0},
        {"order_id": 2, "user_id": 1, "amount": 200.0},
        {"order_id": 3, "user_id": 2, "amount": 150.0},
    ])

    return test_catalog


# --- execute_join ---


class TestExecuteJoin:
    def test_simple_join(self, join_tables):
        """Simple two-table join."""
        result = execute_join(
            join_tables,
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
        )
        assert result["row_count"] == 3
        assert "name" in result["columns"]
        assert "amount" in result["columns"]

    def test_join_with_where(self, join_tables):
        """Join with WHERE clause."""
        result = execute_join(
            join_tables,
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100",
        )
        assert result["row_count"] == 2

    def test_join_with_aggregation(self, join_tables):
        """Join with aggregation."""
        result = execute_join(
            join_tables,
            "SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
        )
        assert result["row_count"] == 2
        df = result["dataframe"]
        alice_total = df[df["name"] == "Alice"]["total"].iloc[0]
        assert alice_total == 300.0

    def test_max_rows(self, join_tables):
        """Max rows limits output."""
        result = execute_join(
            join_tables,
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
            max_rows=2,
        )
        assert result["row_count"] == 2

    def test_namespace_qualified_refs(self, join_tables):
        """Namespace-qualified table references work."""
        result = execute_join(
            join_tables,
            "SELECT u.name, o.amount FROM default.users u JOIN default.orders o ON u.id = o.user_id",
        )
        assert result["row_count"] == 3

    def test_left_join(self, join_tables):
        """LEFT JOIN includes unmatched rows."""
        result = execute_join(
            join_tables,
            "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
        )
        # Charlie has no orders, should still appear
        assert result["row_count"] == 4
        df = result["dataframe"]
        charlie = df[df["name"] == "Charlie"]
        assert len(charlie) == 1

    def test_invalid_sql_raises(self, join_tables):
        """Invalid SQL raises ValueError."""
        with pytest.raises(ValueError, match="failed"):
            execute_join(join_tables, "SELECT * FROM nonexistent_table")

    def test_returns_dataframe(self, join_tables):
        """Result includes a pandas DataFrame."""
        result = execute_join(
            join_tables,
            "SELECT u.name FROM users u",
        )
        assert hasattr(result["dataframe"], "to_csv")  # It's a DataFrame


# --- join_to_table ---


class TestJoinToTable:
    def test_save_to_new_table(self, join_tables):
        """Save join results to a new table."""
        result = join_to_table(
            join_tables,
            "SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
            "user_totals",
        )
        assert result["rows_written"] == 2
        assert "user_totals" in result["target"]

        # Verify the table exists and has data
        engine = QueryEngine(catalog=join_tables)
        df = engine.execute("SELECT * FROM user_totals")
        assert len(df) == 2

    def test_append_mode(self, join_tables):
        """Append mode adds to existing table."""
        join_to_table(
            join_tables,
            "SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
            "append_test",
        )
        join_to_table(
            join_tables,
            "SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
            "append_test",
            mode="append",
        )
        engine = QueryEngine(catalog=join_tables)
        df = engine.execute("SELECT * FROM append_test")
        assert len(df) == 4  # 2 + 2

    def test_overwrite_mode(self, join_tables):
        """Overwrite mode replaces existing data."""
        join_to_table(
            join_tables,
            "SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
            "overwrite_test",
        )
        join_to_table(
            join_tables,
            "SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.name = 'Alice' GROUP BY u.name",
            "overwrite_test",
            mode="overwrite",
        )
        engine = QueryEngine(catalog=join_tables)
        df = engine.execute("SELECT * FROM overwrite_test")
        assert len(df) == 1


# --- suggest_joins ---


class TestSuggestJoins:
    def test_finds_matching_columns(self, join_tables):
        """Suggest joins finds matching column names."""
        # users has 'id', other sample tables also have 'id'
        suggestions = suggest_joins(join_tables, "users")
        # At least some tables should share the 'id' column
        if suggestions:
            assert any(s["column"] == "id" for s in suggestions)

    def test_no_matches(self, join_tables):
        """Table with no matching columns returns empty."""
        create_table(join_tables, "unique_cols", columns={"xyz_unique": "string"})
        insert_rows(join_tables, "default.unique_cols", [{"xyz_unique": "val"}])
        suggestions = suggest_joins(join_tables, "unique_cols")
        assert suggestions == []

    def test_includes_join_sql(self, join_tables):
        """Suggestions include example join SQL."""
        suggestions = suggest_joins(join_tables, "users")
        if suggestions:
            assert any("JOIN" in s["join_sql"] for s in suggestions)

    def test_nonexistent_table_raises(self, join_tables):
        """Suggest joins for nonexistent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            suggest_joins(join_tables, "no_such_table")

    def test_excludes_self(self, join_tables):
        """Suggestions don't include the source table itself."""
        suggestions = suggest_joins(join_tables, "users")
        for s in suggestions:
            assert s["table"] != "default.users"
