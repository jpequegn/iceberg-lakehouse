"""Tests for Vortex integration with DuckDB query engine."""

import pytest
import pyarrow as pa

from lakehouse.vortex_io import write_vortex
from lakehouse.query import QueryEngine


@pytest.fixture
def vortex_file(tmp_path):
    """Create a sample Vortex file for testing."""
    table = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(["alice", "bob", "charlie", "diana", "eve"], type=pa.string()),
        "amount": pa.array([10.5, 20.3, 30.1, 40.9, 50.7], type=pa.float64()),
    })
    path = tmp_path / "test_data.vortex"
    write_vortex(table, path)
    return path


@pytest.fixture
def engine():
    """Create a standalone QueryEngine (no catalog needed for Vortex queries)."""
    return QueryEngine.__new__(QueryEngine)


class TestHasVortex:
    """Test DuckDB Vortex extension detection."""

    def test_has_vortex_returns_bool(self):
        """Test that has_vortex returns a boolean."""
        engine = QueryEngine.__new__(QueryEngine)
        engine.catalog = None
        engine.warehouse = None
        engine._conn = None
        engine._vortex_available = None

        # Accessing has_vortex triggers connection + extension detection
        result = engine.has_vortex
        assert isinstance(result, bool)

    def test_has_vortex_cached(self):
        """Test that _vortex_available is cached after first check."""
        engine = QueryEngine.__new__(QueryEngine)
        engine._vortex_available = True
        assert engine.has_vortex is True

        engine._vortex_available = False
        assert engine.has_vortex is False


class TestQueryVortex:
    """Test query_vortex method for direct Vortex file queries."""

    def test_select_all(self, vortex_file):
        """Test SELECT * from a Vortex file."""
        engine = QueryEngine.__new__(QueryEngine)
        engine._conn = None
        engine._vortex_available = None

        result = engine.query_vortex("SELECT * FROM data", vortex_file)
        assert len(result) == 5
        assert "id" in result.columns
        assert "name" in result.columns
        assert "amount" in result.columns

    def test_select_with_filter(self, vortex_file):
        """Test SELECT with WHERE clause."""
        engine = QueryEngine.__new__(QueryEngine)
        engine._conn = None
        engine._vortex_available = None

        result = engine.query_vortex("SELECT * FROM data WHERE amount > 25.0", vortex_file)
        assert len(result) == 3
        assert all(v > 25.0 for v in result["amount"].tolist())

    def test_aggregate_query(self, vortex_file):
        """Test aggregate query on Vortex file."""
        engine = QueryEngine.__new__(QueryEngine)
        engine._conn = None
        engine._vortex_available = None

        result = engine.query_vortex("SELECT count(*) as cnt, sum(amount) as total FROM data", vortex_file)
        assert len(result) == 1
        assert result["cnt"].iloc[0] == 5

    def test_custom_table_name(self, vortex_file):
        """Test using a custom table name."""
        engine = QueryEngine.__new__(QueryEngine)
        engine._conn = None
        engine._vortex_available = None

        result = engine.query_vortex(
            "SELECT * FROM my_table WHERE id <= 2",
            vortex_file,
            table_name="my_table",
        )
        assert len(result) == 2

    def test_max_rows_limit(self, vortex_file):
        """Test that max_rows limits results."""
        engine = QueryEngine.__new__(QueryEngine)
        engine._conn = None
        engine._vortex_available = None

        result = engine.query_vortex("SELECT * FROM data", vortex_file, max_rows=3)
        assert len(result) == 3

    def test_nonexistent_file_raises(self, tmp_path):
        """Test querying non-existent file raises FileNotFoundError."""
        engine = QueryEngine.__new__(QueryEngine)
        engine._conn = None
        engine._vortex_available = None

        with pytest.raises(FileNotFoundError):
            engine.query_vortex("SELECT * FROM data", tmp_path / "nope.vortex")

    def test_order_by(self, vortex_file):
        """Test ORDER BY on Vortex file."""
        engine = QueryEngine.__new__(QueryEngine)
        engine._conn = None
        engine._vortex_available = None

        result = engine.query_vortex(
            "SELECT name, amount FROM data ORDER BY amount DESC LIMIT 3",
            vortex_file,
        )
        assert len(result) == 3
        amounts = result["amount"].tolist()
        assert amounts == sorted(amounts, reverse=True)


class TestRegisterVortex:
    """Test register_vortex method."""

    def test_register_and_query(self, vortex_file):
        """Test registering a Vortex file and querying it."""
        engine = QueryEngine.__new__(QueryEngine)
        engine.catalog = None
        engine.warehouse = None
        engine._conn = None
        engine._vortex_available = None

        engine.register_vortex("test_table", vortex_file)
        conn = engine._get_connection()

        result = conn.execute("SELECT count(*) as cnt FROM test_table").fetchdf()
        assert result["cnt"].iloc[0] == 5

    def test_register_nonexistent_raises(self, tmp_path):
        """Test registering non-existent file raises FileNotFoundError."""
        engine = QueryEngine.__new__(QueryEngine)
        engine.catalog = None
        engine.warehouse = None
        engine._conn = None
        engine._vortex_available = None

        with pytest.raises(FileNotFoundError):
            engine.register_vortex("nope", tmp_path / "nope.vortex")

    def test_register_multiple_files(self, tmp_path):
        """Test registering multiple Vortex files."""
        # Create two Vortex files
        t1 = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        t2 = pa.table({"y": pa.array([10, 20], type=pa.int64())})

        p1 = tmp_path / "t1.vortex"
        p2 = tmp_path / "t2.vortex"
        write_vortex(t1, p1)
        write_vortex(t2, p2)

        engine = QueryEngine.__new__(QueryEngine)
        engine.catalog = None
        engine.warehouse = None
        engine._conn = None
        engine._vortex_available = None

        engine.register_vortex("table_a", p1)
        engine.register_vortex("table_b", p2)

        conn = engine._get_connection()
        r1 = conn.execute("SELECT count(*) as cnt FROM table_a").fetchdf()
        r2 = conn.execute("SELECT count(*) as cnt FROM table_b").fetchdf()
        assert r1["cnt"].iloc[0] == 3
        assert r2["cnt"].iloc[0] == 2

    def test_register_overwrite(self, tmp_path):
        """Test re-registering with same name replaces the table."""
        t1 = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        t2 = pa.table({"x": pa.array([10, 20, 30], type=pa.int64())})

        p1 = tmp_path / "v1.vortex"
        p2 = tmp_path / "v2.vortex"
        write_vortex(t1, p1)
        write_vortex(t2, p2)

        engine = QueryEngine.__new__(QueryEngine)
        engine.catalog = None
        engine.warehouse = None
        engine._conn = None
        engine._vortex_available = None

        engine.register_vortex("data", p1)
        conn = engine._get_connection()
        r1 = conn.execute("SELECT count(*) as cnt FROM data").fetchdf()
        assert r1["cnt"].iloc[0] == 2

        engine.register_vortex("data", p2)
        r2 = conn.execute("SELECT count(*) as cnt FROM data").fetchdf()
        assert r2["cnt"].iloc[0] == 3
