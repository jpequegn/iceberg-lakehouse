"""Tests for Vortex I/O utilities."""

import pytest
import pyarrow as pa

from lakehouse.vortex_io import (
    write_vortex,
    read_vortex,
    arrow_to_vortex,
    vortex_to_arrow,
    vortex_file_info,
)


@pytest.fixture
def sample_table():
    """Create a sample Arrow table with various types."""
    return pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(["alice", "bob", "charlie", "diana", "eve"], type=pa.string()),
        "amount": pa.array([10.5, 20.3, 30.1, 40.9, 50.7], type=pa.float64()),
    })


class TestWriteVortex:
    """Test write_vortex function."""

    def test_write_creates_file(self, tmp_path, sample_table):
        """Test that write creates a .vortex file."""
        path = tmp_path / "out.vortex"
        result = write_vortex(sample_table, path)
        assert path.exists()
        assert result["rows"] == 5
        assert result["size_bytes"] > 0

    def test_write_creates_parent_dirs(self, tmp_path, sample_table):
        """Test that write creates parent directories."""
        path = tmp_path / "sub" / "dir" / "out.vortex"
        write_vortex(sample_table, path)
        assert path.exists()

    def test_write_compact_mode(self, tmp_path, sample_table):
        """Test compact write produces a file."""
        path = tmp_path / "compact.vortex"
        result = write_vortex(sample_table, path, compact=True)
        assert path.exists()
        assert result["rows"] == 5

    def test_write_string_path(self, tmp_path, sample_table):
        """Test write with string path."""
        path = str(tmp_path / "str.vortex")
        result = write_vortex(sample_table, path)
        assert result["path"] == path


class TestReadVortex:
    """Test read_vortex function."""

    def test_roundtrip(self, tmp_path, sample_table):
        """Test write then read preserves data."""
        path = tmp_path / "rt.vortex"
        write_vortex(sample_table, path)
        result = read_vortex(path)

        assert result.num_rows == sample_table.num_rows
        assert result.num_columns == sample_table.num_columns
        assert result.column("id").to_pylist() == sample_table.column("id").to_pylist()
        assert result.column("name").to_pylist() == sample_table.column("name").to_pylist()

    def test_read_nonexistent_raises(self, tmp_path):
        """Test reading non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            read_vortex(tmp_path / "nope.vortex")

    def test_roundtrip_with_nulls(self, tmp_path):
        """Test roundtrip with null values."""
        table = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "val": pa.array([10.0, None, 30.0], type=pa.float64()),
        })
        path = tmp_path / "nulls.vortex"
        write_vortex(table, path)
        result = read_vortex(path)
        assert result.column("val").to_pylist() == [10.0, None, 30.0]

    def test_roundtrip_large_table(self, tmp_path):
        """Test roundtrip with larger dataset."""
        n = 10_000
        table = pa.table({
            "id": pa.array(list(range(n)), type=pa.int64()),
            "val": pa.array([float(i) for i in range(n)], type=pa.float64()),
        })
        path = tmp_path / "large.vortex"
        write_vortex(table, path)
        result = read_vortex(path)
        assert result.num_rows == n


class TestArrowConversion:
    """Test in-memory Arrow <-> Vortex conversion."""

    def test_arrow_to_vortex(self, sample_table):
        """Test converting Arrow table to Vortex array."""
        arr = arrow_to_vortex(sample_table)
        assert len(arr) == 5
        assert arr.nbytes > 0

    def test_vortex_to_arrow(self, sample_table):
        """Test converting Vortex array back to Arrow table."""
        arr = arrow_to_vortex(sample_table)
        result = vortex_to_arrow(arr)
        assert result.num_rows == 5
        assert result.column("id").to_pylist() == [1, 2, 3, 4, 5]

    def test_roundtrip_in_memory(self, sample_table):
        """Test full in-memory roundtrip."""
        arr = arrow_to_vortex(sample_table)
        result = vortex_to_arrow(arr)
        assert result.column("name").to_pylist() == sample_table.column("name").to_pylist()
        assert result.column("amount").to_pylist() == sample_table.column("amount").to_pylist()


class TestVortexFileInfo:
    """Test vortex_file_info function."""

    def test_file_info(self, tmp_path, sample_table):
        """Test getting file metadata."""
        path = tmp_path / "info.vortex"
        write_vortex(sample_table, path)
        info = vortex_file_info(path)
        assert info["size_bytes"] > 0
        assert "dtype" in info

    def test_file_info_nonexistent_raises(self, tmp_path):
        """Test info on non-existent file raises."""
        with pytest.raises(FileNotFoundError):
            vortex_file_info(tmp_path / "nope.vortex")
