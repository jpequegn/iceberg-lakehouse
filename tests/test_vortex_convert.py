"""Tests for Vortex format conversion utilities."""

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from lakehouse.vortex_io import (
    convert_parquet_to_vortex,
    convert_vortex_to_parquet,
    convert_table_to_vortex,
    write_vortex,
    read_vortex,
)


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample Parquet file."""
    table = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
        "amount": pa.array([10.0, 20.0, 30.0, 40.0, 50.0], type=pa.float64()),
    })
    path = tmp_path / "sample.parquet"
    pq.write_table(table, str(path))
    return path


@pytest.fixture
def sample_vortex(tmp_path):
    """Create a sample Vortex file."""
    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "value": pa.array([100.0, 200.0, 300.0], type=pa.float64()),
    })
    path = tmp_path / "sample.vortex"
    write_vortex(table, path)
    return path


class TestParquetToVortex:
    """Test convert_parquet_to_vortex function."""

    def test_convert_default_output(self, sample_parquet):
        """Test conversion with default output path."""
        result = convert_parquet_to_vortex(sample_parquet)
        assert result["rows"] == 5
        assert result["output"].endswith(".vortex")
        assert result["input_size"] > 0
        assert result["output_size"] > 0

        # Verify the output file is readable
        table = read_vortex(result["output"])
        assert table.num_rows == 5

    def test_convert_custom_output(self, sample_parquet, tmp_path):
        """Test conversion with custom output path."""
        out = tmp_path / "custom" / "out.vortex"
        result = convert_parquet_to_vortex(sample_parquet, out)
        assert result["output"] == str(out)
        assert out.exists()

    def test_convert_compact(self, sample_parquet):
        """Test compact conversion."""
        result = convert_parquet_to_vortex(sample_parquet, compact=True)
        assert result["rows"] == 5
        assert result["output_size"] > 0

    def test_convert_nonexistent_raises(self, tmp_path):
        """Test conversion of non-existent file raises."""
        with pytest.raises(FileNotFoundError):
            convert_parquet_to_vortex(tmp_path / "nope.parquet")

    def test_roundtrip_data_integrity(self, sample_parquet):
        """Test that data survives Parquet -> Vortex conversion."""
        original = pq.read_table(str(sample_parquet))
        result = convert_parquet_to_vortex(sample_parquet)
        converted = read_vortex(result["output"])

        assert converted.column("id").to_pylist() == original.column("id").to_pylist()
        assert converted.column("name").to_pylist() == original.column("name").to_pylist()
        assert converted.column("amount").to_pylist() == original.column("amount").to_pylist()


class TestVortexToParquet:
    """Test convert_vortex_to_parquet function."""

    def test_convert_default_output(self, sample_vortex):
        """Test conversion with default output path."""
        result = convert_vortex_to_parquet(sample_vortex)
        assert result["rows"] == 3
        assert result["output"].endswith(".parquet")

        # Verify the output file is readable
        table = pq.read_table(result["output"])
        assert table.num_rows == 3

    def test_convert_custom_output(self, sample_vortex, tmp_path):
        """Test conversion with custom output path."""
        out = tmp_path / "custom" / "out.parquet"
        result = convert_vortex_to_parquet(sample_vortex, out)
        assert result["output"] == str(out)
        assert out.exists()

    def test_convert_nonexistent_raises(self, tmp_path):
        """Test conversion of non-existent file raises."""
        with pytest.raises(FileNotFoundError):
            convert_vortex_to_parquet(tmp_path / "nope.vortex")

    def test_roundtrip_data_integrity(self, sample_vortex):
        """Test that data survives Vortex -> Parquet conversion."""
        original = read_vortex(sample_vortex)
        result = convert_vortex_to_parquet(sample_vortex)
        converted = pq.read_table(result["output"])

        assert converted.column("id").to_pylist() == original.column("id").to_pylist()
        assert converted.column("value").to_pylist() == original.column("value").to_pylist()


class TestFullRoundtrip:
    """Test Parquet -> Vortex -> Parquet roundtrip."""

    def test_parquet_vortex_parquet(self, sample_parquet, tmp_path):
        """Test full Parquet -> Vortex -> Parquet roundtrip."""
        original = pq.read_table(str(sample_parquet))

        # Parquet -> Vortex
        vortex_path = tmp_path / "step1.vortex"
        convert_parquet_to_vortex(sample_parquet, vortex_path)

        # Vortex -> Parquet
        parquet_path = tmp_path / "step2.parquet"
        convert_vortex_to_parquet(vortex_path, parquet_path)

        final = pq.read_table(str(parquet_path))
        assert final.column("id").to_pylist() == original.column("id").to_pylist()
        assert final.column("name").to_pylist() == original.column("name").to_pylist()


class TestConvertTableToVortex:
    """Test convert_table_to_vortex with Iceberg tables."""

    def test_convert_table(self, test_catalog, tmp_path):
        """Test exporting an Iceberg table to Vortex."""
        from lakehouse.catalog import insert_rows

        insert_rows(test_catalog, "expenses", [
            {"id": 9000, "category": "test", "amount": 10.0, "currency": "USD"},
            {"id": 9001, "category": "test", "amount": 20.0, "currency": "EUR"},
        ])

        result = convert_table_to_vortex(test_catalog, "expenses", tmp_path / "export")
        assert result["rows"] == 2
        assert result["size_bytes"] > 0

        # Verify the exported file
        table = read_vortex(result["output"])
        assert table.num_rows == 2
        ids = table.column("id").to_pylist()
        assert 9000 in ids
        assert 9001 in ids

    def test_convert_table_compact(self, test_catalog, tmp_path):
        """Test compact export."""
        from lakehouse.catalog import insert_rows

        insert_rows(test_catalog, "expenses", [
            {"id": 9010, "category": "test", "amount": 10.0, "currency": "USD"},
        ])

        result = convert_table_to_vortex(test_catalog, "expenses", tmp_path / "export", compact=True)
        assert result["rows"] == 1

    def test_convert_nonexistent_table(self, test_catalog, tmp_path):
        """Test converting non-existent table raises."""
        with pytest.raises(ValueError, match="not found"):
            convert_table_to_vortex(test_catalog, "nonexistent", tmp_path)
