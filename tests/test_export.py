"""Tests for table export operations."""

import json
import pytest
import pyarrow.parquet as pq

from lakehouse.catalog import export_table, insert_rows


class TestExportCSV:
    """Test exporting to CSV."""

    def test_export_csv(self, test_catalog, tmp_path):
        """Export table to CSV."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "date": "2025-01-01", "category": "food", "amount": 25.0, "currency": "USD"},
            {"id": 2, "date": "2025-01-02", "category": "transport", "amount": 15.0, "currency": "USD"},
        ])

        output = tmp_path / "expenses.csv"
        result = export_table(test_catalog, "expenses", output, file_format="csv")

        assert result["rows_exported"] == 2
        assert result["format"] == "csv"
        assert output.exists()

        content = output.read_text()
        assert "food" in content
        assert "transport" in content

    def test_export_csv_auto_detect(self, test_catalog, tmp_path):
        """Export auto-detects CSV from extension."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        output = tmp_path / "data.csv"
        result = export_table(test_catalog, "expenses", output)
        assert result["format"] == "csv"
        assert output.exists()

    def test_export_csv_default_path(self, test_catalog, tmp_path, monkeypatch):
        """Export uses default path when none specified."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        monkeypatch.chdir(tmp_path)
        result = export_table(test_catalog, "expenses", file_format="csv")
        assert result["format"] == "csv"
        assert result["output"] == "expenses.csv"


class TestExportJSON:
    """Test exporting to JSON."""

    def test_export_json(self, test_catalog, tmp_path):
        """Export table to JSON array format."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0, "currency": "USD"},
            {"id": 2, "category": "transport", "amount": 15.0, "currency": "EUR"},
        ])

        output = tmp_path / "data.json"
        result = export_table(test_catalog, "expenses", output, file_format="json")

        assert result["rows_exported"] == 2
        assert result["format"] == "json"

        data = json.loads(output.read_text())
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["category"] == "food"

    def test_export_ndjson(self, test_catalog, tmp_path):
        """Export table to NDJSON format."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
            {"id": 2, "category": "transport", "amount": 15.0},
        ])

        output = tmp_path / "data.ndjson"
        result = export_table(test_catalog, "expenses", output, file_format="ndjson")

        assert result["rows_exported"] == 2
        assert result["format"] == "ndjson"

        lines = [l for l in output.read_text().strip().split("\n") if l]
        assert len(lines) == 2
        assert json.loads(lines[0])["category"] == "food"
        assert json.loads(lines[1])["category"] == "transport"

    def test_export_json_auto_detect(self, test_catalog, tmp_path):
        """Export auto-detects JSON from .json extension."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        output = tmp_path / "data.json"
        result = export_table(test_catalog, "expenses", output)
        assert result["format"] == "json"

    def test_export_ndjson_auto_detect(self, test_catalog, tmp_path):
        """Export auto-detects NDJSON from .ndjson extension."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        output = tmp_path / "data.ndjson"
        result = export_table(test_catalog, "expenses", output)
        assert result["format"] == "ndjson"

    def test_export_jsonl_auto_detect(self, test_catalog, tmp_path):
        """Export auto-detects NDJSON from .jsonl extension."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        output = tmp_path / "data.jsonl"
        result = export_table(test_catalog, "expenses", output)
        assert result["format"] == "ndjson"


class TestExportParquet:
    """Test exporting to Parquet."""

    def test_export_parquet(self, test_catalog, tmp_path):
        """Export table to Parquet."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0, "currency": "USD"},
            {"id": 2, "category": "transport", "amount": 15.0, "currency": "EUR"},
        ])

        output = tmp_path / "expenses.parquet"
        result = export_table(test_catalog, "expenses", output, file_format="parquet")

        assert result["rows_exported"] == 2
        assert result["format"] == "parquet"

        # Verify readable by PyArrow
        table = pq.read_table(str(output))
        assert table.num_rows == 2

    def test_export_parquet_auto_detect(self, test_catalog, tmp_path):
        """Export auto-detects Parquet from extension."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        output = tmp_path / "data.parquet"
        result = export_table(test_catalog, "expenses", output)
        assert result["format"] == "parquet"


class TestExportFiltering:
    """Test export with WHERE, columns, and limit."""

    def test_export_with_where(self, test_catalog, tmp_path):
        """Export with WHERE filter."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
            {"id": 2, "category": "transport", "amount": 150.0},
            {"id": 3, "category": "food", "amount": 200.0},
        ])

        output = tmp_path / "big.csv"
        result = export_table(test_catalog, "expenses", output, file_format="csv", where="amount > 100")

        assert result["rows_exported"] == 2
        content = output.read_text()
        # amount=25 should be excluded, amounts 150 and 200 should be present
        lines = content.strip().split("\n")
        # Header + 2 data rows
        assert len(lines) == 3
        assert "transport" in content
        assert "food" in content

    def test_export_with_columns(self, test_catalog, tmp_path):
        """Export with column selection."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0, "currency": "USD"},
        ])

        output = tmp_path / "subset.json"
        result = export_table(
            test_catalog, "expenses", output,
            file_format="json", columns=["id", "category"],
        )

        data = json.loads(output.read_text())
        assert set(data[0].keys()) == {"id", "category"}

    def test_export_with_limit(self, test_catalog, tmp_path):
        """Export with row limit."""
        insert_rows(test_catalog, "expenses", [
            {"id": i, "category": "test", "amount": float(i)} for i in range(1, 11)
        ])

        output = tmp_path / "limited.json"
        result = export_table(test_catalog, "expenses", output, file_format="json", limit=3)

        assert result["rows_exported"] == 3
        data = json.loads(output.read_text())
        assert len(data) == 3

    def test_export_with_where_and_columns_and_limit(self, test_catalog, tmp_path):
        """Export combining WHERE, columns, and limit."""
        insert_rows(test_catalog, "expenses", [
            {"id": i, "category": "food" if i % 2 == 0 else "transport", "amount": float(i * 10)}
            for i in range(1, 11)
        ])

        output = tmp_path / "combo.json"
        result = export_table(
            test_catalog, "expenses", output,
            file_format="json",
            where="category = 'food'",
            columns=["id", "amount"],
            limit=2,
        )

        assert result["rows_exported"] == 2
        data = json.loads(output.read_text())
        assert len(data) == 2
        assert set(data[0].keys()) == {"id", "amount"}

    def test_export_invalid_column(self, test_catalog, tmp_path):
        """Export with invalid column name raises error."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        with pytest.raises(ValueError, match="Columns not found"):
            export_table(
                test_catalog, "expenses", tmp_path / "out.csv",
                file_format="csv", columns=["nonexistent"],
            )


class TestExportErrors:
    """Test export error handling."""

    def test_export_nonexistent_table(self, test_catalog, tmp_path):
        """Export nonexistent table raises error."""
        with pytest.raises(ValueError, match="not found"):
            export_table(test_catalog, "nonexistent", tmp_path / "out.csv")

    def test_export_unsupported_format(self, test_catalog, tmp_path):
        """Export with unsupported format raises error."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        with pytest.raises(ValueError, match="Unsupported format"):
            export_table(
                test_catalog, "expenses", tmp_path / "out.xml",
                file_format="xml",
            )

    def test_export_empty_table(self, test_catalog, tmp_path):
        """Export empty table produces valid empty output."""
        output = tmp_path / "empty.csv"
        result = export_table(test_catalog, "expenses", output, file_format="csv")

        assert result["rows_exported"] == 0
        assert output.exists()

    def test_export_with_namespace(self, test_catalog, tmp_path):
        """Export with explicit namespace in table name."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        output = tmp_path / "out.csv"
        result = export_table(test_catalog, "default.expenses", output, file_format="csv")
        assert result["table"] == "default.expenses"
        assert result["rows_exported"] == 1

    def test_export_creates_parent_dirs(self, test_catalog, tmp_path):
        """Export creates parent directories if needed."""
        insert_rows(test_catalog, "expenses", [
            {"id": 1, "category": "food", "amount": 25.0},
        ])

        output = tmp_path / "nested" / "dir" / "out.csv"
        result = export_table(test_catalog, "expenses", output, file_format="csv")
        assert output.exists()
        assert result["rows_exported"] == 1
