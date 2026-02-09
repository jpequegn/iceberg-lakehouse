"""Tests for file import operations."""

import json
import pytest

from lakehouse.catalog import import_file


class TestImportCSV:
    """Test importing CSV files."""

    def test_import_csv_new_table(self, test_catalog, query_engine, tmp_path):
        """Import CSV into a new table."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name,score\n1,Alice,95.5\n2,Bob,87.3\n3,Charlie,92.1\n")

        result = import_file(test_catalog, csv_file, "students")
        assert result["rows_imported"] == 3
        assert result["format"] == "csv"
        assert result["table"] == "default.students"

        # Verify data
        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 3
        assert list(df["name"]) == ["Alice", "Bob", "Charlie"]

    def test_import_csv_append(self, test_catalog, query_engine, tmp_path):
        """Import CSV appending to an existing table."""
        # First import
        csv1 = tmp_path / "batch1.csv"
        csv1.write_text("id,name,score\n1,Alice,95.5\n2,Bob,87.3\n")
        import_file(test_catalog, csv1, "students")

        # Second import (append)
        csv2 = tmp_path / "batch2.csv"
        csv2.write_text("id,name,score\n3,Charlie,92.1\n4,Diana,78.9\n")
        result = import_file(test_catalog, csv2, "students", if_exists="append")
        assert result["rows_imported"] == 2

        # Verify all rows present
        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 4

    def test_import_csv_replace(self, test_catalog, query_engine, tmp_path):
        """Import CSV replacing existing table data."""
        # First import
        csv1 = tmp_path / "original.csv"
        csv1.write_text("id,name,score\n1,Alice,95.5\n2,Bob,87.3\n")
        import_file(test_catalog, csv1, "students")

        # Replace
        csv2 = tmp_path / "replacement.csv"
        csv2.write_text("id,name,score\n10,Eve,99.0\n")
        result = import_file(test_catalog, csv2, "students", if_exists="replace")
        assert result["rows_imported"] == 1

        # Verify only new data
        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Eve"

    def test_import_csv_fail_if_exists(self, test_catalog, tmp_path):
        """Import CSV fails if table exists and if_exists='fail'."""
        csv = tmp_path / "data.csv"
        csv.write_text("id,name\n1,Alice\n")
        import_file(test_catalog, csv, "students")

        csv2 = tmp_path / "data2.csv"
        csv2.write_text("id,name\n2,Bob\n")
        with pytest.raises(ValueError, match="already exists"):
            import_file(test_catalog, csv2, "students", if_exists="fail")

    def test_import_csv_custom_delimiter(self, test_catalog, query_engine, tmp_path):
        """Import CSV with custom delimiter."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id;name;score\n1;Alice;95.5\n2;Bob;87.3\n")

        result = import_file(test_catalog, csv_file, "students", delimiter=";")
        assert result["rows_imported"] == 2

        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 2
        assert list(df["name"]) == ["Alice", "Bob"]

    def test_import_tsv_auto_detect(self, test_catalog, query_engine, tmp_path):
        """Import TSV file with auto-detected delimiter."""
        tsv_file = tmp_path / "data.tsv"
        tsv_file.write_text("id\tname\tscore\n1\tAlice\t95.5\n2\tBob\t87.3\n")

        result = import_file(test_catalog, tsv_file, "students")
        assert result["rows_imported"] == 2
        assert result["format"] == "csv"

    def test_import_csv_no_header(self, test_catalog, query_engine, tmp_path):
        """Import CSV without headers."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("1,Alice,95.5\n2,Bob,87.3\n")

        result = import_file(test_catalog, csv_file, "students", has_header=False)
        assert result["rows_imported"] == 2

    def test_import_csv_append_to_existing_iceberg_table(self, test_catalog, query_engine, tmp_path):
        """Import CSV appending to a pre-existing Iceberg table (expenses)."""
        csv_file = tmp_path / "new_expenses.csv"
        csv_file.write_text(
            "id,date,category,description,amount,currency\n"
            "100,2025-06-01,test,Test expense,42.50,USD\n"
        )

        result = import_file(test_catalog, csv_file, "expenses", if_exists="append")
        assert result["rows_imported"] == 1

        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM expenses WHERE id = 100")
        assert len(df) == 1
        assert df.iloc[0]["category"] == "test"

    def test_import_csv_schema_mismatch_extra_columns(self, test_catalog, tmp_path):
        """Import CSV with extra columns not in table fails on append."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")
        import_file(test_catalog, csv_file, "students")

        csv2 = tmp_path / "extra.csv"
        csv2.write_text("id,name,extra_col\n2,Bob,foo\n")
        with pytest.raises(ValueError, match="columns not in table"):
            import_file(test_catalog, csv2, "students", if_exists="append")

    def test_import_csv_append_with_missing_columns(self, test_catalog, query_engine, tmp_path):
        """Import CSV with fewer columns fills missing with nulls on append."""
        csv1 = tmp_path / "full.csv"
        csv1.write_text("id,name,score\n1,Alice,95.5\n")
        import_file(test_catalog, csv1, "students")

        csv2 = tmp_path / "partial.csv"
        csv2.write_text("id,name\n2,Bob\n")
        result = import_file(test_catalog, csv2, "students", if_exists="append")
        assert result["rows_imported"] == 1

        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 2
        # Bob's score should be null
        assert df.iloc[1]["score"] is None or str(df.iloc[1]["score"]) == "nan"


class TestImportJSON:
    """Test importing JSON files."""

    def test_import_json_array(self, test_catalog, query_engine, tmp_path):
        """Import JSON array format."""
        json_file = tmp_path / "data.json"
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.3},
        ]
        json_file.write_text(json.dumps(data))

        result = import_file(test_catalog, json_file, "students")
        assert result["rows_imported"] == 2
        assert result["format"] == "json"

        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 2
        assert list(df["name"]) == ["Alice", "Bob"]

    def test_import_ndjson(self, test_catalog, query_engine, tmp_path):
        """Import newline-delimited JSON."""
        ndjson_file = tmp_path / "data.ndjson"
        lines = [
            json.dumps({"id": 1, "name": "Alice", "score": 95.5}),
            json.dumps({"id": 2, "name": "Bob", "score": 87.3}),
            json.dumps({"id": 3, "name": "Charlie", "score": 92.1}),
        ]
        ndjson_file.write_text("\n".join(lines) + "\n")

        result = import_file(test_catalog, ndjson_file, "students")
        assert result["rows_imported"] == 3
        assert result["format"] == "ndjson"

        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 3

    def test_import_jsonl(self, test_catalog, query_engine, tmp_path):
        """Import .jsonl file (auto-detected as ndjson)."""
        jsonl_file = tmp_path / "data.jsonl"
        lines = [
            json.dumps({"id": 1, "name": "Alice"}),
            json.dumps({"id": 2, "name": "Bob"}),
        ]
        jsonl_file.write_text("\n".join(lines) + "\n")

        result = import_file(test_catalog, jsonl_file, "students")
        assert result["rows_imported"] == 2
        assert result["format"] == "ndjson"

    def test_import_json_append(self, test_catalog, query_engine, tmp_path):
        """Import JSON appending to an existing table."""
        json1 = tmp_path / "batch1.json"
        json1.write_text(json.dumps([{"id": 1, "name": "Alice"}]))
        import_file(test_catalog, json1, "students")

        json2 = tmp_path / "batch2.json"
        json2.write_text(json.dumps([{"id": 2, "name": "Bob"}]))
        import_file(test_catalog, json2, "students", if_exists="append")

        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 2

    def test_import_json_replace(self, test_catalog, query_engine, tmp_path):
        """Import JSON replacing existing table data."""
        json1 = tmp_path / "original.json"
        json1.write_text(json.dumps([{"id": 1, "name": "Alice"}]))
        import_file(test_catalog, json1, "students")

        json2 = tmp_path / "replacement.json"
        json2.write_text(json.dumps([{"id": 10, "name": "Zara"}]))
        import_file(test_catalog, json2, "students", if_exists="replace")

        query_engine.refresh()
        df = query_engine.execute("SELECT * FROM students ORDER BY id")
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Zara"


class TestImportErrors:
    """Test import error handling."""

    def test_import_missing_file(self, test_catalog):
        """Import non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="File not found"):
            import_file(test_catalog, "/nonexistent/data.csv", "test_table")

    def test_import_unsupported_extension(self, test_catalog, tmp_path):
        """Import file with unsupported extension raises ValueError."""
        parquet_file = tmp_path / "data.parquet"
        parquet_file.write_text("not real parquet")

        with pytest.raises(ValueError, match="Cannot auto-detect format"):
            import_file(test_catalog, parquet_file, "test_table")

    def test_import_unsupported_format(self, test_catalog, tmp_path):
        """Import with explicit unsupported format raises ValueError."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        with pytest.raises(ValueError, match="Unsupported format"):
            import_file(test_catalog, csv_file, "test_table", file_format="xml")

    def test_import_format_override(self, test_catalog, query_engine, tmp_path):
        """Format override works regardless of extension."""
        # JSON file with .txt extension
        txt_file = tmp_path / "data.txt"
        txt_file.write_text(json.dumps([{"id": 1, "name": "Alice"}]))

        result = import_file(test_catalog, txt_file, "students", file_format="json")
        assert result["rows_imported"] == 1
        assert result["format"] == "json"

    def test_import_with_namespace(self, test_catalog, query_engine, tmp_path):
        """Import with explicit namespace in table name."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        result = import_file(test_catalog, csv_file, "default.students")
        assert result["table"] == "default.students"
        assert result["rows_imported"] == 1
