"""Tests for data catalog enrichment (descriptions, glossary, classifications)."""

import json
import pytest

from lakehouse.catalog_metadata import (
    set_column_description,
    get_column_descriptions,
    classify_column,
    get_classifications,
    add_glossary_term,
    search_glossary,
    list_glossary,
    remove_glossary_term,
    get_enriched_schema,
)
from lakehouse.catalog import create_table, insert_rows


@pytest.fixture
def meta_path(tmp_path):
    """Return a temporary catalog metadata store path."""
    return tmp_path / "catalog_metadata.json"


@pytest.fixture
def enriched_table(test_catalog, meta_path):
    """Create a table with descriptions and classifications."""
    create_table(test_catalog, "users", columns={"id": "long", "name": "string", "email": "string"})
    insert_rows(test_catalog, "default.users", [{"id": 1, "name": "Alice", "email": "alice@test.com"}])
    set_column_description("users", "id", "Unique user identifier", store_path=meta_path)
    set_column_description("users", "email", "User email address", store_path=meta_path)
    classify_column("users", "email", "pii", store_path=meta_path)
    classify_column("users", "name", "pii", store_path=meta_path)
    return test_catalog


# --- Column Descriptions ---


class TestColumnDescriptions:
    def test_set_and_get(self, meta_path):
        """Set and retrieve a column description."""
        set_column_description("t", "col1", "A description", store_path=meta_path)
        result = get_column_descriptions("t", store_path=meta_path)
        assert result["descriptions"]["col1"] == "A description"

    def test_multiple_columns(self, meta_path):
        """Set descriptions for multiple columns."""
        set_column_description("t", "a", "Column A", store_path=meta_path)
        set_column_description("t", "b", "Column B", store_path=meta_path)
        result = get_column_descriptions("t", store_path=meta_path)
        assert len(result["descriptions"]) == 2

    def test_overwrite(self, meta_path):
        """Overwriting a description replaces the old one."""
        set_column_description("t", "col", "Old", store_path=meta_path)
        set_column_description("t", "col", "New", store_path=meta_path)
        result = get_column_descriptions("t", store_path=meta_path)
        assert result["descriptions"]["col"] == "New"

    def test_empty_table(self, meta_path):
        """Getting descriptions for a table with none returns empty."""
        result = get_column_descriptions("no_such", store_path=meta_path)
        assert result["descriptions"] == {}

    def test_message(self, meta_path):
        """Result includes a message."""
        result = set_column_description("t", "col", "desc", store_path=meta_path)
        assert "description set" in result["message"].lower()


# --- Classifications ---


class TestClassifications:
    def test_classify_valid(self, meta_path):
        """Classify a column with a valid classification."""
        result = classify_column("t", "email", "pii", store_path=meta_path)
        assert result["classification"] == "pii"

    def test_invalid_classification_raises(self, meta_path):
        """Invalid classification raises ValueError."""
        with pytest.raises(ValueError, match="Invalid classification"):
            classify_column("t", "col", "secret", store_path=meta_path)

    def test_get_by_table(self, meta_path):
        """Get classifications filtered by table."""
        classify_column("t1", "a", "pii", store_path=meta_path)
        classify_column("t2", "b", "financial", store_path=meta_path)
        results = get_classifications(table_name="t1", store_path=meta_path)
        assert len(results) == 1
        assert results[0]["table"] == "default.t1"

    def test_get_by_type(self, meta_path):
        """Get classifications filtered by type."""
        classify_column("t", "a", "pii", store_path=meta_path)
        classify_column("t", "b", "financial", store_path=meta_path)
        classify_column("t", "c", "pii", store_path=meta_path)
        results = get_classifications(classification="pii", store_path=meta_path)
        assert len(results) == 2


# --- Glossary ---


class TestGlossary:
    def test_add_and_search(self, meta_path):
        """Add a term and find it by search."""
        add_glossary_term("MRR", "Monthly Recurring Revenue", store_path=meta_path)
        results = search_glossary("MRR", store_path=meta_path)
        assert len(results) == 1
        assert results[0]["term"] == "MRR"

    def test_search_by_definition(self, meta_path):
        """Search matches on definition text."""
        add_glossary_term("MRR", "Monthly Recurring Revenue", store_path=meta_path)
        results = search_glossary("revenue", store_path=meta_path)
        assert len(results) == 1

    def test_search_by_alias(self, meta_path):
        """Search matches on aliases."""
        add_glossary_term("MRR", "Monthly Recurring Revenue", aliases=["monthly revenue"], store_path=meta_path)
        results = search_glossary("monthly revenue", store_path=meta_path)
        assert len(results) == 1

    def test_list_glossary(self, meta_path):
        """List all glossary terms."""
        add_glossary_term("A", "Term A", store_path=meta_path)
        add_glossary_term("B", "Term B", store_path=meta_path)
        terms = list_glossary(store_path=meta_path)
        assert len(terms) == 2

    def test_remove_glossary(self, meta_path):
        """Remove a glossary term."""
        add_glossary_term("MRR", "Monthly Recurring Revenue", store_path=meta_path)
        remove_glossary_term("MRR", store_path=meta_path)
        assert list_glossary(store_path=meta_path) == []

    def test_remove_nonexistent(self, meta_path):
        """Removing a nonexistent term is a no-op."""
        result = remove_glossary_term("nope", store_path=meta_path)
        assert "not found" in result["message"].lower()


# --- Enriched Schema ---


class TestEnrichedSchema:
    def test_includes_descriptions(self, enriched_table, meta_path):
        """Enriched schema includes column descriptions."""
        result = get_enriched_schema(enriched_table, "users", store_path=meta_path)
        email_field = next(f for f in result["fields"] if f["name"] == "email")
        assert email_field["description"] == "User email address"

    def test_includes_classifications(self, enriched_table, meta_path):
        """Enriched schema includes classifications."""
        result = get_enriched_schema(enriched_table, "users", store_path=meta_path)
        email_field = next(f for f in result["fields"] if f["name"] == "email")
        assert email_field["classification"] == "pii"

    def test_includes_glossary_matches(self, enriched_table, meta_path):
        """Enriched schema includes glossary term matches."""
        add_glossary_term("email", "Electronic mail address", store_path=meta_path)
        result = get_enriched_schema(enriched_table, "users", store_path=meta_path)
        email_field = next(f for f in result["fields"] if f["name"] == "email")
        assert "email" in email_field["glossary_matches"]

    def test_counts(self, enriched_table, meta_path):
        """Enriched schema includes field counts."""
        result = get_enriched_schema(enriched_table, "users", store_path=meta_path)
        assert result["total_fields"] == 3
        assert result["described_fields"] == 2
        assert result["classified_fields"] == 2

    def test_nonexistent_table_raises(self, test_catalog, meta_path):
        """Nonexistent table raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            get_enriched_schema(test_catalog, "no_such", store_path=meta_path)


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, meta_path):
        """Store is valid JSON with expected structure."""
        set_column_description("t", "col", "desc", store_path=meta_path)
        classify_column("t", "col", "pii", store_path=meta_path)
        add_glossary_term("Term", "Definition", store_path=meta_path)

        data = json.loads(meta_path.read_text())
        assert "column_descriptions" in data
        assert "classifications" in data
        assert "glossary" in data
        assert "default.t" in data["column_descriptions"]
        assert "default.t" in data["classifications"]
        assert "Term" in data["glossary"]
