"""Tests for data masking policies."""

import json
import pytest

from lakehouse.masking import (
    add_masking_policy,
    list_masking_policies,
    remove_masking_policy,
    query_with_masking,
    preview_masking,
)
from lakehouse.catalog import create_table, insert_rows
from lakehouse.query import QueryEngine


@pytest.fixture
def mask_path(tmp_path):
    """Return a temporary masking store path."""
    return tmp_path / "masking.json"


@pytest.fixture
def mask_table(test_catalog):
    """Create a table with sample data for masking tests."""
    create_table(test_catalog, "users", columns={"id": "long", "name": "string", "email": "string"})
    insert_rows(test_catalog, "default.users", [
        {"id": 1, "name": "Alice Smith", "email": "alice@example.com"},
        {"id": 2, "name": "Bob Jones", "email": "bob@example.com"},
        {"id": 3, "name": "Carol White", "email": "carol@example.com"},
    ])
    return test_catalog


# --- add_masking_policy ---


class TestAddMaskingPolicy:
    def test_add_hash(self, mask_path):
        """Add a hash masking policy."""
        result = add_masking_policy("users", "email", "hash", store_path=mask_path)
        assert result["strategy"] == "hash"
        assert result["table"] == "default.users"

    def test_add_redact(self, mask_path):
        """Add a redact masking policy."""
        result = add_masking_policy("users", "name", "redact", options={"replacement": "[REDACTED]"}, store_path=mask_path)
        assert result["strategy"] == "redact"
        assert result["options"]["replacement"] == "[REDACTED]"

    def test_add_truncate(self, mask_path):
        """Add a truncate masking policy."""
        result = add_masking_policy("users", "ssn", "truncate", options={"length": 3}, store_path=mask_path)
        assert result["strategy"] == "truncate"
        assert result["options"]["length"] == 3

    def test_add_nullify(self, mask_path):
        """Add a nullify masking policy."""
        result = add_masking_policy("users", "secret", "nullify", store_path=mask_path)
        assert result["strategy"] == "nullify"

    def test_add_expression(self, mask_path):
        """Add an expression masking policy."""
        result = add_masking_policy(
            "users", "phone", "expression",
            options={"sql": "'***-' || RIGHT(col, 4)"},
            store_path=mask_path,
        )
        assert result["strategy"] == "expression"

    def test_invalid_strategy_raises(self, mask_path):
        """Invalid strategy raises ValueError."""
        with pytest.raises(ValueError, match="Invalid strategy"):
            add_masking_policy("users", "col", "encrypt", store_path=mask_path)

    def test_expression_without_sql_raises(self, mask_path):
        """Expression without sql option raises ValueError."""
        with pytest.raises(ValueError, match="sql"):
            add_masking_policy("users", "col", "expression", store_path=mask_path)

    def test_duplicate_raises(self, mask_path):
        """Adding duplicate policy raises ValueError."""
        add_masking_policy("users", "email", "hash", store_path=mask_path)
        with pytest.raises(ValueError, match="already exists"):
            add_masking_policy("users", "email", "redact", store_path=mask_path)


# --- list_masking_policies ---


class TestListMaskingPolicies:
    def test_list_all(self, mask_path):
        """List all policies."""
        add_masking_policy("t1", "a", "hash", store_path=mask_path)
        add_masking_policy("t2", "b", "redact", store_path=mask_path)
        policies = list_masking_policies(store_path=mask_path)
        assert len(policies) == 2

    def test_list_by_table(self, mask_path):
        """List policies for a specific table."""
        add_masking_policy("t1", "a", "hash", store_path=mask_path)
        add_masking_policy("t2", "b", "redact", store_path=mask_path)
        policies = list_masking_policies(table_name="t1", store_path=mask_path)
        assert len(policies) == 1
        assert policies[0]["table"] == "default.t1"

    def test_list_empty(self, mask_path):
        """Empty store returns empty list."""
        assert list_masking_policies(store_path=mask_path) == []


# --- remove_masking_policy ---


class TestRemoveMaskingPolicy:
    def test_remove_existing(self, mask_path):
        """Remove an existing policy."""
        add_masking_policy("users", "email", "hash", store_path=mask_path)
        result = remove_masking_policy("users", "email", store_path=mask_path)
        assert "removed" in result["message"].lower()
        assert list_masking_policies(store_path=mask_path) == []

    def test_remove_nonexistent(self, mask_path):
        """Removing nonexistent policy is a no-op."""
        result = remove_masking_policy("users", "nope", store_path=mask_path)
        assert "no masking policy" in result["message"].lower()


# --- query_with_masking ---


class TestQueryWithMasking:
    def test_hash_masking(self, mask_table, mask_path):
        """Hash masking produces hashed values."""
        add_masking_policy("users", "email", "hash", store_path=mask_path)
        engine = QueryEngine(catalog=mask_table)
        df = query_with_masking(engine, "SELECT * FROM users", store_path=mask_path)
        # Hashed emails should not contain @
        for email in df["email"]:
            assert "@" not in str(email)

    def test_redact_masking(self, mask_table, mask_path):
        """Redact masking replaces values."""
        add_masking_policy("users", "name", "redact", options={"replacement": "[REDACTED]"}, store_path=mask_path)
        engine = QueryEngine(catalog=mask_table)
        df = query_with_masking(engine, "SELECT * FROM users", store_path=mask_path)
        for name in df["name"]:
            assert name == "[REDACTED]"

    def test_truncate_masking(self, mask_table, mask_path):
        """Truncate masking keeps first N chars."""
        add_masking_policy("users", "name", "truncate", options={"length": 3}, store_path=mask_path)
        engine = QueryEngine(catalog=mask_table)
        df = query_with_masking(engine, "SELECT * FROM users", store_path=mask_path)
        for name in df["name"]:
            assert name.endswith("***")
            assert len(name) == 6  # 3 chars + "***"

    def test_unmasked_columns_unchanged(self, mask_table, mask_path):
        """Columns without policies pass through unchanged."""
        add_masking_policy("users", "email", "hash", store_path=mask_path)
        engine = QueryEngine(catalog=mask_table)
        df = query_with_masking(engine, "SELECT * FROM users", store_path=mask_path)
        # id and name should be unchanged
        assert list(df["id"]) == [1, 2, 3]
        names = list(df["name"])
        assert "Alice Smith" in names


# --- preview_masking ---


class TestPreviewMasking:
    def test_preview(self, mask_table, mask_path):
        """Preview shows original and masked data."""
        add_masking_policy("users", "email", "hash", store_path=mask_path)
        result = preview_masking(mask_table, "users", max_rows=3, store_path=mask_path)
        assert len(result["original"]) == 3
        assert len(result["masked"]) == 3
        assert result["policies_applied"] == 1
        # Original should have real emails
        assert "@" in result["original"][0]["email"]
        # Masked should have hashed emails
        assert "@" not in str(result["masked"][0]["email"])

    def test_nonexistent_table_raises(self, test_catalog, mask_path):
        """Nonexistent table raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            preview_masking(test_catalog, "no_such", store_path=mask_path)


# --- Storage format ---


class TestStorageFormat:
    def test_json_structure(self, mask_path):
        """Store is valid JSON with expected structure."""
        add_masking_policy("users", "email", "hash", store_path=mask_path)
        add_masking_policy("users", "name", "redact", options={"replacement": "***"}, store_path=mask_path)

        data = json.loads(mask_path.read_text())
        assert "default.users" in data
        assert "email" in data["default.users"]
        assert "name" in data["default.users"]
        assert data["default.users"]["email"]["strategy"] == "hash"
        assert data["default.users"]["name"]["strategy"] == "redact"
        assert "created_at" in data["default.users"]["email"]
