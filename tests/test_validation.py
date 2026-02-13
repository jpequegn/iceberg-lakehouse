"""Tests for data validation rules."""

import json
import pytest
from pathlib import Path

from lakehouse.validation import (
    add_validation_rule,
    list_validation_rules,
    remove_validation_rule,
    validate_rows,
    ValidationError,
    _load_rules,
)
from lakehouse.catalog import (
    create_table,
    insert_rows,
    update_rows,
    upsert_rows,
)


@pytest.fixture
def rules_path(tmp_path):
    """Return a temporary validation rules path."""
    return tmp_path / "validation.json"


@pytest.fixture
def validated_table(test_catalog, rules_path):
    """Create a table and return (catalog, table_name, rules_path)."""
    create_table(test_catalog, "validated", {"id": "long", "name": "string", "amount": "double"})
    return test_catalog, "validated", rules_path


# --- Rule CRUD ---

class TestAddRule:
    """Test adding validation rules."""

    def test_add_not_null(self, rules_path):
        result = add_validation_rule("expenses", {"type": "not_null", "column": "id"}, store_path=rules_path)
        assert result["type"] == "not_null"
        assert result["column"] == "id"
        assert "id" in result
        assert "Added" in result["message"]

    def test_add_unique(self, rules_path):
        result = add_validation_rule("expenses", {"type": "unique", "columns": ["id"]}, store_path=rules_path)
        assert result["type"] == "unique"
        assert result["columns"] == ["id"]

    def test_add_range(self, rules_path):
        result = add_validation_rule("expenses", {"type": "range", "column": "amount", "min": 0, "max": 10000}, store_path=rules_path)
        assert result["type"] == "range"
        assert result["min"] == 0
        assert result["max"] == 10000

    def test_add_regex(self, rules_path):
        result = add_validation_rule("expenses", {"type": "regex", "column": "email", "pattern": "^[^@]+@[^@]+$"}, store_path=rules_path)
        assert result["type"] == "regex"

    def test_add_expression(self, rules_path):
        result = add_validation_rule("expenses", {"type": "expression", "sql": "amount > 0"}, store_path=rules_path)
        assert result["type"] == "expression"

    def test_add_persists(self, rules_path):
        add_validation_rule("expenses", {"type": "not_null", "column": "id"}, store_path=rules_path)
        data = json.loads(rules_path.read_text())
        assert "expenses" in data
        assert len(data["expenses"]) == 1

    def test_add_multiple(self, rules_path):
        add_validation_rule("expenses", {"type": "not_null", "column": "id"}, store_path=rules_path)
        add_validation_rule("expenses", {"type": "range", "column": "amount", "min": 0}, store_path=rules_path)
        rules = list_validation_rules("expenses", store_path=rules_path)
        assert len(rules) == 2

    def test_add_invalid_type(self, rules_path):
        with pytest.raises(ValueError, match="Invalid rule type"):
            add_validation_rule("expenses", {"type": "invalid"}, store_path=rules_path)

    def test_add_not_null_missing_column(self, rules_path):
        with pytest.raises(ValueError, match="requires 'column'"):
            add_validation_rule("expenses", {"type": "not_null"}, store_path=rules_path)

    def test_add_range_missing_bounds(self, rules_path):
        with pytest.raises(ValueError, match="requires at least"):
            add_validation_rule("expenses", {"type": "range", "column": "amount"}, store_path=rules_path)

    def test_add_regex_invalid_pattern(self, rules_path):
        with pytest.raises(ValueError, match="Invalid regex"):
            add_validation_rule("expenses", {"type": "regex", "column": "name", "pattern": "[invalid"}, store_path=rules_path)


class TestListRules:
    """Test listing rules."""

    def test_list_empty(self, rules_path):
        rules = list_validation_rules("expenses", store_path=rules_path)
        assert rules == []

    def test_list_returns_all(self, rules_path):
        add_validation_rule("expenses", {"type": "not_null", "column": "id"}, store_path=rules_path)
        add_validation_rule("expenses", {"type": "not_null", "column": "name"}, store_path=rules_path)
        rules = list_validation_rules("expenses", store_path=rules_path)
        assert len(rules) == 2

    def test_list_different_tables(self, rules_path):
        add_validation_rule("expenses", {"type": "not_null", "column": "id"}, store_path=rules_path)
        add_validation_rule("health", {"type": "not_null", "column": "metric"}, store_path=rules_path)
        assert len(list_validation_rules("expenses", store_path=rules_path)) == 1
        assert len(list_validation_rules("health", store_path=rules_path)) == 1


class TestRemoveRule:
    """Test removing rules."""

    def test_remove_existing(self, rules_path):
        result = add_validation_rule("expenses", {"type": "not_null", "column": "id"}, store_path=rules_path)
        rule_id = result["id"]

        removed = remove_validation_rule("expenses", rule_id, store_path=rules_path)
        assert removed["id"] == rule_id
        assert "Removed" in removed["message"]

        assert list_validation_rules("expenses", store_path=rules_path) == []

    def test_remove_nonexistent(self, rules_path):
        with pytest.raises(ValueError, match="not found"):
            remove_validation_rule("expenses", "nonexistent", store_path=rules_path)

    def test_remove_one_keeps_others(self, rules_path):
        r1 = add_validation_rule("expenses", {"type": "not_null", "column": "id"}, store_path=rules_path)
        add_validation_rule("expenses", {"type": "not_null", "column": "name"}, store_path=rules_path)

        remove_validation_rule("expenses", r1["id"], store_path=rules_path)
        rules = list_validation_rules("expenses", store_path=rules_path)
        assert len(rules) == 1
        assert rules[0]["column"] == "name"


# --- Validation logic ---

class TestValidateNotNull:
    """Test not_null validation."""

    def test_passes(self):
        rules = [{"id": "r1", "type": "not_null", "column": "id"}]
        result = validate_rows([{"id": 1}, {"id": 2}], rules)
        assert result["valid"]

    def test_fails(self):
        rules = [{"id": "r1", "type": "not_null", "column": "id"}]
        result = validate_rows([{"id": 1}, {"id": None}], rules)
        assert not result["valid"]
        assert len(result["failures"]) == 1
        assert result["failures"][0]["row_index"] == 1

    def test_missing_key_is_null(self):
        rules = [{"id": "r1", "type": "not_null", "column": "id"}]
        result = validate_rows([{"name": "test"}], rules)
        assert not result["valid"]


class TestValidateRange:
    """Test range validation."""

    def test_in_range(self):
        rules = [{"id": "r1", "type": "range", "column": "amount", "min": 0, "max": 100}]
        result = validate_rows([{"amount": 50}], rules)
        assert result["valid"]

    def test_below_min(self):
        rules = [{"id": "r1", "type": "range", "column": "amount", "min": 0, "max": 100}]
        result = validate_rows([{"amount": -5}], rules)
        assert not result["valid"]

    def test_above_max(self):
        rules = [{"id": "r1", "type": "range", "column": "amount", "min": 0, "max": 100}]
        result = validate_rows([{"amount": 150}], rules)
        assert not result["valid"]

    def test_null_passes(self):
        """Null values pass range checks (not_null should catch those)."""
        rules = [{"id": "r1", "type": "range", "column": "amount", "min": 0}]
        result = validate_rows([{"amount": None}], rules)
        assert result["valid"]

    def test_min_only(self):
        rules = [{"id": "r1", "type": "range", "column": "amount", "min": 0}]
        result = validate_rows([{"amount": 50}], rules)
        assert result["valid"]


class TestValidateRegex:
    """Test regex validation."""

    def test_matches(self):
        rules = [{"id": "r1", "type": "regex", "column": "category", "pattern": "^[a-z_]+$"}]
        result = validate_rows([{"category": "food_and_drink"}], rules)
        assert result["valid"]

    def test_no_match(self):
        rules = [{"id": "r1", "type": "regex", "column": "category", "pattern": "^[a-z_]+$"}]
        result = validate_rows([{"category": "INVALID"}], rules)
        assert not result["valid"]

    def test_null_passes(self):
        rules = [{"id": "r1", "type": "regex", "column": "category", "pattern": "^[a-z]+$"}]
        result = validate_rows([{"category": None}], rules)
        assert result["valid"]


class TestValidateExpression:
    """Test expression validation."""

    def test_passes(self):
        rules = [{"id": "r1", "type": "expression", "sql": "amount > 0"}]
        result = validate_rows([{"amount": 10}, {"amount": 20}], rules)
        assert result["valid"]

    def test_fails(self):
        rules = [{"id": "r1", "type": "expression", "sql": "amount > 0"}]
        result = validate_rows([{"amount": 10}, {"amount": -5}], rules)
        assert not result["valid"]
        assert len(result["failures"]) == 1


class TestValidateUnique:
    """Test unique validation."""

    def test_unique_within_batch(self):
        rules = [{"id": "r1", "type": "unique", "columns": ["id"]}]
        result = validate_rows([{"id": 1}, {"id": 2}], rules)
        assert result["valid"]

    def test_duplicate_within_batch(self):
        rules = [{"id": "r1", "type": "unique", "columns": ["id"]}]
        result = validate_rows([{"id": 1}, {"id": 1}], rules)
        assert not result["valid"]

    def test_duplicate_against_existing(self):
        rules = [{"id": "r1", "type": "unique", "columns": ["id"]}]
        existing = [{"id": 1}, {"id": 2}]
        result = validate_rows([{"id": 1}], rules, existing_data=existing)
        assert not result["valid"]

    def test_unique_against_existing(self):
        rules = [{"id": "r1", "type": "unique", "columns": ["id"]}]
        existing = [{"id": 1}, {"id": 2}]
        result = validate_rows([{"id": 3}], rules, existing_data=existing)
        assert result["valid"]


class TestValidateMultipleRules:
    """Test multiple rules on same data."""

    def test_all_pass(self):
        rules = [
            {"id": "r1", "type": "not_null", "column": "id"},
            {"id": "r2", "type": "range", "column": "amount", "min": 0},
        ]
        result = validate_rows([{"id": 1, "amount": 50}], rules)
        assert result["valid"]

    def test_multiple_failures(self):
        rules = [
            {"id": "r1", "type": "not_null", "column": "id"},
            {"id": "r2", "type": "range", "column": "amount", "min": 0},
        ]
        result = validate_rows([{"id": None, "amount": -5}], rules)
        assert not result["valid"]
        assert len(result["failures"]) == 2


class TestValidateEmpty:
    """Test validation with no rules."""

    def test_no_rules_passes(self):
        result = validate_rows([{"id": 1}], [])
        assert result["valid"]

    def test_empty_rows_passes(self):
        rules = [{"id": "r1", "type": "not_null", "column": "id"}]
        result = validate_rows([], rules)
        assert result["valid"]


# --- Integration with write operations ---

class TestInsertValidation:
    """Test validation on insert."""

    def test_insert_valid_data(self, validated_table, rules_path):
        catalog, table, rp = validated_table
        add_validation_rule(f"default.{table}", {"type": "not_null", "column": "id"}, store_path=rp)

        # Patch the default path for integration
        import lakehouse.validation as val_mod
        original = val_mod.DEFAULT_VALIDATION_PATH
        val_mod.DEFAULT_VALIDATION_PATH = rp
        try:
            count = insert_rows(catalog, table, [{"id": 1, "name": "test", "amount": 10.0}])
            assert count == 1
        finally:
            val_mod.DEFAULT_VALIDATION_PATH = original

    def test_insert_invalid_data_raises(self, validated_table, rules_path):
        catalog, table, rp = validated_table
        add_validation_rule(f"default.{table}", {"type": "not_null", "column": "id"}, store_path=rp)

        import lakehouse.validation as val_mod
        original = val_mod.DEFAULT_VALIDATION_PATH
        val_mod.DEFAULT_VALIDATION_PATH = rp
        try:
            with pytest.raises(ValidationError):
                insert_rows(catalog, table, [{"id": None, "name": "test", "amount": 10.0}])
        finally:
            val_mod.DEFAULT_VALIDATION_PATH = original

    def test_insert_range_violation(self, validated_table, rules_path):
        catalog, table, rp = validated_table
        add_validation_rule(f"default.{table}", {"type": "range", "column": "amount", "min": 0}, store_path=rp)

        import lakehouse.validation as val_mod
        original = val_mod.DEFAULT_VALIDATION_PATH
        val_mod.DEFAULT_VALIDATION_PATH = rp
        try:
            with pytest.raises(ValidationError):
                insert_rows(catalog, table, [{"id": 1, "name": "test", "amount": -5.0}])
        finally:
            val_mod.DEFAULT_VALIDATION_PATH = original


class TestUpdateValidation:
    """Test validation on update."""

    def test_update_valid_data(self, validated_table, rules_path):
        catalog, table, rp = validated_table
        insert_rows(catalog, table, [{"id": 1, "name": "test", "amount": 10.0}])

        add_validation_rule(f"default.{table}", {"type": "range", "column": "amount", "min": 0}, store_path=rp)

        import lakehouse.validation as val_mod
        original = val_mod.DEFAULT_VALIDATION_PATH
        val_mod.DEFAULT_VALIDATION_PATH = rp
        try:
            count = update_rows(catalog, table, "id = 1", {"amount": 20.0})
            assert count == 1
        finally:
            val_mod.DEFAULT_VALIDATION_PATH = original

    def test_update_invalid_data_raises(self, validated_table, rules_path):
        catalog, table, rp = validated_table
        insert_rows(catalog, table, [{"id": 1, "name": "test", "amount": 10.0}])

        add_validation_rule(f"default.{table}", {"type": "range", "column": "amount", "min": 0}, store_path=rp)

        import lakehouse.validation as val_mod
        original = val_mod.DEFAULT_VALIDATION_PATH
        val_mod.DEFAULT_VALIDATION_PATH = rp
        try:
            with pytest.raises(ValidationError):
                update_rows(catalog, table, "id = 1", {"amount": -50.0})
        finally:
            val_mod.DEFAULT_VALIDATION_PATH = original


class TestStoreResilience:
    """Test edge cases."""

    def test_load_nonexistent(self, rules_path):
        data = _load_rules(rules_path)
        assert data == {}

    def test_load_corrupt_json(self, rules_path):
        rules_path.parent.mkdir(parents=True, exist_ok=True)
        rules_path.write_text("not json{{{")
        data = _load_rules(rules_path)
        assert data == {}

    def test_creates_parent_dirs(self, rules_path):
        nested = rules_path.parent / "deep" / "nested" / "validation.json"
        add_validation_rule("expenses", {"type": "not_null", "column": "id"}, store_path=nested)
        assert nested.exists()
