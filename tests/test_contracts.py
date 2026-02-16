"""Tests for data contracts."""

import pytest

from lakehouse.contracts import (
    create_contract,
    get_contract,
    list_contracts,
    update_contract,
    remove_contract,
    get_contract_summary,
    validate_contract,
    validate_data_against_contract,
    get_contract_violations,
)
from lakehouse.catalog import create_table, insert_rows


@pytest.fixture
def store(tmp_path):
    return tmp_path / "contracts.json"


@pytest.fixture
def sample_contract():
    return {
        "schema": {
            "id": {"type": "long", "nullable": True},
            "name": {"type": "string", "nullable": True},
            "value": {"type": "double", "nullable": True},
        },
        "quality": {"min_score": 70},
        "freshness": {"max_age_hours": 48},
        "constraints": [
            {"column": "id", "rule": "not_null"},
            {"column": "value", "rule": "range", "min": 0, "max": 1000},
        ],
        "owner": "data-team",
        "description": "Core metrics table contract",
    }


@pytest.fixture
def contract_table(test_catalog):
    """Create a table matching the sample contract schema."""
    create_table(test_catalog, "metrics", columns={"id": "long", "name": "string", "value": "double"})
    insert_rows(test_catalog, "default.metrics", [
        {"id": 1, "name": "alice", "value": 10.0},
        {"id": 2, "name": "bob", "value": 20.0},
    ])
    return test_catalog


# --- create_contract ---


class TestCreateContract:
    def test_create_and_get(self, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        result = get_contract("metrics", store_path=store)
        assert result is not None
        assert result["table"] == "default.metrics"
        assert result["version"] == 1
        assert result["schema"]["id"]["type"] == "long"
        assert result["owner"] == "data-team"

    def test_create_normalizes_table_name(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = get_contract("tbl", store_path=store)
        assert result["table"] == "default.tbl"

    def test_create_duplicate_raises(self, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        with pytest.raises(ValueError, match="already exists"):
            create_contract("metrics", sample_contract, store_path=store)

    def test_create_minimal_contract(self, store):
        create_contract("tbl", {"description": "minimal"}, store_path=store)
        result = get_contract("tbl", store_path=store)
        assert result["description"] == "minimal"
        assert result["schema"] == {}
        assert result["constraints"] == []

    def test_create_full_contract(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = get_contract("tbl", store_path=store)
        assert len(result["constraints"]) == 2
        assert result["quality"]["min_score"] == 70
        assert result["freshness"]["max_age_hours"] == 48


# --- get_contract ---


class TestGetContract:
    def test_get_nonexistent(self, store):
        result = get_contract("nonexistent", store_path=store)
        assert result is None

    def test_get_excludes_internal_fields(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = get_contract("tbl", store_path=store)
        assert "_history" not in result


# --- list_contracts ---


class TestListContracts:
    def test_list_empty(self, store):
        assert list_contracts(store_path=store) == []

    def test_list_all(self, store, sample_contract):
        create_contract("t1", sample_contract, store_path=store)
        create_contract("t2", {"description": "other"}, store_path=store)
        result = list_contracts(store_path=store)
        assert len(result) == 2

    def test_list_filtered_by_namespace(self, store, sample_contract):
        create_contract("default.t1", sample_contract, store_path=store)
        create_contract("staging.t2", {"description": "staging"}, store_path=store)
        result = list_contracts(namespace="default", store_path=store)
        assert len(result) == 1
        assert result[0]["table"] == "default.t1"

    def test_list_shows_summary_fields(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = list_contracts(store_path=store)
        assert "owner" in result[0]
        assert "version" in result[0]
        assert "status" in result[0]


# --- update_contract ---


class TestUpdateContract:
    def test_partial_update(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        update_contract("tbl", {"owner": "new-team"}, store_path=store)
        result = get_contract("tbl", store_path=store)
        assert result["owner"] == "new-team"
        # Other fields preserved
        assert result["description"] == "Core metrics table contract"
        assert result["version"] == 2

    def test_update_bumps_version(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        update_contract("tbl", {"description": "v2"}, store_path=store)
        update_contract("tbl", {"description": "v3"}, store_path=store)
        result = get_contract("tbl", store_path=store)
        assert result["version"] == 3

    def test_update_nonexistent_raises(self, store):
        with pytest.raises(ValueError, match="No contract found"):
            update_contract("nonexistent", {"owner": "x"}, store_path=store)

    def test_update_schema(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        new_schema = {"id": {"type": "long", "nullable": False}, "email": {"type": "string", "nullable": True}}
        update_contract("tbl", {"schema": new_schema}, store_path=store)
        result = get_contract("tbl", store_path=store)
        assert "email" in result["schema"]
        assert "name" not in result["schema"]


# --- remove_contract ---


class TestRemoveContract:
    def test_remove_existing(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = remove_contract("tbl", store_path=store)
        assert "Removed" in result["message"]
        assert get_contract("tbl", store_path=store) is None

    def test_remove_nonexistent(self, store):
        result = remove_contract("nonexistent", store_path=store)
        assert "No contract found" in result["message"]


# --- get_contract_summary ---


class TestGetContractSummary:
    def test_summary_with_matching_schema(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        result = get_contract_summary(contract_table, "metrics", store_path=store)
        assert result["has_contract"] is True
        assert result["schema_match"] is True
        assert len(result["schema_issues"]) == 0

    def test_summary_no_contract(self, contract_table, store):
        result = get_contract_summary(contract_table, "metrics", store_path=store)
        assert result["has_contract"] is False

    def test_summary_schema_drift(self, contract_table, store):
        # Contract expects a column that doesn't exist
        contract = {
            "schema": {
                "id": {"type": "long"},
                "name": {"type": "string"},
                "missing_col": {"type": "string"},
            },
        }
        create_contract("metrics", contract, store_path=store)
        result = get_contract_summary(contract_table, "metrics", store_path=store)
        assert result["schema_match"] is False
        assert any("Missing column" in i for i in result["schema_issues"])

    def test_summary_type_mismatch(self, contract_table, store):
        contract = {
            "schema": {
                "id": {"type": "string"},  # Actually long
            },
        }
        create_contract("metrics", contract, store_path=store)
        result = get_contract_summary(contract_table, "metrics", store_path=store)
        assert result["schema_match"] is False
        assert any("Type mismatch" in i for i in result["schema_issues"])

    def test_summary_includes_constraint_count(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        result = get_contract_summary(contract_table, "metrics", store_path=store)
        assert result["constraint_count"] == 2


# --- validate_contract ---


class TestValidateContract:
    def test_validate_passes_compliant_table(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        result = validate_contract(contract_table, "metrics", store_path=store)
        assert result["valid"] is True
        assert result["violation_count"] == 0

    def test_validate_no_contract(self, contract_table, store):
        result = validate_contract(contract_table, "metrics", store_path=store)
        assert result["valid"] is True  # No contract = skip

    def test_validate_missing_column(self, contract_table, store):
        contract = {"schema": {"missing_col": {"type": "string"}}}
        create_contract("metrics", contract, store_path=store)
        result = validate_contract(contract_table, "metrics", store_path=store)
        assert result["valid"] is False
        assert any(v["rule"] == "exists" for v in result["violations"])

    def test_validate_type_mismatch(self, contract_table, store):
        contract = {"schema": {"id": {"type": "string"}}}  # Actually long
        create_contract("metrics", contract, store_path=store)
        result = validate_contract(contract_table, "metrics", store_path=store)
        assert result["valid"] is False
        assert any(v["rule"] == "type" for v in result["violations"])

    def test_validate_range_constraint(self, contract_table, store):
        contract = {"constraints": [{"column": "value", "rule": "range", "min": 0, "max": 15}]}
        create_contract("metrics", contract, store_path=store)
        result = validate_contract(contract_table, "metrics", store_path=store)
        # value=20.0 exceeds max=15
        assert result["valid"] is False
        assert any(v["rule"] == "range" for v in result["violations"])

    def test_validate_not_null_constraint(self, test_catalog, store):
        create_table(test_catalog, "nullable_tbl", columns={"id": "long", "name": "string"})
        insert_rows(test_catalog, "default.nullable_tbl", [
            {"id": 1, "name": None},
        ])
        contract = {"constraints": [{"column": "name", "rule": "not_null"}]}
        create_contract("nullable_tbl", contract, store_path=store)
        result = validate_contract(test_catalog, "nullable_tbl", store_path=store)
        assert result["valid"] is False
        assert any(v["rule"] == "not_null" for v in result["violations"])


# --- validate_data_against_contract ---


class TestValidateDataAgainstContract:
    def test_compliant_rows(self, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        rows = [{"id": 1, "name": "alice", "value": 10.0}]
        result = validate_data_against_contract("metrics", rows, store_path=store)
        assert result["valid"] is True

    def test_missing_column(self, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        rows = [{"id": 1, "name": "alice"}]  # Missing 'value'
        result = validate_data_against_contract("metrics", rows, store_path=store)
        assert result["valid"] is False
        assert any(v["rule"] == "exists" for v in result["violations"])

    def test_null_in_non_nullable(self, store):
        contract = {"schema": {"id": {"type": "long", "nullable": False}}}
        create_contract("tbl", contract, store_path=store)
        rows = [{"id": None}]
        result = validate_data_against_contract("tbl", rows, store_path=store)
        assert result["valid"] is False
        assert any(v["rule"] == "nullable" for v in result["violations"])

    def test_range_violation(self, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        rows = [{"id": 1, "name": "alice", "value": -5.0}]  # Below min=0
        result = validate_data_against_contract("metrics", rows, store_path=store)
        assert result["valid"] is False
        assert any(v["rule"] == "range" for v in result["violations"])

    def test_enum_violation(self, store):
        contract = {"constraints": [{"column": "status", "rule": "enum", "values": ["active", "inactive"]}]}
        create_contract("tbl", contract, store_path=store)
        rows = [{"status": "unknown"}]
        result = validate_data_against_contract("tbl", rows, store_path=store)
        assert result["valid"] is False
        assert any(v["rule"] == "enum" for v in result["violations"])

    def test_empty_rows_pass(self, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        result = validate_data_against_contract("metrics", [], store_path=store)
        assert result["valid"] is True

    def test_no_contract_accepts_all(self, store):
        rows = [{"anything": "goes"}]
        result = validate_data_against_contract("no_contract", rows, store_path=store)
        assert result["valid"] is True

    def test_violation_report_includes_row_index(self, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        rows = [
            {"id": 1, "name": "ok", "value": 10.0},
            {"id": 2, "name": "bad", "value": 2000.0},  # Above max=1000
        ]
        result = validate_data_against_contract("metrics", rows, store_path=store)
        assert result["valid"] is False
        bad_rows = [v for v in result["violations"] if v.get("row") == 1]
        assert len(bad_rows) > 0


# --- get_contract_violations ---


class TestGetContractViolations:
    def test_violations_includes_schema_and_constraints(self, contract_table, store):
        contract = {
            "schema": {"missing": {"type": "string"}},
            "constraints": [{"column": "value", "rule": "range", "min": 0, "max": 5}],
        }
        create_contract("metrics", contract, store_path=store)
        result = get_contract_violations(contract_table, "metrics", store_path=store)
        assert result["violation_count"] >= 2
        types = {v["type"] for v in result["violations"]}
        assert "schema" in types
        assert "constraint" in types
