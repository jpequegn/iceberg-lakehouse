"""Tests for data contracts."""

import pytest

from lakehouse.contracts import (
    create_contract,
    get_contract,
    list_contracts,
    update_contract,
    remove_contract,
    get_contract_summary,
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
