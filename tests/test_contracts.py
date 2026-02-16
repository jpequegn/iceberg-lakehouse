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
    get_contract_history,
    get_contract_version,
    diff_contract_versions,
    deprecate_contract,
    get_contract_status,
    monitor_contract,
    get_compliance_history,
    get_compliance_score,
    add_consumer,
    add_producer,
    list_consumers,
    list_producers,
    remove_consumer,
    get_contract_coverage,
    generate_contract,
    preview_contract,
    apply_generated_contract,
    dry_run_contract,
    dry_run_migration,
    dry_run_report,
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


# --- get_contract_history ---


class TestGetContractHistory:
    def test_empty_history_on_new_contract(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        history = get_contract_history("tbl", store_path=store)
        assert history == []

    def test_history_after_updates(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        update_contract("tbl", {"owner": "team-a"}, store_path=store)
        update_contract("tbl", {"owner": "team-b"}, store_path=store)
        history = get_contract_history("tbl", store_path=store)
        assert len(history) == 2
        # Most recent first
        assert history[0]["owner"] == "team-a"
        assert history[1]["owner"] == "data-team"

    def test_history_limit(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        for i in range(5):
            update_contract("tbl", {"description": f"v{i + 2}"}, store_path=store)
        history = get_contract_history("tbl", limit=2, store_path=store)
        assert len(history) == 2

    def test_history_nonexistent(self, store):
        assert get_contract_history("nonexistent", store_path=store) == []

    def test_history_includes_snapshot_at(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        update_contract("tbl", {"owner": "new"}, store_path=store)
        history = get_contract_history("tbl", store_path=store)
        assert "snapshot_at" in history[0]


# --- get_contract_version ---


class TestGetContractVersion:
    def test_get_current_version(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = get_contract_version("tbl", 1, store_path=store)
        assert result is not None
        assert result["version"] == 1

    def test_get_historical_version(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        update_contract("tbl", {"owner": "new-team"}, store_path=store)
        # Version 1 should be in history
        v1 = get_contract_version("tbl", 1, store_path=store)
        assert v1 is not None
        assert v1["owner"] == "data-team"
        # Current is version 2
        v2 = get_contract_version("tbl", 2, store_path=store)
        assert v2 is not None
        assert v2["owner"] == "new-team"

    def test_get_nonexistent_version(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        assert get_contract_version("tbl", 99, store_path=store) is None

    def test_get_version_no_contract(self, store):
        assert get_contract_version("nonexistent", 1, store_path=store) is None


# --- diff_contract_versions ---


class TestDiffContractVersions:
    def test_diff_shows_changes(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        update_contract("tbl", {"owner": "new-team", "description": "updated"}, store_path=store)
        diff = diff_contract_versions("tbl", 1, 2, store_path=store)
        assert diff["change_count"] >= 2
        fields_changed = {c["field"] for c in diff["changes"]}
        assert "owner" in fields_changed
        assert "description" in fields_changed

    def test_diff_schema_added_column(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        new_schema = dict(sample_contract["schema"])
        new_schema["email"] = {"type": "string", "nullable": True}
        update_contract("tbl", {"schema": new_schema}, store_path=store)
        diff = diff_contract_versions("tbl", 1, 2, store_path=store)
        assert "email" in diff["schema_added"]

    def test_diff_schema_removed_column(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        new_schema = {"id": {"type": "long", "nullable": True}}
        update_contract("tbl", {"schema": new_schema}, store_path=store)
        diff = diff_contract_versions("tbl", 1, 2, store_path=store)
        assert "name" in diff["schema_removed"]
        assert "value" in diff["schema_removed"]

    def test_diff_nonexistent_version(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        diff = diff_contract_versions("tbl", 1, 99, store_path=store)
        assert "error" in diff

    def test_diff_no_changes(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        diff = diff_contract_versions("tbl", 1, 1, store_path=store)
        assert diff["change_count"] == 0


# --- deprecate_contract ---


class TestDeprecateContract:
    def test_deprecate(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = deprecate_contract("tbl", "Replaced by new table", store_path=store)
        assert result["status"] == "deprecated"
        contract = get_contract("tbl", store_path=store)
        assert contract["status"] == "deprecated"
        assert contract["deprecation_reason"] == "Replaced by new table"

    def test_deprecate_with_sunset(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        deprecate_contract("tbl", "EOL", sunset_date="2026-06-01", store_path=store)
        contract = get_contract("tbl", store_path=store)
        assert contract["sunset_date"] == "2026-06-01"

    def test_deprecate_nonexistent_raises(self, store):
        with pytest.raises(ValueError, match="No contract found"):
            deprecate_contract("nonexistent", "reason", store_path=store)


# --- get_contract_status ---


class TestGetContractStatus:
    def test_active_status(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = get_contract_status("tbl", store_path=store)
        assert result["status"] == "active"
        assert result["version"] == 1

    def test_deprecated_status(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        deprecate_contract("tbl", "old", store_path=store)
        result = get_contract_status("tbl", store_path=store)
        assert result["status"] == "deprecated"
        assert result["deprecation_reason"] == "old"
        assert "deprecated_at" in result

    def test_status_not_found(self, store):
        result = get_contract_status("nonexistent", store_path=store)
        assert result["status"] == "not_found"


# --- monitor_contract ---


class TestMonitorContract:
    def test_monitor_records_compliance(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        result = monitor_contract(contract_table, "metrics", store_path=store)
        assert result["checked"] is True
        assert result["passed"] is True

    def test_monitor_records_history(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        monitor_contract(contract_table, "metrics", store_path=store)
        history = get_compliance_history("metrics", store_path=store)
        assert len(history) == 1
        assert history[0]["passed"] is True

    def test_monitor_detects_violations(self, contract_table, store):
        contract = {"schema": {"missing_col": {"type": "string"}}}
        create_contract("metrics", contract, store_path=store)
        result = monitor_contract(contract_table, "metrics", store_path=store)
        assert result["passed"] is False
        assert result["violation_count"] > 0

    def test_monitor_fires_notification(self, contract_table, store, tmp_path):
        """Violations should fire contract_violation event."""
        from lakehouse.notifications import register_handler, get_event_history
        notif_store = tmp_path / "notifications.json"

        # Register a log handler for contract violations
        log_file = tmp_path / "violations.log"
        register_handler("*", "contract_violation", "log", {"file": str(log_file)}, store_path=notif_store)

        contract = {"schema": {"missing_col": {"type": "string"}}}
        create_contract("metrics", contract, store_path=store)
        monitor_contract(contract_table, "metrics", store_path=store, notification_store_path=notif_store)

        history = get_event_history(store_path=notif_store)
        assert len(history) >= 1
        assert history[0]["event_type"] == "contract_violation"

    def test_monitor_no_contract(self, contract_table, store):
        result = monitor_contract(contract_table, "metrics", store_path=store)
        assert result["checked"] is False


# --- get_compliance_history ---


class TestGetComplianceHistory:
    def test_empty_history(self, store):
        assert get_compliance_history("nonexistent", store_path=store) == []

    def test_history_accumulates(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        monitor_contract(contract_table, "metrics", store_path=store)
        monitor_contract(contract_table, "metrics", store_path=store)
        history = get_compliance_history("metrics", store_path=store)
        assert len(history) == 2

    def test_history_limit(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        for _ in range(5):
            monitor_contract(contract_table, "metrics", store_path=store)
        history = get_compliance_history("metrics", limit=2, store_path=store)
        assert len(history) == 2


# --- get_compliance_score ---


class TestGetComplianceScore:
    def test_perfect_score(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        result = get_compliance_score(contract_table, "metrics", store_path=store)
        assert result["score"] == 100.0
        assert result["schema_ratio"] == 1.0
        assert result["constraint_ratio"] == 1.0

    def test_score_decreases_with_schema_violations(self, contract_table, store):
        contract = {"schema": {"id": {"type": "long"}, "missing": {"type": "string"}}}
        create_contract("metrics", contract, store_path=store)
        result = get_compliance_score(contract_table, "metrics", store_path=store)
        assert result["score"] < 100.0
        assert result["schema_ratio"] < 1.0

    def test_score_decreases_with_constraint_violations(self, contract_table, store):
        contract = {"constraints": [{"column": "value", "rule": "range", "min": 0, "max": 5}]}
        create_contract("metrics", contract, store_path=store)
        result = get_compliance_score(contract_table, "metrics", store_path=store)
        assert result["score"] < 100.0
        assert result["constraint_ratio"] < 1.0

    def test_score_no_contract(self, contract_table, store):
        result = get_compliance_score(contract_table, "metrics", store_path=store)
        assert result["score"] is None


# --- add_consumer / list_consumers / remove_consumer ---


class TestConsumers:
    def test_add_and_list_consumer(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        add_consumer("tbl", "analytics-team", store_path=store)
        consumers = list_consumers("tbl", store_path=store)
        assert len(consumers) == 1
        assert consumers[0]["name"] == "analytics-team"

    def test_add_consumer_with_contact(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        add_consumer("tbl", "ml-team", contact="ml@example.com", usage="model training", store_path=store)
        consumers = list_consumers("tbl", store_path=store)
        assert consumers[0]["contact"] == "ml@example.com"
        assert consumers[0]["usage"] == "model training"

    def test_multiple_consumers(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        add_consumer("tbl", "team-a", store_path=store)
        add_consumer("tbl", "team-b", store_path=store)
        consumers = list_consumers("tbl", store_path=store)
        assert len(consumers) == 2

    def test_duplicate_consumer(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        add_consumer("tbl", "team-a", store_path=store)
        result = add_consumer("tbl", "team-a", store_path=store)
        assert "already registered" in result["message"]
        assert len(list_consumers("tbl", store_path=store)) == 1

    def test_remove_consumer(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        add_consumer("tbl", "team-a", store_path=store)
        remove_consumer("tbl", "team-a", store_path=store)
        assert list_consumers("tbl", store_path=store) == []

    def test_remove_nonexistent_consumer(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        result = remove_consumer("tbl", "ghost", store_path=store)
        assert "not found" in result["message"]

    def test_no_contract_returns_empty(self, store):
        assert list_consumers("nonexistent", store_path=store) == []

    def test_add_consumer_no_contract_raises(self, store):
        with pytest.raises(ValueError, match="No contract found"):
            add_consumer("nonexistent", "team", store_path=store)


# --- add_producer / list_producers ---


class TestProducers:
    def test_add_and_list_producer(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        add_producer("tbl", "ingestion-pipeline", store_path=store)
        producers = list_producers("tbl", store_path=store)
        assert len(producers) == 1
        assert producers[0]["name"] == "ingestion-pipeline"

    def test_producer_with_contact(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        add_producer("tbl", "etl", contact="data-eng@example.com", store_path=store)
        producers = list_producers("tbl", store_path=store)
        assert producers[0]["contact"] == "data-eng@example.com"

    def test_replace_producer(self, store, sample_contract):
        create_contract("tbl", sample_contract, store_path=store)
        add_producer("tbl", "old-pipeline", store_path=store)
        add_producer("tbl", "new-pipeline", store_path=store)
        producers = list_producers("tbl", store_path=store)
        assert len(producers) == 1
        assert producers[0]["name"] == "new-pipeline"

    def test_no_contract_returns_empty(self, store):
        assert list_producers("nonexistent", store_path=store) == []


# --- get_contract_coverage ---


class TestContractCoverage:
    def test_coverage_with_mix(self, test_catalog, store, sample_contract):
        from lakehouse.catalog import list_tables
        all_tables = list_tables(test_catalog, namespace="*")
        # Contract one table, leave rest uncovered
        create_contract(all_tables[0], sample_contract, store_path=store)
        result = get_contract_coverage(test_catalog, store_path=store)
        assert result["total_tables"] == len(all_tables)
        assert result["contracted"] == 1
        assert result["uncovered_count"] == len(all_tables) - 1
        assert 0 < result["coverage_pct"] < 100

    def test_coverage_all_contracted(self, test_catalog, store, sample_contract):
        from lakehouse.catalog import list_tables
        all_tables = list_tables(test_catalog, namespace="*")
        for t in all_tables:
            create_contract(t, sample_contract, store_path=store)
        result = get_contract_coverage(test_catalog, store_path=store)
        assert result["coverage_pct"] == 100.0

    def test_coverage_none_contracted(self, test_catalog, store):
        result = get_contract_coverage(test_catalog, store_path=store)
        assert result["coverage_pct"] == 0.0
        assert result["uncovered_count"] == result["total_tables"]


# --- generate_contract / preview_contract / apply_generated_contract ---


class TestGenerateContract:
    def test_generate_infers_schema(self, contract_table, store):
        result = generate_contract(contract_table, "metrics", store_path=store)
        contract = result["contract"]
        assert "id" in contract["schema"]
        assert contract["schema"]["id"]["type"] == "long"
        assert "name" in contract["schema"]
        assert "value" in contract["schema"]

    def test_generate_saves_contract(self, contract_table, store):
        generate_contract(contract_table, "metrics", store_path=store)
        result = get_contract("metrics", store_path=store)
        assert result is not None
        assert result["version"] == 1

    def test_generate_infers_not_null(self, contract_table, store):
        result = generate_contract(contract_table, "metrics", store_path=store)
        constraints = result["contract"]["constraints"]
        not_null_cols = [c["column"] for c in constraints if c["rule"] == "not_null"]
        # All columns in contract_table have non-null values
        assert "id" in not_null_cols

    def test_generate_infers_range(self, contract_table, store):
        result = generate_contract(contract_table, "metrics", store_path=store)
        constraints = result["contract"]["constraints"]
        range_constraints = [c for c in constraints if c["rule"] == "range"]
        assert len(range_constraints) > 0  # id and value are numeric

    def test_generate_infers_enum(self, test_catalog, store):
        create_table(test_catalog, "status_tbl", columns={"status": "string", "id": "long"})
        insert_rows(test_catalog, "default.status_tbl", [
            {"status": "active", "id": 1},
            {"status": "inactive", "id": 2},
            {"status": "active", "id": 3},
        ])
        result = generate_contract(test_catalog, "status_tbl", store_path=store)
        constraints = result["contract"]["constraints"]
        enum_constraints = [c for c in constraints if c["rule"] == "enum"]
        assert len(enum_constraints) == 1
        assert set(enum_constraints[0]["values"]) == {"active", "inactive"}

    def test_strict_mode_tighter(self, contract_table, store):
        contract = preview_contract(contract_table, "metrics", strict=True)
        # Strict should have range constraints with exact min/max
        range_constraints = [c for c in contract["constraints"] if c["rule"] == "range" and c["column"] == "value"]
        if range_constraints:
            rc = range_constraints[0]
            assert rc["min"] == 10.0  # Exact data min
            assert rc["max"] == 20.0  # Exact data max


class TestPreviewContract:
    def test_preview_does_not_save(self, contract_table, store):
        preview_contract(contract_table, "metrics")
        result = get_contract("metrics", store_path=store)
        assert result is None

    def test_preview_returns_contract(self, contract_table, store):
        contract = preview_contract(contract_table, "metrics")
        assert "schema" in contract
        assert "constraints" in contract
        assert "description" in contract


class TestApplyGeneratedContract:
    def test_apply_creates_new(self, contract_table, store):
        contract = preview_contract(contract_table, "metrics")
        result = apply_generated_contract(contract_table, "metrics", contract, store_path=store)
        assert "Created" in result["message"] or "version" in result
        saved = get_contract("metrics", store_path=store)
        assert saved is not None

    def test_apply_updates_existing(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        contract = preview_contract(contract_table, "metrics")
        result = apply_generated_contract(contract_table, "metrics", contract, store_path=store)
        saved = get_contract("metrics", store_path=store)
        assert saved["version"] == 2


# --- dry_run_contract ---


class TestDryRunContract:
    def test_passes_on_compliant_data(self, contract_table):
        contract = {
            "schema": {"id": {"type": "long"}, "name": {"type": "string"}, "value": {"type": "double"}},
            "constraints": [{"column": "value", "rule": "range", "min": 0, "max": 100}],
        }
        result = dry_run_contract(contract_table, "metrics", contract)
        assert result["valid"] is True
        assert result["violation_count"] == 0

    def test_fails_on_non_compliant(self, contract_table):
        contract = {"schema": {"missing_col": {"type": "string"}}}
        result = dry_run_contract(contract_table, "metrics", contract)
        assert result["valid"] is False
        assert any(v["rule"] == "exists" for v in result["violations"])

    def test_does_not_persist(self, contract_table, store):
        contract = {"schema": {"id": {"type": "long"}}}
        dry_run_contract(contract_table, "metrics", contract)
        assert get_contract("metrics", store_path=store) is None

    def test_constraint_violation(self, contract_table):
        contract = {"constraints": [{"column": "value", "rule": "range", "min": 0, "max": 15}]}
        result = dry_run_contract(contract_table, "metrics", contract)
        assert result["valid"] is False


# --- dry_run_migration ---


class TestDryRunMigration:
    def test_migration_introduces_violations(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        # New contract with tighter range
        new_contract = {"schema": sample_contract["schema"], "constraints": [{"column": "value", "rule": "range", "min": 0, "max": 15}]}
        result = dry_run_migration(contract_table, "metrics", new_contract, store_path=store)
        assert result["introduced_count"] > 0
        assert result["safe_to_migrate"] is False

    def test_migration_resolves_violations(self, contract_table, store):
        # Current contract with missing column
        tight = {"schema": {"missing": {"type": "string"}}}
        create_contract("metrics", tight, store_path=store)
        # New contract that matches actual schema
        relaxed = {"schema": {"id": {"type": "long"}}}
        result = dry_run_migration(contract_table, "metrics", relaxed, store_path=store)
        assert result["resolved_count"] > 0

    def test_safe_migration(self, contract_table, store, sample_contract):
        create_contract("metrics", sample_contract, store_path=store)
        # Same contract = no new violations
        result = dry_run_migration(contract_table, "metrics", sample_contract, store_path=store)
        assert result["safe_to_migrate"] is True

    def test_no_current_contract(self, contract_table, store):
        new_contract = {"schema": {"id": {"type": "long"}}}
        result = dry_run_migration(contract_table, "metrics", new_contract, store_path=store)
        assert result["current_violations"] == 0


# --- dry_run_report ---


class TestDryRunReport:
    def test_report_all_pass(self, contract_table):
        contract = {
            "schema": {"id": {"type": "long"}, "name": {"type": "string"}},
            "constraints": [{"column": "id", "rule": "not_null"}],
        }
        report = dry_run_report(contract_table, "metrics", contract)
        assert report["schema_compatible"] is True
        assert report["overall_pass"] is True
        assert report["constraint_results"][0]["pass_rate"] == 100.0

    def test_report_with_violations(self, contract_table):
        contract = {
            "schema": {"missing": {"type": "string"}},
            "constraints": [{"column": "value", "rule": "range", "min": 0, "max": 5}],
        }
        report = dry_run_report(contract_table, "metrics", contract)
        assert report["schema_compatible"] is False
        assert report["overall_pass"] is False
        # value=20 > 5, so violations > 0
        range_results = [r for r in report["constraint_results"] if r["rule"] == "range"]
        assert range_results[0]["violations"] > 0
        assert range_results[0]["pass_rate"] < 100.0

    def test_report_per_column_pass_rate(self, contract_table):
        contract = {"constraints": [{"column": "value", "rule": "range", "min": 0, "max": 15}]}
        report = dry_run_report(contract_table, "metrics", contract)
        # value=10 passes, value=20 fails → 50% pass rate
        assert report["constraint_results"][0]["pass_rate"] == 50.0

    def test_report_empty_contract(self, contract_table):
        report = dry_run_report(contract_table, "metrics", {})
        assert report["schema_compatible"] is True
        assert report["overall_pass"] is True
