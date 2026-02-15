"""Tests for event notifications."""

import json
import pytest
from pathlib import Path

from lakehouse.notifications import (
    register_handler,
    list_handlers,
    remove_handler,
    fire_event,
    get_event_history,
    send_test_event,
    VALID_EVENT_TYPES,
    VALID_HANDLER_TYPES,
)


@pytest.fixture
def store(tmp_path):
    return tmp_path / "notifications.json"


# --- register_handler ---


class TestRegisterHandler:
    def test_register_webhook(self, store):
        result = register_handler("my_table", "write", "webhook", {"url": "http://example.com/hook"}, store_path=store)
        assert result["handler_id"]
        assert result["table"] == "default.my_table"
        assert result["event_type"] == "write"
        assert result["handler_type"] == "webhook"

    def test_register_shell(self, store):
        result = register_handler("my_table", "write", "shell", {"command": "echo hello"}, store_path=store)
        assert result["handler_type"] == "shell"

    def test_register_log(self, store):
        log_file = str(store.parent / "events.log")
        result = register_handler("my_table", "write", "log", {"file": log_file}, store_path=store)
        assert result["handler_type"] == "log"

    def test_normalize_table_name(self, store):
        result = register_handler("tbl", "write", "log", {"file": "/tmp/l.log"}, store_path=store)
        assert result["table"] == "default.tbl"

    def test_invalid_event_type(self, store):
        with pytest.raises(ValueError, match="Invalid event type"):
            register_handler("tbl", "bad_event", "log", {"file": "/tmp/l.log"}, store_path=store)

    def test_invalid_handler_type(self, store):
        with pytest.raises(ValueError, match="Invalid handler type"):
            register_handler("tbl", "write", "email", {}, store_path=store)

    def test_webhook_missing_url(self, store):
        with pytest.raises(ValueError, match="url"):
            register_handler("tbl", "write", "webhook", {}, store_path=store)

    def test_shell_missing_command(self, store):
        with pytest.raises(ValueError, match="command"):
            register_handler("tbl", "write", "shell", {}, store_path=store)

    def test_log_missing_file(self, store):
        with pytest.raises(ValueError, match="file"):
            register_handler("tbl", "write", "log", {}, store_path=store)

    def test_empty_table_name(self, store):
        with pytest.raises(ValueError, match="empty"):
            register_handler("", "write", "log", {"file": "/tmp/l.log"}, store_path=store)


# --- list_handlers ---


class TestListHandlers:
    def test_list_empty(self, store):
        assert list_handlers(store_path=store) == []

    def test_list_all(self, store):
        register_handler("t1", "write", "log", {"file": "/tmp/a.log"}, store_path=store)
        register_handler("t2", "write", "log", {"file": "/tmp/b.log"}, store_path=store)
        result = list_handlers(store_path=store)
        assert len(result) == 2

    def test_list_filtered_by_table(self, store):
        register_handler("t1", "write", "log", {"file": "/tmp/a.log"}, store_path=store)
        register_handler("t2", "write", "log", {"file": "/tmp/b.log"}, store_path=store)
        result = list_handlers(table_name="t1", store_path=store)
        assert len(result) == 1
        assert result[0]["table"] == "default.t1"


# --- remove_handler ---


class TestRemoveHandler:
    def test_remove_existing(self, store):
        reg = register_handler("tbl", "write", "log", {"file": "/tmp/l.log"}, store_path=store)
        result = remove_handler(reg["handler_id"], store_path=store)
        assert "Removed" in result["message"]
        assert list_handlers(store_path=store) == []

    def test_remove_nonexistent(self, store):
        result = remove_handler("nonexistent", store_path=store)
        assert "No handler found" in result["message"]


# --- fire_event ---


class TestFireEvent:
    def test_fire_matches_handler(self, store):
        log_file = str(store.parent / "events.log")
        register_handler("tbl", "write", "log", {"file": log_file}, store_path=store)
        result = fire_event("tbl", "write", {"rows": 10}, store_path=store)
        assert result["handlers_triggered"] == 1
        assert result["results"][0]["status"] == "success"
        # Verify log file written
        assert Path(log_file).exists()

    def test_fire_no_match(self, store):
        register_handler("tbl", "write", "log", {"file": "/tmp/l.log"}, store_path=store)
        result = fire_event("other_table", "write", {}, store_path=store)
        assert result["handlers_triggered"] == 0

    def test_fire_event_type_all(self, store):
        """Handler with event_type='all' should match any event."""
        log_file = str(store.parent / "all_events.log")
        register_handler("tbl", "all", "log", {"file": log_file}, store_path=store)
        result = fire_event("tbl", "schema_change", {"change": "added column"}, store_path=store)
        assert result["handlers_triggered"] == 1

    def test_fire_wildcard_table(self, store):
        """Handler with table='*' should match any table."""
        log_file = str(store.parent / "wild.log")
        register_handler("*", "write", "log", {"file": log_file}, store_path=store)
        result = fire_event("any_table", "write", {}, store_path=store)
        assert result["handlers_triggered"] == 1

    def test_fire_shell_handler(self, store):
        register_handler("tbl", "write", "shell", {"command": "echo test"}, store_path=store)
        result = fire_event("tbl", "write", {"rows": 5}, store_path=store)
        assert result["handlers_triggered"] == 1
        assert result["results"][0]["status"] == "success"

    def test_fire_multiple_handlers(self, store):
        log1 = str(store.parent / "e1.log")
        log2 = str(store.parent / "e2.log")
        register_handler("tbl", "write", "log", {"file": log1}, store_path=store)
        register_handler("tbl", "write", "log", {"file": log2}, store_path=store)
        result = fire_event("tbl", "write", {}, store_path=store)
        assert result["handlers_triggered"] == 2


# --- get_event_history ---


class TestGetEventHistory:
    def test_empty_history(self, store):
        assert get_event_history(store_path=store) == []

    def test_history_after_fire(self, store):
        log_file = str(store.parent / "h.log")
        register_handler("tbl", "write", "log", {"file": log_file}, store_path=store)
        fire_event("tbl", "write", {}, store_path=store)
        fire_event("tbl", "write", {}, store_path=store)
        history = get_event_history(store_path=store)
        assert len(history) == 2

    def test_history_filter_by_table(self, store):
        log_file = str(store.parent / "hf.log")
        register_handler("t1", "write", "log", {"file": log_file}, store_path=store)
        register_handler("t2", "write", "log", {"file": log_file}, store_path=store)
        fire_event("t1", "write", {}, store_path=store)
        fire_event("t2", "write", {}, store_path=store)
        history = get_event_history(table_name="t1", store_path=store)
        assert len(history) == 1
        assert history[0]["table"] == "default.t1"

    def test_history_filter_by_event_type(self, store):
        log_file = str(store.parent / "he.log")
        register_handler("tbl", "all", "log", {"file": log_file}, store_path=store)
        fire_event("tbl", "write", {}, store_path=store)
        fire_event("tbl", "schema_change", {}, store_path=store)
        history = get_event_history(event_type="write", store_path=store)
        assert len(history) == 1

    def test_history_limit(self, store):
        log_file = str(store.parent / "hl.log")
        register_handler("tbl", "write", "log", {"file": log_file}, store_path=store)
        for _ in range(10):
            fire_event("tbl", "write", {}, store_path=store)
        history = get_event_history(limit=3, store_path=store)
        assert len(history) == 3


# --- test_handler ---


class TestTestHandler:
    def test_valid_handler(self, store):
        log_file = str(store.parent / "test.log")
        reg = register_handler("tbl", "write", "log", {"file": log_file}, store_path=store)
        result = send_test_event(reg["handler_id"], store_path=store)
        assert result["result"]["status"] == "success"
        assert Path(log_file).exists()

    def test_nonexistent_handler(self, store):
        result = send_test_event("nonexistent", store_path=store)
        assert "No handler found" in result["message"]
