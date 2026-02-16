"""Event notifications — register handlers for table events."""

import datetime
import json
import subprocess
import urllib.request
import uuid
from pathlib import Path
from typing import Optional

DEFAULT_NOTIFICATIONS_PATH = Path.home() / ".lakehouse" / "notifications.json"
MAX_HISTORY = 200
VALID_EVENT_TYPES = {"write", "schema_change", "sla_violation", "maintenance", "contract_violation", "all"}
VALID_HANDLER_TYPES = {"webhook", "shell", "log"}


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_NOTIFICATIONS_PATH
    if not path.exists():
        return {"handlers": {}, "history": []}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {"handlers": {}, "history": []}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_NOTIFICATIONS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def register_handler(
    table_name: str,
    event_type: str,
    handler_type: str,
    config: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Register a notification handler for table events."""
    if not table_name or not table_name.strip():
        raise ValueError("Table name cannot be empty")

    table_name = _normalize(table_name)

    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"Invalid event type '{event_type}'. Must be one of: {VALID_EVENT_TYPES}")

    if handler_type not in VALID_HANDLER_TYPES:
        raise ValueError(f"Invalid handler type '{handler_type}'. Must be one of: {VALID_HANDLER_TYPES}")

    if handler_type == "webhook" and "url" not in config:
        raise ValueError("Webhook handler requires 'url' in config")
    if handler_type == "shell" and "command" not in config:
        raise ValueError("Shell handler requires 'command' in config")
    if handler_type == "log" and "file" not in config:
        raise ValueError("Log handler requires 'file' in config")

    store = _load_store(store_path)
    handler_id = uuid.uuid4().hex[:12]

    store.setdefault("handlers", {})[handler_id] = {
        "table": table_name,
        "event_type": event_type,
        "handler_type": handler_type,
        "config": config,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    _save_store(store, store_path)

    return {
        "handler_id": handler_id,
        "table": table_name,
        "event_type": event_type,
        "handler_type": handler_type,
        "message": f"Registered {handler_type} handler for '{table_name}' on {event_type} events",
    }


def list_handlers(
    table_name: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List registered handlers, optionally filtered by table."""
    store = _load_store(store_path)
    handlers = []

    if table_name:
        table_name = _normalize(table_name)

    for hid, h in store.get("handlers", {}).items():
        if table_name and h["table"] != table_name:
            continue
        handlers.append({"handler_id": hid, **h})

    return handlers


def remove_handler(
    handler_id: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a registered handler."""
    store = _load_store(store_path)
    handlers = store.get("handlers", {})

    if handler_id not in handlers:
        return {"handler_id": handler_id, "message": f"No handler found with ID '{handler_id}'"}

    del handlers[handler_id]
    _save_store(store, store_path)

    return {"handler_id": handler_id, "message": f"Removed handler '{handler_id}'"}


def fire_event(
    table_name: str,
    event_type: str,
    payload: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Fire an event, triggering all matching handlers."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    handlers = store.get("handlers", {})

    matched = []
    results = []

    for hid, h in handlers.items():
        # Match on table (exact or wildcard "*")
        table_match = h["table"] == table_name or h["table"] == "default.*" or h["table"] == "*"
        # Match on event type
        event_match = h["event_type"] == event_type or h["event_type"] == "all"

        if table_match and event_match:
            matched.append(hid)
            result = _execute_handler(h, table_name, event_type, payload)
            results.append({"handler_id": hid, **result})

    # Record in history
    history_entry = {
        "table": table_name,
        "event_type": event_type,
        "fired_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "handlers_triggered": len(matched),
        "results": results,
    }
    store.setdefault("history", []).append(history_entry)
    store["history"] = store["history"][-MAX_HISTORY:]
    _save_store(store, store_path)

    return {
        "table": table_name,
        "event_type": event_type,
        "handlers_triggered": len(matched),
        "results": results,
        "message": f"Fired '{event_type}' event for '{table_name}': {len(matched)} handlers triggered",
    }


def _execute_handler(handler: dict, table_name: str, event_type: str, payload: dict) -> dict:
    """Execute a single handler. Best-effort: errors don't propagate."""
    handler_type = handler["handler_type"]
    config = handler["config"]

    event_data = json.dumps({
        "table": table_name,
        "event_type": event_type,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "payload": payload,
    }, default=str)

    try:
        if handler_type == "webhook":
            url = config["url"]
            method = config.get("method", "POST")
            req = urllib.request.Request(
                url,
                data=event_data.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method=method,
            )
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return {"status": "success", "http_status": resp.status}
            except Exception as e:
                return {"status": "error", "error": str(e)}

        elif handler_type == "shell":
            command = config["command"]
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True, timeout=10,
                env={"LAKEHOUSE_EVENT": event_data},
            )
            return {
                "status": "success" if result.returncode == 0 else "error",
                "returncode": result.returncode,
                "stdout": result.stdout[:500],
                "stderr": result.stderr[:500],
            }

        elif handler_type == "log":
            log_file = Path(config["file"])
            log_file.parent.mkdir(parents=True, exist_ok=True)
            with open(log_file, "a") as f:
                f.write(event_data + "\n")
            return {"status": "success", "file": str(log_file)}

    except Exception as e:
        return {"status": "error", "error": str(e)}

    return {"status": "error", "error": f"Unknown handler type: {handler_type}"}


def get_event_history(
    table_name: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = 50,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Get history of fired events."""
    store = _load_store(store_path)
    history = store.get("history", [])

    if table_name:
        table_name = _normalize(table_name)
        history = [h for h in history if h["table"] == table_name]

    if event_type:
        history = [h for h in history if h["event_type"] == event_type]

    return list(reversed(history[-limit:]))


def send_test_event(
    handler_id: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Send a test event to verify a handler is working."""
    store = _load_store(store_path)
    handlers = store.get("handlers", {})

    if handler_id not in handlers:
        return {"handler_id": handler_id, "message": f"No handler found with ID '{handler_id}'"}

    handler = handlers[handler_id]
    test_payload = {"test": True, "message": "This is a test event from lakehouse"}

    result = _execute_handler(handler, handler["table"], "test", test_payload)

    return {
        "handler_id": handler_id,
        "handler_type": handler["handler_type"],
        "result": result,
        "message": f"Test event sent to handler '{handler_id}': {result['status']}",
    }
