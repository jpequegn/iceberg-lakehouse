"""Saved queries and query history management."""

import datetime
import json
from pathlib import Path
from typing import Optional


DEFAULT_QUERIES_PATH = Path.home() / ".lakehouse" / "queries.json"
MAX_HISTORY_ENTRIES = 1000


def _load_store(store_path: Optional[Path] = None) -> dict:
    """Load the queries store from disk."""
    path = store_path or DEFAULT_QUERIES_PATH
    if not path.exists():
        return {"saved": {}, "history": []}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {"saved": {}, "history": []}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    """Save the queries store to disk."""
    path = store_path or DEFAULT_QUERIES_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def save_query(
    name: str,
    sql: str,
    description: str = "",
    store_path: Optional[Path] = None,
) -> dict:
    """Save a named query.

    Args:
        name: Query name (must be unique)
        sql: SQL query string
        description: Optional description
        store_path: Optional path to queries store

    Returns:
        Dict with saved query details

    Raises:
        ValueError: If name is empty or already exists
    """
    if not name or not name.strip():
        raise ValueError("Query name must not be empty")
    if not sql or not sql.strip():
        raise ValueError("SQL query must not be empty")

    store = _load_store(store_path)

    if name in store.get("saved", {}):
        raise ValueError(f"Query '{name}' already exists. Delete it first to replace.")

    store.setdefault("saved", {})[name] = {
        "sql": sql,
        "description": description,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    _save_store(store, store_path)

    return {
        "name": name,
        "sql": sql,
        "description": description,
        "message": f"Saved query '{name}'",
    }


def list_saved_queries(
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all saved queries.

    Returns:
        List of dicts with name, sql, description, created_at
    """
    store = _load_store(store_path)
    queries = []
    for name, data in store.get("saved", {}).items():
        queries.append({
            "name": name,
            "sql": data["sql"],
            "description": data.get("description", ""),
            "created_at": data.get("created_at", ""),
        })
    return queries


def get_saved_query(
    name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get a saved query by name.

    Args:
        name: Query name

    Returns:
        Dict with query details

    Raises:
        ValueError: If query not found
    """
    store = _load_store(store_path)
    saved = store.get("saved", {})

    if name not in saved:
        raise ValueError(f"Saved query '{name}' not found")

    data = saved[name]
    return {
        "name": name,
        "sql": data["sql"],
        "description": data.get("description", ""),
        "created_at": data.get("created_at", ""),
    }


def delete_saved_query(
    name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Delete a saved query.

    Args:
        name: Query name

    Returns:
        Dict with deletion details

    Raises:
        ValueError: If query not found
    """
    store = _load_store(store_path)
    saved = store.get("saved", {})

    if name not in saved:
        raise ValueError(f"Saved query '{name}' not found")

    del saved[name]
    _save_store(store, store_path)

    return {
        "name": name,
        "message": f"Deleted saved query '{name}'",
    }


def add_history_entry(
    sql: str,
    rows_returned: int = 0,
    duration_ms: int = 0,
    store_path: Optional[Path] = None,
) -> None:
    """Add a query to the execution history.

    Args:
        sql: SQL query that was executed
        rows_returned: Number of rows returned
        duration_ms: Execution time in milliseconds
        store_path: Optional path to queries store
    """
    store = _load_store(store_path)
    history = store.setdefault("history", [])

    history.append({
        "sql": sql,
        "executed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "rows_returned": rows_returned,
        "duration_ms": duration_ms,
    })

    # Cap at MAX_HISTORY_ENTRIES
    if len(history) > MAX_HISTORY_ENTRIES:
        store["history"] = history[-MAX_HISTORY_ENTRIES:]

    _save_store(store, store_path)


def get_history(
    limit: int = 20,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Get recent query history.

    Args:
        limit: Maximum entries to return (most recent first)

    Returns:
        List of history entry dicts (most recent first)
    """
    store = _load_store(store_path)
    history = store.get("history", [])
    # Return most recent first
    return list(reversed(history[-limit:]))


def clear_history(
    store_path: Optional[Path] = None,
) -> dict:
    """Clear all query history.

    Returns:
        Dict with clear details
    """
    store = _load_store(store_path)
    count = len(store.get("history", []))
    store["history"] = []
    _save_store(store, store_path)

    return {
        "cleared": count,
        "message": f"Cleared {count} history entries",
    }
