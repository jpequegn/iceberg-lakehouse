"""SQL views — named virtual tables resolved at query time."""

import datetime
import json
from pathlib import Path
from typing import Optional

import pandas as pd

DEFAULT_VIEWS_PATH = Path.home() / ".lakehouse" / "views.json"


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_VIEWS_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_VIEWS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def create_view(
    name: str,
    sql: str,
    description: str = "",
    store_path: Optional[Path] = None,
) -> dict:
    """Create a named SQL view.

    Args:
        name: View name (e.g. 'recent_expenses')
        sql: SQL SELECT query defining the view
        description: Optional description
        store_path: Optional path to views store

    Returns:
        Dict with view details.

    Raises:
        ValueError: If name is empty, already exists, or SQL is empty.
    """
    if not name or not name.strip():
        raise ValueError("View name cannot be empty")
    if not sql or not sql.strip():
        raise ValueError("View SQL cannot be empty")

    name = name.strip()
    store = _load_store(store_path)

    if name in store:
        raise ValueError(f"View '{name}' already exists. Drop it first to recreate.")

    entry = {
        "sql": sql.strip(),
        "description": description,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    store[name] = entry
    _save_store(store, store_path)

    return {
        "name": name,
        **entry,
        "message": f"View '{name}' created",
    }


def list_views(store_path: Optional[Path] = None) -> list[dict]:
    """List all views with their names, SQL, and descriptions."""
    store = _load_store(store_path)
    return [
        {
            "name": name,
            "sql": data["sql"],
            "description": data.get("description", ""),
            "created_at": data.get("created_at", ""),
        }
        for name, data in store.items()
    ]


def get_view(name: str, store_path: Optional[Path] = None) -> dict:
    """Get a view definition by name.

    Raises:
        ValueError: If view not found.
    """
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"View '{name}' not found")

    data = store[name]
    return {
        "name": name,
        "sql": data["sql"],
        "description": data.get("description", ""),
        "created_at": data.get("created_at", ""),
    }


def drop_view(name: str, store_path: Optional[Path] = None) -> dict:
    """Drop a view by name.

    Raises:
        ValueError: If view not found.
    """
    store = _load_store(store_path)
    if name not in store:
        raise ValueError(f"View '{name}' not found")

    del store[name]
    _save_store(store, store_path)

    return {"name": name, "message": f"View '{name}' dropped"}


def query_view(
    name: str,
    engine,
    max_rows: int = 1000,
    store_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Execute a view's SQL and return results.

    Args:
        name: View name
        engine: QueryEngine instance
        max_rows: Maximum rows to return
        store_path: Optional path to views store

    Returns:
        DataFrame with query results.

    Raises:
        ValueError: If view not found.
    """
    view = get_view(name, store_path)
    return engine.execute(view["sql"], max_rows=max_rows)
