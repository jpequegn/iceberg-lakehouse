"""Dependency auto-refresh — cascade refreshes through the lineage graph."""

import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_REFRESH_PATH = Path.home() / ".lakehouse" / "auto_refresh.json"
MAX_HISTORY = 100


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_REFRESH_PATH
    if not path.exists():
        return {"configs": {}, "history": []}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {"configs": {}, "history": []}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_REFRESH_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def set_auto_refresh(
    table_name: str,
    enabled: bool = True,
    config: Optional[dict] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Enable/disable auto-refresh for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    defaults = {
        "cascade_depth": 3,
        "refresh_matviews": True,
        "rerun_pipelines": True,
        "invalidate_caches": True,
    }
    if config:
        defaults.update(config)

    store.setdefault("configs", {})[table_name] = {
        "enabled": enabled,
        "cascade_depth": defaults["cascade_depth"],
        "refresh_matviews": defaults["refresh_matviews"],
        "rerun_pipelines": defaults["rerun_pipelines"],
        "invalidate_caches": defaults["invalidate_caches"],
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    _save_store(store, store_path)

    status = "enabled" if enabled else "disabled"
    return {
        "table": table_name,
        "enabled": enabled,
        "config": defaults,
        "message": f"Auto-refresh {status} for '{table_name}'",
    }


def get_auto_refresh(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get auto-refresh configuration for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    config = store.get("configs", {}).get(table_name)

    if config is None:
        return {
            "table": table_name,
            "enabled": False,
            "message": f"No auto-refresh config for '{table_name}'",
        }

    return {
        "table": table_name,
        **config,
        "message": f"Auto-refresh for '{table_name}': {'enabled' if config['enabled'] else 'disabled'}",
    }


def list_auto_refresh(
    store_path: Optional[Path] = None,
) -> list[dict]:
    """List all tables with auto-refresh configured."""
    store = _load_store(store_path)
    result = []
    for table_name, config in store.get("configs", {}).items():
        result.append({"table": table_name, **config})
    return result


def remove_auto_refresh(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Disable and remove auto-refresh for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    configs = store.get("configs", {})

    if table_name not in configs:
        return {"table": table_name, "message": f"No auto-refresh config for '{table_name}'"}

    del configs[table_name]
    _save_store(store, store_path)
    return {"table": table_name, "message": f"Removed auto-refresh for '{table_name}'"}


def get_refresh_plan(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
    lineage_store_path: Optional[Path] = None,
) -> dict:
    """Dry-run: show what would be refreshed without executing."""
    from .lineage import get_downstream

    table_name = _normalize(table_name)
    store = _load_store(store_path)
    config = store.get("configs", {}).get(table_name, {})
    max_depth = config.get("cascade_depth", 3)
    refresh_matviews = config.get("refresh_matviews", True)
    rerun_pipelines = config.get("rerun_pipelines", True)
    invalidate_caches = config.get("invalidate_caches", True)

    downstream = get_downstream(table_name, store_path=lineage_store_path)
    # Filter by max depth
    downstream = [d for d in downstream if d["depth"] <= max_depth]

    actions = []

    # Always invalidate cache for the source table itself
    if invalidate_caches:
        actions.append({
            "table": table_name,
            "action": "invalidate_cache",
            "depth": 0,
        })

    for dep in downstream:
        dep_table = dep["table"]
        depth = dep["depth"]
        operation = dep.get("operation", "")

        if invalidate_caches:
            actions.append({
                "table": dep_table,
                "action": "invalidate_cache",
                "depth": depth,
            })

        if refresh_matviews and operation in ("materialized_view", "create_matview", ""):
            actions.append({
                "table": dep_table,
                "action": "refresh_matview",
                "depth": depth,
            })

        if rerun_pipelines and operation in ("pipeline", ""):
            actions.append({
                "table": dep_table,
                "action": "rerun_pipeline",
                "depth": depth,
            })

    # Sort by depth for correct order
    actions.sort(key=lambda a: a["depth"])

    return {
        "table": table_name,
        "downstream_count": len(downstream),
        "actions": actions,
        "max_depth": max_depth,
        "message": f"Refresh plan for '{table_name}': {len(actions)} actions across {len(downstream)} downstream dependencies",
    }


def trigger_refresh(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
    lineage_store_path: Optional[Path] = None,
) -> dict:
    """Trigger a cascade refresh from a source table."""
    table_name = _normalize(table_name)
    plan = get_refresh_plan(catalog, table_name, store_path=store_path, lineage_store_path=lineage_store_path)

    results = []
    for action in plan["actions"]:
        result = _execute_action(catalog, action)
        results.append(result)

    # Record in history
    store = _load_store(store_path)
    history_entry = {
        "table": table_name,
        "triggered_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "actions_executed": len(results),
        "successes": sum(1 for r in results if r.get("status") == "success"),
        "errors": sum(1 for r in results if r.get("status") == "error"),
        "results": results,
    }
    store.setdefault("history", []).append(history_entry)
    store["history"] = store["history"][-MAX_HISTORY:]
    _save_store(store, store_path)

    return {
        "table": table_name,
        "actions_executed": len(results),
        "successes": history_entry["successes"],
        "errors": history_entry["errors"],
        "results": results,
        "message": f"Refresh cascade for '{table_name}': {len(results)} actions ({history_entry['successes']} success, {history_entry['errors']} errors)",
    }


def _execute_action(catalog, action: dict) -> dict:
    """Execute a single refresh action. Best-effort."""
    table = action["table"]
    action_type = action["action"]

    try:
        if action_type == "invalidate_cache":
            from .query_cache import invalidate
            invalidate(table_name=table)
            return {"table": table, "action": action_type, "status": "success"}

        elif action_type == "refresh_matview":
            # Attempt matview refresh — silently skip if not a matview
            try:
                from .matviews import refresh_materialized_view
                from .query import QueryEngine
                engine = QueryEngine(catalog=catalog)
                # Use the table name as matview name (short name)
                short_name = table.split(".")[-1] if "." in table else table
                refresh_materialized_view(short_name, engine=engine, catalog=catalog)
                return {"table": table, "action": action_type, "status": "success"}
            except (ValueError, KeyError):
                return {"table": table, "action": action_type, "status": "skipped", "reason": "not a materialized view"}

        elif action_type == "rerun_pipeline":
            # Attempt pipeline re-run — silently skip if no pipeline
            try:
                from .pipelines import run_pipeline
                from .query import QueryEngine
                engine = QueryEngine(catalog=catalog)
                short_name = table.split(".")[-1] if "." in table else table
                run_pipeline(short_name, catalog=catalog, engine=engine)
                return {"table": table, "action": action_type, "status": "success"}
            except (ValueError, KeyError):
                return {"table": table, "action": action_type, "status": "skipped", "reason": "no pipeline found"}

    except Exception as e:
        return {"table": table, "action": action_type, "status": "error", "error": str(e)}

    return {"table": table, "action": action_type, "status": "error", "error": f"Unknown action: {action_type}"}


def get_refresh_history(
    table_name: Optional[str] = None,
    limit: int = 20,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Get history of auto-refresh executions."""
    store = _load_store(store_path)
    history = store.get("history", [])

    if table_name:
        table_name = _normalize(table_name)
        history = [h for h in history if h["table"] == table_name]

    return list(reversed(history[-limit:]))
