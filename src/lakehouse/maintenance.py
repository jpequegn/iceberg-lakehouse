"""Scheduled maintenance policies for auto-compact, auto-expire, and orphan cleanup."""

import datetime
import json
from pathlib import Path
from typing import Optional

from pyiceberg.catalog import Catalog

DEFAULT_MAINTENANCE_PATH = Path.home() / ".lakehouse" / "maintenance.json"

DEFAULT_POLICY = {
    "auto_compact_threshold": 10,
    "auto_expire_retain_last": 5,
    "auto_expire_older_than": None,
    "auto_cleanup_orphans": True,
}


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_MAINTENANCE_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_MAINTENANCE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize_name(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def set_maintenance_policy(
    table_name: str,
    policy: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Set a maintenance policy for a table.

    Args:
        table_name: Table name (with or without namespace)
        policy: Dict with policy keys:
            auto_compact_threshold: int (default: 10)
            auto_expire_retain_last: int (default: 5)
            auto_expire_older_than: str or None (e.g. '30d')
            auto_cleanup_orphans: bool (default: True)
        store_path: Optional path to maintenance store

    Returns:
        Dict with the saved policy and message.
    """
    table_name = _normalize_name(table_name)

    # Merge with defaults
    merged = dict(DEFAULT_POLICY)
    for key in DEFAULT_POLICY:
        if key in policy:
            merged[key] = policy[key]

    merged["created_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    merged["last_run"] = None

    store = _load_store(store_path)
    store[table_name] = merged
    _save_store(store, store_path)

    return {
        "table": table_name,
        "policy": merged,
        "message": f"Maintenance policy set for {table_name}",
    }


def get_maintenance_policy(
    table_name: str,
    store_path: Optional[Path] = None,
) -> Optional[dict]:
    """Get the maintenance policy for a table.

    Returns None if no policy exists.
    """
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)
    return store.get(table_name)


def remove_maintenance_policy(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove maintenance policy for a table."""
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)

    if table_name not in store:
        return {"table": table_name, "message": f"No policy found for {table_name}"}

    del store[table_name]
    _save_store(store, store_path)
    return {"table": table_name, "message": f"Maintenance policy removed for {table_name}"}


def check_maintenance_needed(
    catalog: Catalog,
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Check if a table needs maintenance based on its policy.

    Returns:
        Dict with needs_compact, needs_expire, needs_cleanup booleans
        and detail messages.
    """
    from .catalog import maintenance_status

    table_name = _normalize_name(table_name)
    policy = get_maintenance_policy(table_name, store_path)

    if policy is None:
        return {
            "table": table_name,
            "has_policy": False,
            "needs_compact": False,
            "needs_expire": False,
            "needs_cleanup": False,
            "message": f"No maintenance policy for {table_name}",
        }

    maint = maintenance_status(catalog, table_name)

    needs_compact = maint["data_files"] >= policy["auto_compact_threshold"]
    needs_expire = maint["snapshots"] > policy["auto_expire_retain_last"]
    needs_cleanup = policy["auto_cleanup_orphans"] and maint.get("orphan_files", 0) > 0

    actions = []
    if needs_compact:
        actions.append(f"compact ({maint['data_files']} files >= {policy['auto_compact_threshold']} threshold)")
    if needs_expire:
        actions.append(f"expire ({maint['snapshots']} snapshots > {policy['auto_expire_retain_last']} retain)")
    if needs_cleanup:
        actions.append(f"cleanup ({maint.get('orphan_files', 0)} orphan files)")

    return {
        "table": table_name,
        "has_policy": True,
        "needs_compact": needs_compact,
        "needs_expire": needs_expire,
        "needs_cleanup": needs_cleanup,
        "data_files": maint["data_files"],
        "snapshots": maint["snapshots"],
        "orphan_files": maint.get("orphan_files", 0),
        "actions_needed": actions,
        "message": f"{len(actions)} action(s) needed" if actions else "No maintenance needed",
    }


def run_maintenance(
    catalog: Catalog,
    table_name: Optional[str] = None,
    dry_run: bool = False,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Run maintenance for a table or all tables with policies.

    Args:
        catalog: The Iceberg catalog
        table_name: Table to maintain (None = all tables with policies)
        dry_run: If True, report actions without executing
        store_path: Optional path to maintenance store

    Returns:
        List of action dicts with table, action, details, and status.
    """
    from .catalog import compact_table, expire_snapshots, cleanup_orphans
    from .audit import log_operation

    if table_name:
        table_name = _normalize_name(table_name)
        tables = [table_name]
    else:
        store = _load_store(store_path)
        tables = list(store.keys())

    actions = []

    for tbl in tables:
        check = check_maintenance_needed(catalog, tbl, store_path)
        if not check["has_policy"]:
            continue

        policy = get_maintenance_policy(tbl, store_path)

        # Compact
        if check["needs_compact"]:
            if dry_run:
                actions.append({
                    "table": tbl,
                    "action": "compact",
                    "status": "dry_run",
                    "detail": f"Would compact ({check['data_files']} files)",
                })
            else:
                try:
                    result = compact_table(catalog, tbl)
                    actions.append({
                        "table": tbl,
                        "action": "compact",
                        "status": "completed",
                        "detail": f"Compacted: {result.get('files_before', '?')} → {result.get('files_after', '?')} files",
                    })
                except Exception as e:
                    actions.append({
                        "table": tbl,
                        "action": "compact",
                        "status": "failed",
                        "detail": str(e),
                    })

        # Expire
        if check["needs_expire"]:
            if dry_run:
                actions.append({
                    "table": tbl,
                    "action": "expire",
                    "status": "dry_run",
                    "detail": f"Would expire snapshots (retain last {policy['auto_expire_retain_last']})",
                })
            else:
                try:
                    kwargs = {"retain_last": policy["auto_expire_retain_last"]}
                    if policy.get("auto_expire_older_than"):
                        kwargs["older_than"] = policy["auto_expire_older_than"]
                    result = expire_snapshots(catalog, tbl, **kwargs)
                    actions.append({
                        "table": tbl,
                        "action": "expire",
                        "status": "completed",
                        "detail": f"Expired: {result.get('expired_count', 0)} snapshot(s)",
                    })
                except Exception as e:
                    actions.append({
                        "table": tbl,
                        "action": "expire",
                        "status": "failed",
                        "detail": str(e),
                    })

        # Cleanup orphans
        if check["needs_cleanup"]:
            if dry_run:
                actions.append({
                    "table": tbl,
                    "action": "cleanup",
                    "status": "dry_run",
                    "detail": f"Would cleanup {check['orphan_files']} orphan file(s)",
                })
            else:
                try:
                    result = cleanup_orphans(catalog, tbl, dry_run=False)
                    actions.append({
                        "table": tbl,
                        "action": "cleanup",
                        "status": "completed",
                        "detail": f"Removed {result.get('files_removed', 0)} orphan file(s)",
                    })
                except Exception as e:
                    actions.append({
                        "table": tbl,
                        "action": "cleanup",
                        "status": "failed",
                        "detail": str(e),
                    })

        # Update last_run
        if not dry_run and actions:
            store = _load_store(store_path)
            if tbl in store:
                store[tbl]["last_run"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                _save_store(store, store_path)

            log_operation(
                tbl,
                "maintenance",
                source="api",
                details={"actions": [a["action"] for a in actions if a["table"] == tbl]},
            )

    return actions
