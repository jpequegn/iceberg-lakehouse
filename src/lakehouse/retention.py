"""Snapshot retention policies — automated lifecycle rules for snapshot expiration."""

import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_RETENTION_PATH = Path.home() / ".lakehouse" / "retention.json"


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_RETENTION_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_RETENTION_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def set_retention_policy(
    table_name: str,
    policy: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Set a snapshot retention policy for a table.

    Args:
        table_name: Table name
        policy: Dict with:
            - max_snapshot_age_hours: Expire snapshots older than this (optional)
            - max_snapshot_count: Keep at most this many snapshots (optional)
            - min_snapshots_to_keep: Never go below this count (default: 1)

    Returns:
        Dict with policy details.
    """
    if not table_name or not table_name.strip():
        raise ValueError("Table name cannot be empty")

    table_name = _normalize(table_name)

    max_age = policy.get("max_snapshot_age_hours")
    max_count = policy.get("max_snapshot_count")
    min_keep = policy.get("min_snapshots_to_keep", 1)

    if max_age is not None and (not isinstance(max_age, (int, float)) or max_age <= 0):
        raise ValueError("max_snapshot_age_hours must be a positive number")
    if max_count is not None and (not isinstance(max_count, int) or max_count < 1):
        raise ValueError("max_snapshot_count must be a positive integer")
    if not isinstance(min_keep, int) or min_keep < 1:
        raise ValueError("min_snapshots_to_keep must be a positive integer")

    store = _load_store(store_path)
    store[table_name] = {
        "max_snapshot_age_hours": max_age,
        "max_snapshot_count": max_count,
        "min_snapshots_to_keep": min_keep,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "last_evaluated": None,
    }
    _save_store(store, store_path)

    return {
        "table": table_name,
        "policy": store[table_name],
        "message": f"Retention policy set for '{table_name}'",
    }


def get_retention_policy(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get the retention policy for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    policy = store.get(table_name)
    if policy is None:
        return {"table": table_name, "policy": None, "message": f"No retention policy for '{table_name}'"}
    return {"table": table_name, "policy": policy, "message": f"Retention policy for '{table_name}'"}


def list_retention_policies(store_path: Optional[Path] = None) -> list[dict]:
    """List all retention policies."""
    store = _load_store(store_path)
    return [
        {"table": table_name, **policy}
        for table_name, policy in store.items()
    ]


def remove_retention_policy(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a retention policy."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    if table_name in store:
        del store[table_name]
        _save_store(store, store_path)
        return {"table": table_name, "message": f"Retention policy removed for '{table_name}'"}
    return {"table": table_name, "message": f"No retention policy found for '{table_name}'"}


def evaluate_retention(
    catalog,
    table_name: Optional[str] = None,
    dry_run: bool = False,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Evaluate retention policies and expire snapshots.

    Args:
        catalog: Iceberg catalog
        table_name: Single table (or None for all tables with policies)
        dry_run: Preview what would be expired without acting

    Returns:
        List of actions taken/planned per table.
    """
    from .catalog import expire_snapshots
    from .audit import log_operation

    store = _load_store(store_path)

    if table_name:
        table_name = _normalize(table_name)
        tables = {table_name: store.get(table_name)} if table_name in store else {}
    else:
        tables = dict(store)

    results = []

    for tbl, policy in tables.items():
        if policy is None:
            continue

        try:
            table = catalog.load_table(tbl)
        except Exception as e:
            results.append({
                "table": tbl,
                "action": "error",
                "message": f"Could not load table: {e}",
            })
            continue

        metadata = table.metadata
        snapshots = sorted(metadata.snapshots, key=lambda s: s.timestamp_ms)
        total_snapshots = len(snapshots)
        min_keep = policy.get("min_snapshots_to_keep", 1)

        # Determine which snapshots to expire
        to_expire = set()

        # By age
        max_age_hours = policy.get("max_snapshot_age_hours")
        if max_age_hours is not None:
            now = datetime.datetime.now(datetime.timezone.utc)
            cutoff = now - datetime.timedelta(hours=max_age_hours)
            cutoff_ms = int(cutoff.timestamp() * 1000)
            for snap in snapshots:
                if snap.timestamp_ms < cutoff_ms:
                    to_expire.add(snap.snapshot_id)

        # By count
        max_count = policy.get("max_snapshot_count")
        if max_count is not None and total_snapshots > max_count:
            excess = total_snapshots - max_count
            for snap in snapshots[:excess]:
                to_expire.add(snap.snapshot_id)

        # Enforce min_snapshots_to_keep: ensure we keep at least min_keep snapshots
        if len(to_expire) > 0:
            keep_count = total_snapshots - len(to_expire)
            if keep_count < min_keep:
                # Remove some from to_expire to meet minimum
                sorted_expire = sorted(to_expire, key=lambda sid: next(
                    s.timestamp_ms for s in snapshots if s.snapshot_id == sid
                ))
                # Keep the most recent ones by removing oldest from expire list last
                needed = min_keep - keep_count
                for sid in reversed(sorted_expire):
                    if needed <= 0:
                        break
                    to_expire.discard(sid)
                    needed -= 1

        expire_count = len(to_expire)

        if expire_count == 0:
            results.append({
                "table": tbl,
                "action": "no_action",
                "total_snapshots": total_snapshots,
                "expired": 0,
                "remaining": total_snapshots,
                "dry_run": dry_run,
                "message": f"No snapshots to expire for '{tbl}'",
            })
            continue

        if dry_run:
            results.append({
                "table": tbl,
                "action": "would_expire",
                "total_snapshots": total_snapshots,
                "would_expire": expire_count,
                "would_remain": total_snapshots - expire_count,
                "dry_run": True,
                "message": f"Would expire {expire_count} snapshot(s) from '{tbl}' (keeping {total_snapshots - expire_count})",
            })
        else:
            # Use retain_last to do the actual expiration
            retain = total_snapshots - expire_count
            try:
                result = expire_snapshots(catalog, tbl, retain_last=retain)
                # Update last_evaluated
                store[tbl]["last_evaluated"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                _save_store(store, store_path)

                log_operation(
                    tbl, "retention_expire",
                    rows_affected=0,
                    source="retention",
                    details={"expired": expire_count, "retained": retain},
                )

                results.append({
                    "table": tbl,
                    "action": "expired",
                    "total_snapshots": total_snapshots,
                    "expired": expire_count,
                    "remaining": retain,
                    "dry_run": False,
                    "message": f"Expired {expire_count} snapshot(s) from '{tbl}' (keeping {retain})",
                })
            except Exception as e:
                results.append({
                    "table": tbl,
                    "action": "error",
                    "message": f"Expiration failed: {e}",
                })

    return results
