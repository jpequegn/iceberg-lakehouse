"""Audit log for tracking all write operations."""

import datetime
import json
from pathlib import Path
from typing import Optional


DEFAULT_AUDIT_PATH = Path.home() / ".lakehouse" / "audit.log"
MAX_AUDIT_ENTRIES = 10000


def log_operation(
    table_name: str,
    operation: str,
    rows_affected: int = 0,
    source: str = "api",
    details: Optional[dict] = None,
    store_path: Optional[Path] = None,
) -> None:
    """Append an audit entry.

    Args:
        table_name: Name of the table
        operation: Operation type (insert, update, delete, etc.)
        rows_affected: Number of rows affected
        source: Source of operation (cli, mcp, api)
        details: Additional operation-specific metadata
        store_path: Optional path to audit log file
    """
    path = store_path or DEFAULT_AUDIT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)

    entry = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "table": table_name,
        "operation": operation,
        "rows_affected": rows_affected,
        "source": source,
        "details": details or {},
    }

    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")

    # Enforce cap
    _enforce_cap(path)


def get_audit_log(
    table_name: Optional[str] = None,
    operation: Optional[str] = None,
    limit: int = 50,
    since: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Query the audit log with optional filters.

    Args:
        table_name: Filter by table name
        operation: Filter by operation type
        limit: Maximum entries to return
        since: ISO timestamp — only entries after this time
        store_path: Optional path to audit log file

    Returns:
        List of audit entries (most recent first)
    """
    path = store_path or DEFAULT_AUDIT_PATH
    if not path.exists():
        return []

    entries = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        if table_name and entry.get("table") != table_name:
            continue
        if operation and entry.get("operation") != operation:
            continue
        if since:
            entry_ts = entry.get("timestamp", "")
            if entry_ts < since:
                continue

        entries.append(entry)

    # Most recent first
    entries.reverse()
    return entries[:limit]


def clear_audit_log(
    older_than: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Clear audit entries.

    Args:
        older_than: If provided, only clear entries older than this.
                    Accepts ISO timestamp or duration like '30d', '24h'.
        store_path: Optional path to audit log file

    Returns:
        Dict with cleared count and message
    """
    path = store_path or DEFAULT_AUDIT_PATH
    if not path.exists():
        return {"cleared": 0, "message": "No audit log found"}

    lines = path.read_text().splitlines()
    if not lines:
        return {"cleared": 0, "message": "Audit log is empty"}

    if older_than is None:
        count = len(lines)
        path.write_text("")
        return {"cleared": count, "message": f"Cleared all {count} audit entries"}

    cutoff = _parse_older_than(older_than)
    kept = []
    cleared = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            entry_ts = entry.get("timestamp", "")
            if entry_ts < cutoff:
                cleared += 1
            else:
                kept.append(line)
        except json.JSONDecodeError:
            kept.append(line)

    path.write_text("\n".join(kept) + "\n" if kept else "")
    return {"cleared": cleared, "remaining": len(kept), "message": f"Cleared {cleared} entries older than {older_than}"}


def _parse_older_than(value: str) -> str:
    """Parse an older_than value to an ISO timestamp string.

    Accepts ISO timestamp or duration strings like '30d', '24h', '7d'.
    """
    # Try as ISO timestamp first
    try:
        datetime.datetime.fromisoformat(value)
        return value
    except (ValueError, TypeError):
        pass

    # Parse duration
    now = datetime.datetime.now(datetime.timezone.utc)
    value = value.strip().lower()

    if value.endswith("d"):
        days = int(value[:-1])
        cutoff = now - datetime.timedelta(days=days)
    elif value.endswith("h"):
        hours = int(value[:-1])
        cutoff = now - datetime.timedelta(hours=hours)
    elif value.endswith("m"):
        minutes = int(value[:-1])
        cutoff = now - datetime.timedelta(minutes=minutes)
    else:
        raise ValueError(f"Invalid older_than format: '{value}'. Use ISO timestamp or duration (e.g., '30d', '24h')")

    return cutoff.isoformat()


def _enforce_cap(path: Path) -> None:
    """Enforce max entries cap by removing oldest entries."""
    lines = path.read_text().splitlines()
    if len(lines) > MAX_AUDIT_ENTRIES:
        # Keep only the most recent entries
        path.write_text("\n".join(lines[-MAX_AUDIT_ENTRIES:]) + "\n")
