"""Lakehouse dashboard — comprehensive status overview."""

from pathlib import Path
from typing import Optional

from pyiceberg.catalog import Catalog


def _format_size(size_bytes: int) -> str:
    """Format bytes into human-readable size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _table_health(data_files: int, orphan_files: int, is_stale: bool) -> str:
    """Determine health indicator for a table.

    Returns one of: Good, Compact, Orphans, Stale
    """
    if is_stale:
        return "Stale"
    if orphan_files > 0:
        return "Orphans"
    if data_files >= 10:
        return "Compact"
    return "Good"


def get_dashboard(
    catalog: Catalog,
    warehouse_path: Optional[Path] = None,
    stats_store_path: Optional[Path] = None,
    audit_store_path: Optional[Path] = None,
    queries_store_path: Optional[Path] = None,
) -> dict:
    """Generate comprehensive lakehouse dashboard data.

    Args:
        catalog: The Iceberg catalog
        warehouse_path: Path to warehouse directory
        stats_store_path: Optional path to stats cache
        audit_store_path: Optional path to audit log
        queries_store_path: Optional path to queries store

    Returns:
        Dict with storage_path, namespaces, total_tables, total_size_bytes,
        tables (list), recent_activity, saved_queries_count,
        history_entries_count.
    """
    from .catalog import list_tables, list_namespaces, maintenance_status, DEFAULT_WAREHOUSE
    from .stats import get_cached_stats, is_stats_stale
    from .audit import get_audit_log
    from .queries import list_saved_queries, get_history

    storage_path = str(warehouse_path or DEFAULT_WAREHOUSE)

    # Namespaces
    namespaces = list_namespaces(catalog)

    # All tables across namespaces
    all_table_names = list_tables(catalog, namespace="*")

    # Gather per-table info
    tables = []
    total_size = 0

    for tbl_name in all_table_names:
        # Try cached stats first for speed
        cached = get_cached_stats(tbl_name, stats_store_path)
        stale = is_stats_stale(tbl_name, catalog, stats_store_path)

        if cached:
            row_count = cached["row_count"]
            size_bytes = cached["size_bytes"]
            data_files = cached["data_files"]
        else:
            # Fall back to live scan
            try:
                maint = maintenance_status(catalog, tbl_name)
                row_count = None  # maintenance_status doesn't include row count
                size_bytes = maint["total_size_bytes"]
                data_files = maint["data_files"]
            except Exception:
                row_count = None
                size_bytes = 0
                data_files = 0

        # Get orphan info from maintenance
        orphan_files = 0
        try:
            maint = maintenance_status(catalog, tbl_name)
            orphan_files = maint.get("orphan_files", 0)
            # If we didn't have cached stats, use maintenance data
            if row_count is None:
                data_files = maint["data_files"]
                size_bytes = maint["total_size_bytes"]
        except Exception:
            pass

        # If still no row count, try a quick scan
        if row_count is None:
            try:
                table = catalog.load_table(tbl_name)
                arrow_table = table.scan().to_arrow()
                row_count = arrow_table.num_rows
            except Exception:
                row_count = 0

        health = _table_health(data_files, orphan_files, stale)
        total_size += size_bytes

        tables.append({
            "name": tbl_name,
            "rows": row_count,
            "size_bytes": size_bytes,
            "size_display": _format_size(size_bytes),
            "data_files": data_files,
            "health": health,
        })

    # Recent activity from audit log
    recent_activity = get_audit_log(limit=5, store_path=audit_store_path)

    # Saved queries count
    saved_queries = list_saved_queries(store_path=queries_store_path)
    saved_queries_count = len(saved_queries)

    # Query history count
    history = get_history(limit=10000, store_path=queries_store_path)
    history_entries_count = len(history)

    return {
        "storage_path": storage_path,
        "namespaces": namespaces,
        "total_tables": len(all_table_names),
        "total_size_bytes": total_size,
        "total_size_display": _format_size(total_size),
        "tables": tables,
        "recent_activity": recent_activity,
        "saved_queries_count": saved_queries_count,
        "history_entries_count": history_entries_count,
    }
