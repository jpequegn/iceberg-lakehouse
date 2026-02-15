"""Data lineage tracking — table-level dependency graph."""

import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_LINEAGE_PATH = Path.home() / ".lakehouse" / "lineage.json"


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_LINEAGE_PATH
    if not path.exists():
        return {"edges": []}
    try:
        data = json.loads(path.read_text())
        if "edges" not in data:
            data["edges"] = []
        return data
    except (json.JSONDecodeError, KeyError):
        return {"edges": []}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_LINEAGE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize_name(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def record_lineage(
    source_tables: list[str],
    target_table: str,
    operation: str = "manual",
    sql: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Record a lineage edge: source_tables → target_table.

    Args:
        source_tables: List of source table names
        target_table: Target table name
        operation: Operation type (insert_from, view, pipeline, manual)
        sql: Optional SQL that produced this relationship

    Returns:
        Dict with lineage entry details.
    """
    if not source_tables:
        raise ValueError("source_tables must not be empty")
    if not target_table or not target_table.strip():
        raise ValueError("target_table must not be empty")

    sources = sorted(set(_normalize_name(s) for s in source_tables if s.strip()))
    if not sources:
        raise ValueError("source_tables must contain at least one non-empty name")
    target = _normalize_name(target_table)

    store = _load_store(store_path)

    # Check for duplicate edge (same sources + target)
    for edge in store["edges"]:
        if sorted(edge["sources"]) == sources and edge["target"] == target:
            # Update existing edge
            edge["operation"] = operation
            edge["sql"] = sql
            edge["recorded_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            _save_store(store, store_path)
            return {
                "sources": sources,
                "target": target,
                "operation": operation,
                "sql": sql,
                "recorded_at": edge["recorded_at"],
                "message": f"Updated lineage: {sources} → {target}",
            }

    edge = {
        "sources": sources,
        "target": target,
        "operation": operation,
        "sql": sql,
        "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    store["edges"].append(edge)
    _save_store(store, store_path)

    return {
        "sources": sources,
        "target": target,
        "operation": operation,
        "sql": sql,
        "recorded_at": edge["recorded_at"],
        "message": f"Recorded lineage: {sources} → {target}",
    }


def get_upstream(
    table_name: str,
    store_path: Optional[Path] = None,
    transitive: bool = True,
) -> list[dict]:
    """Get all tables that feed into this table.

    Args:
        table_name: Table to find upstream dependencies for
        transitive: If True, include indirect dependencies (default)

    Returns:
        List of dicts with source info and depth.
    """
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)

    if not transitive:
        results = []
        for edge in store["edges"]:
            if edge["target"] == table_name:
                for src in edge["sources"]:
                    results.append({
                        "table": src,
                        "operation": edge["operation"],
                        "depth": 1,
                    })
        return sorted(results, key=lambda r: r["table"])

    # BFS for transitive closure with cycle detection
    visited = set()
    queue = [(table_name, 0)]
    results = []

    while queue:
        current, depth = queue.pop(0)
        for edge in store["edges"]:
            if edge["target"] == current:
                for src in edge["sources"]:
                    if src not in visited:
                        visited.add(src)
                        results.append({
                            "table": src,
                            "operation": edge["operation"],
                            "depth": depth + 1,
                        })
                        queue.append((src, depth + 1))

    return sorted(results, key=lambda r: (r["depth"], r["table"]))


def get_downstream(
    table_name: str,
    store_path: Optional[Path] = None,
    transitive: bool = True,
) -> list[dict]:
    """Get all tables that depend on this table.

    Args:
        table_name: Table to find downstream dependents for
        transitive: If True, include indirect dependents (default)

    Returns:
        List of dicts with target info and depth.
    """
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)

    if not transitive:
        results = []
        for edge in store["edges"]:
            if table_name in edge["sources"]:
                results.append({
                    "table": edge["target"],
                    "operation": edge["operation"],
                    "depth": 1,
                })
        return sorted(results, key=lambda r: r["table"])

    # BFS for transitive closure with cycle detection
    visited = set()
    queue = [(table_name, 0)]
    results = []

    while queue:
        current, depth = queue.pop(0)
        for edge in store["edges"]:
            if current in edge["sources"]:
                target = edge["target"]
                if target not in visited:
                    visited.add(target)
                    results.append({
                        "table": target,
                        "operation": edge["operation"],
                        "depth": depth + 1,
                    })
                    queue.append((target, depth + 1))

    return sorted(results, key=lambda r: (r["depth"], r["table"]))


def get_lineage_graph(
    store_path: Optional[Path] = None,
) -> dict:
    """Get the full lineage graph.

    Returns:
        Dict with nodes (set of all tables) and edges list.
    """
    store = _load_store(store_path)
    nodes = set()
    edges = []

    for edge in store["edges"]:
        for src in edge["sources"]:
            nodes.add(src)
        nodes.add(edge["target"])
        edges.append({
            "sources": edge["sources"],
            "target": edge["target"],
            "operation": edge["operation"],
        })

    return {
        "nodes": sorted(nodes),
        "edges": edges,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


def remove_lineage(
    source_table: str,
    target_table: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a lineage edge between source and target.

    Removes any edge where source_table is in the sources list
    and the target matches target_table.
    """
    source = _normalize_name(source_table)
    target = _normalize_name(target_table)
    store = _load_store(store_path)

    original_count = len(store["edges"])
    store["edges"] = [
        e for e in store["edges"]
        if not (source in e["sources"] and e["target"] == target)
    ]
    removed = original_count - len(store["edges"])

    _save_store(store, store_path)

    if removed == 0:
        return {"message": f"No lineage edge found from {source} to {target}", "removed": 0}
    return {"message": f"Removed {removed} lineage edge(s) from {source} to {target}", "removed": removed}


def get_impact_analysis(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Analyze the impact of dropping or modifying a table.

    Returns downstream dependencies and their depths.
    """
    table_name = _normalize_name(table_name)
    downstream = get_downstream(table_name, store_path=store_path, transitive=True)

    affected = [d["table"] for d in downstream]

    return {
        "table": table_name,
        "affected_tables": affected,
        "affected_count": len(affected),
        "details": downstream,
        "message": (
            f"Dropping {table_name} would affect {len(affected)} table(s)"
            if affected
            else f"No downstream dependencies for {table_name}"
        ),
    }
