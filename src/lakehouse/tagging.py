"""Table bookmarks, tags, and descriptions for catalog enrichment."""

import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_METADATA_PATH = Path.home() / ".lakehouse" / "table_metadata.json"


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_METADATA_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_METADATA_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize_name(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def _get_entry(store: dict, table_name: str) -> dict:
    return store.get(table_name, {"tags": [], "description": "", "bookmarked": False})


# --- Tags ---


def tag_table(
    table_name: str,
    tags: list[str],
    store_path: Optional[Path] = None,
) -> dict:
    """Add tags to a table.

    Args:
        table_name: Table name (with or without namespace)
        tags: List of tag strings

    Returns:
        Dict with table name and updated tags.
    """
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)
    entry = _get_entry(store, table_name)

    normalized_tags = [t.strip().lower() for t in tags if t.strip()]
    existing = set(entry.get("tags", []))
    existing.update(normalized_tags)
    entry["tags"] = sorted(existing)
    entry["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    store[table_name] = entry
    _save_store(store, store_path)

    return {"table": table_name, "tags": entry["tags"], "message": f"Tagged {table_name} with {normalized_tags}"}


def untag_table(
    table_name: str,
    tags: list[str],
    store_path: Optional[Path] = None,
) -> dict:
    """Remove tags from a table."""
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)
    entry = _get_entry(store, table_name)

    to_remove = {t.strip().lower() for t in tags if t.strip()}
    entry["tags"] = sorted(set(entry.get("tags", [])) - to_remove)
    entry["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    store[table_name] = entry
    _save_store(store, store_path)

    return {"table": table_name, "tags": entry["tags"], "message": f"Removed tags {sorted(to_remove)} from {table_name}"}


def get_tags(
    table_name: str,
    store_path: Optional[Path] = None,
) -> list[str]:
    """Get all tags for a table."""
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)
    entry = _get_entry(store, table_name)
    return entry.get("tags", [])


def search_by_tag(
    tag: str,
    store_path: Optional[Path] = None,
) -> list[str]:
    """Find all tables with a given tag."""
    tag = tag.strip().lower()
    store = _load_store(store_path)
    return sorted(
        name for name, entry in store.items()
        if tag in entry.get("tags", [])
    )


# --- Descriptions ---


def set_table_description(
    table_name: str,
    description: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Set a human-readable description for a table."""
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)
    entry = _get_entry(store, table_name)

    entry["description"] = description
    entry["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    store[table_name] = entry
    _save_store(store, store_path)

    return {"table": table_name, "description": description, "message": f"Description set for {table_name}"}


def get_table_description(
    table_name: str,
    store_path: Optional[Path] = None,
) -> str:
    """Get the description for a table."""
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)
    entry = _get_entry(store, table_name)
    return entry.get("description", "")


# --- Bookmarks ---


def bookmark_table(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Bookmark a table for quick access."""
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)
    entry = _get_entry(store, table_name)

    entry["bookmarked"] = True
    entry["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    store[table_name] = entry
    _save_store(store, store_path)

    return {"table": table_name, "message": f"Bookmarked {table_name}"}


def unbookmark_table(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a bookmark."""
    table_name = _normalize_name(table_name)
    store = _load_store(store_path)
    entry = _get_entry(store, table_name)

    entry["bookmarked"] = False
    entry["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    store[table_name] = entry
    _save_store(store, store_path)

    return {"table": table_name, "message": f"Unbookmarked {table_name}"}


def list_bookmarks(
    store_path: Optional[Path] = None,
) -> list[str]:
    """List all bookmarked tables."""
    store = _load_store(store_path)
    return sorted(
        name for name, entry in store.items()
        if entry.get("bookmarked", False)
    )


# --- Search ---


def search_tables(
    query: str,
    catalog=None,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Search tables by name, tag, or description.

    Args:
        query: Search string (matched against name, tags, description)
        catalog: Optional Iceberg catalog (to include tables without metadata)
        store_path: Optional path to metadata store

    Returns:
        List of dicts with table name, tags, description, bookmarked, match_type.
    """
    query_lower = query.strip().lower()
    store = _load_store(store_path)
    results = []
    seen = set()

    # Search metadata store
    for name, entry in store.items():
        match_types = []
        if query_lower in name.lower():
            match_types.append("name")
        if query_lower in entry.get("description", "").lower():
            match_types.append("description")
        if query_lower in entry.get("tags", []):
            match_types.append("tag")

        if match_types:
            results.append({
                "table": name,
                "tags": entry.get("tags", []),
                "description": entry.get("description", ""),
                "bookmarked": entry.get("bookmarked", False),
                "match_type": match_types,
            })
            seen.add(name)

    # Also search catalog table names if provided
    if catalog is not None:
        from .catalog import list_tables
        all_tables = list_tables(catalog, namespace="*")
        for tbl in all_tables:
            if tbl not in seen and query_lower in tbl.lower():
                results.append({
                    "table": tbl,
                    "tags": [],
                    "description": "",
                    "bookmarked": False,
                    "match_type": ["name"],
                })

    return results
