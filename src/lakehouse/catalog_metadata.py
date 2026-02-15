"""Data catalog enrichment — column descriptions, glossary, and data classification."""

import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_CATALOG_META_PATH = Path.home() / ".lakehouse" / "catalog_metadata.json"

VALID_CLASSIFICATIONS = {"pii", "financial", "public", "internal", "confidential"}


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_CATALOG_META_PATH
    if not path.exists():
        return {"column_descriptions": {}, "classifications": {}, "glossary": {}}
    try:
        data = json.loads(path.read_text())
        data.setdefault("column_descriptions", {})
        data.setdefault("classifications", {})
        data.setdefault("glossary", {})
        return data
    except (json.JSONDecodeError, KeyError):
        return {"column_descriptions": {}, "classifications": {}, "glossary": {}}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_CATALOG_META_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


# --- Column Descriptions ---


def set_column_description(
    table_name: str,
    column_name: str,
    description: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Set a description for a table column."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store["column_descriptions"]:
        store["column_descriptions"][table_name] = {}
    store["column_descriptions"][table_name][column_name] = description
    _save_store(store, store_path)

    return {
        "table": table_name,
        "column": column_name,
        "description": description,
        "message": f"Description set for '{table_name}.{column_name}'",
    }


def get_column_descriptions(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get all column descriptions for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    descriptions = store["column_descriptions"].get(table_name, {})
    return {
        "table": table_name,
        "descriptions": descriptions,
        "message": f"{len(descriptions)} description(s) for '{table_name}'",
    }


# --- Classifications ---


def classify_column(
    table_name: str,
    column_name: str,
    classification: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Set data classification for a column."""
    if classification not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Invalid classification '{classification}'. "
            f"Must be one of: {', '.join(sorted(VALID_CLASSIFICATIONS))}"
        )

    table_name = _normalize(table_name)
    store = _load_store(store_path)

    if table_name not in store["classifications"]:
        store["classifications"][table_name] = {}
    store["classifications"][table_name][column_name] = classification
    _save_store(store, store_path)

    return {
        "table": table_name,
        "column": column_name,
        "classification": classification,
        "message": f"Column '{column_name}' classified as '{classification}'",
    }


def get_classifications(
    table_name: Optional[str] = None,
    classification: Optional[str] = None,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Get classifications, optionally filtered by table or classification type."""
    store = _load_store(store_path)
    results = []

    tables = store["classifications"]
    if table_name:
        table_name = _normalize(table_name)
        tables = {table_name: tables.get(table_name, {})}

    for tbl, cols in tables.items():
        for col, cls in cols.items():
            if classification and cls != classification:
                continue
            results.append({
                "table": tbl,
                "column": col,
                "classification": cls,
            })

    return results


# --- Glossary ---


def add_glossary_term(
    term: str,
    definition: str,
    aliases: Optional[list[str]] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Add a business glossary term."""
    store = _load_store(store_path)
    store["glossary"][term] = {
        "definition": definition,
        "aliases": aliases or [],
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    _save_store(store, store_path)

    return {
        "term": term,
        "definition": definition,
        "aliases": aliases or [],
        "message": f"Glossary term '{term}' added",
    }


def search_glossary(
    query: str,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Search glossary terms by name, alias, or definition text (case-insensitive)."""
    store = _load_store(store_path)
    query_lower = query.lower()
    results = []

    for term, info in store["glossary"].items():
        matches = False
        if query_lower in term.lower():
            matches = True
        elif query_lower in info["definition"].lower():
            matches = True
        elif any(query_lower in a.lower() for a in info.get("aliases", [])):
            matches = True

        if matches:
            results.append({
                "term": term,
                "definition": info["definition"],
                "aliases": info.get("aliases", []),
            })

    return results


def list_glossary(store_path: Optional[Path] = None) -> list[dict]:
    """List all glossary terms."""
    store = _load_store(store_path)
    return [
        {
            "term": term,
            "definition": info["definition"],
            "aliases": info.get("aliases", []),
        }
        for term, info in store["glossary"].items()
    ]


def remove_glossary_term(
    term: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove a glossary term."""
    store = _load_store(store_path)
    if term in store["glossary"]:
        del store["glossary"][term]
        _save_store(store, store_path)
        return {"term": term, "message": f"Glossary term '{term}' removed"}
    return {"term": term, "message": f"Glossary term '{term}' not found"}


# --- Enriched Schema ---


def get_enriched_schema(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get table schema enriched with descriptions, classifications, and glossary matches."""
    table_name = _normalize(table_name)

    try:
        table = catalog.load_table(table_name)
    except Exception as e:
        raise ValueError(f"Table '{table_name}' not found: {e}")

    schema = table.schema()
    store = _load_store(store_path)

    descriptions = store["column_descriptions"].get(table_name, {})
    classifications = store["classifications"].get(table_name, {})

    fields = []
    for field in schema.fields:
        # Check for glossary matches in the column name
        glossary_matches = []
        for term, info in store["glossary"].items():
            term_lower = term.lower()
            if term_lower in field.name.lower():
                glossary_matches.append(term)
            elif any(a.lower() in field.name.lower() for a in info.get("aliases", [])):
                glossary_matches.append(term)

        fields.append({
            "field_id": field.field_id,
            "name": field.name,
            "type": str(field.field_type),
            "required": field.required,
            "description": descriptions.get(field.name),
            "classification": classifications.get(field.name),
            "glossary_matches": glossary_matches,
        })

    return {
        "table": table_name,
        "fields": fields,
        "total_fields": len(fields),
        "described_fields": sum(1 for f in fields if f["description"]),
        "classified_fields": sum(1 for f in fields if f["classification"]),
        "message": f"Enriched schema for '{table_name}': {len(fields)} fields",
    }
