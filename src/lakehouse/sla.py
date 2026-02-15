"""Table SLA monitoring — freshness and quality thresholds with alerts."""

import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_SLA_PATH = Path.home() / ".lakehouse" / "slas.json"
MAX_HISTORY = 50


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_SLA_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_SLA_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def set_sla(
    table_name: str,
    sla: dict,
    store_path: Optional[Path] = None,
) -> dict:
    """Set SLA thresholds for a table."""
    if not table_name or not table_name.strip():
        raise ValueError("Table name cannot be empty")

    table_name = _normalize(table_name)
    store = _load_store(store_path)

    max_stale = sla.get("max_staleness_hours")
    min_quality = sla.get("min_quality_score")
    min_rows = sla.get("min_row_count")
    max_null = sla.get("max_null_pct")

    if max_stale is not None and (not isinstance(max_stale, (int, float)) or max_stale <= 0):
        raise ValueError("max_staleness_hours must be a positive number")
    if min_quality is not None and (not isinstance(min_quality, (int, float)) or not 0 <= min_quality <= 100):
        raise ValueError("min_quality_score must be between 0 and 100")
    if min_rows is not None and (not isinstance(min_rows, int) or min_rows < 0):
        raise ValueError("min_row_count must be a non-negative integer")
    if max_null is not None and (not isinstance(max_null, (int, float)) or not 0 <= max_null <= 100):
        raise ValueError("max_null_pct must be between 0 and 100")

    existing = store.get(table_name, {})
    store[table_name] = {
        "max_staleness_hours": max_stale,
        "min_quality_score": min_quality,
        "min_row_count": min_rows,
        "max_null_pct": max_null,
        "created_at": existing.get("created_at", datetime.datetime.now(datetime.timezone.utc).isoformat()),
        "check_history": existing.get("check_history", []),
    }
    _save_store(store, store_path)

    return {
        "table": table_name,
        "sla": {k: v for k, v in store[table_name].items() if k not in ("check_history",)},
        "message": f"SLA set for '{table_name}'",
    }


def get_sla(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Get SLA for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    sla = store.get(table_name)
    if sla is None:
        return {"table": table_name, "sla": None, "message": f"No SLA for '{table_name}'"}
    return {
        "table": table_name,
        "sla": {k: v for k, v in sla.items() if k != "check_history"},
        "message": f"SLA for '{table_name}'",
    }


def list_slas(store_path: Optional[Path] = None) -> list[dict]:
    """List all SLA definitions."""
    store = _load_store(store_path)
    return [
        {"table": table_name, **{k: v for k, v in sla.items() if k != "check_history"}}
        for table_name, sla in store.items()
    ]


def remove_sla(
    table_name: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Remove SLA for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    if table_name in store:
        del store[table_name]
        _save_store(store, store_path)
        return {"table": table_name, "message": f"SLA removed for '{table_name}'"}
    return {"table": table_name, "message": f"No SLA found for '{table_name}'"}


def check_sla(
    catalog,
    table_name: Optional[str] = None,
    store_path: Optional[Path] = None,
    quality_path: Optional[Path] = None,
    stats_path: Optional[Path] = None,
    validation_path: Optional[Path] = None,
) -> dict:
    """Check SLA compliance for one or all tables."""
    from .stats import compute_table_stats
    from .quality import compute_quality_score

    store = _load_store(store_path)

    if table_name:
        table_name = _normalize(table_name)
        tables = {table_name: store.get(table_name)} if table_name in store else {}
    else:
        tables = dict(store)

    results = []

    for tbl, sla in tables.items():
        if sla is None:
            continue

        violations = []
        warnings = []
        recommendations = []

        try:
            stats = compute_table_stats(catalog, tbl, store_path=stats_path)
        except Exception as e:
            results.append({
                "table": tbl,
                "status": "violation",
                "violations": [f"Cannot load table stats: {e}"],
                "warnings": [],
                "recommendations": ["Verify table exists and is accessible"],
            })
            continue

        # Freshness check
        max_stale = sla.get("max_staleness_hours")
        if max_stale is not None:
            last_modified = stats.get("last_modified")
            if last_modified:
                try:
                    mod_time = datetime.datetime.fromisoformat(last_modified)
                    now = datetime.datetime.now(datetime.timezone.utc)
                    age_hours = (now - mod_time).total_seconds() / 3600
                    if age_hours > max_stale:
                        violations.append(
                            f"Staleness: {age_hours:.1f}h exceeds max {max_stale}h"
                        )
                        recommendations.append("Refresh data or run ETL pipeline")
                    elif age_hours > max_stale * 0.9:
                        warnings.append(
                            f"Staleness: {age_hours:.1f}h approaching max {max_stale}h"
                        )
                except (ValueError, TypeError):
                    warnings.append("Could not parse last_modified timestamp")
            else:
                violations.append("No last_modified timestamp — table may never have been written to")
                recommendations.append("Insert data into the table")

        # Quality check
        min_quality = sla.get("min_quality_score")
        if min_quality is not None:
            try:
                quality = compute_quality_score(
                    catalog, tbl,
                    stats_path=stats_path,
                    validation_path=validation_path,
                    store_path=quality_path,
                )
                score = quality["overall_score"]
                if score < min_quality:
                    violations.append(
                        f"Quality: {score}/100 below minimum {min_quality}"
                    )
                    recommendations.append("Review data quality and fix issues")
                elif score < min_quality * 1.1:
                    warnings.append(
                        f"Quality: {score}/100 approaching minimum {min_quality}"
                    )
            except Exception:
                warnings.append("Could not compute quality score")

        # Row count check
        min_rows = sla.get("min_row_count")
        if min_rows is not None:
            row_count = stats.get("row_count", 0)
            if row_count < min_rows:
                violations.append(
                    f"Row count: {row_count} below minimum {min_rows}"
                )
                recommendations.append("Check data ingestion pipeline")
            elif row_count < min_rows * 1.1:
                warnings.append(
                    f"Row count: {row_count} approaching minimum {min_rows}"
                )

        # Null percentage check
        max_null = sla.get("max_null_pct")
        if max_null is not None:
            row_count = stats.get("row_count", 0)
            columns = stats.get("columns", {})
            if row_count > 0:
                for col_name, col_info in columns.items():
                    nulls = col_info.get("nulls", 0)
                    null_pct = (nulls / row_count) * 100
                    if null_pct > max_null:
                        violations.append(
                            f"Null %: column '{col_name}' has {null_pct:.1f}% nulls (max {max_null}%)"
                        )
                        recommendations.append(f"Fix null values in column '{col_name}'")
                    elif null_pct > max_null * 0.9:
                        warnings.append(
                            f"Null %: column '{col_name}' at {null_pct:.1f}% approaching max {max_null}%"
                        )

        # Determine status
        if violations:
            status = "violation"
        elif warnings:
            status = "warning"
        else:
            status = "passing"

        check_entry = {
            "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "status": status,
            "violations": violations,
            "warnings": warnings,
        }

        # Save to history
        sla.setdefault("check_history", [])
        sla["check_history"].append(check_entry)
        sla["check_history"] = sla["check_history"][-MAX_HISTORY:]
        store[tbl] = sla
        _save_store(store, store_path)

        results.append({
            "table": tbl,
            "status": status,
            "violations": violations,
            "warnings": warnings,
            "recommendations": list(set(recommendations)),
        })

    passing = sum(1 for r in results if r["status"] == "passing")
    violating = sum(1 for r in results if r["status"] == "violation")
    warning_count = sum(1 for r in results if r["status"] == "warning")

    return {
        "tables": results,
        "total": len(results),
        "passing": passing,
        "warnings": warning_count,
        "violations": violating,
        "message": f"SLA check: {passing} passing, {warning_count} warnings, {violating} violations",
    }


def get_sla_history(
    table_name: str,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Get SLA check history for a table."""
    table_name = _normalize(table_name)
    store = _load_store(store_path)
    sla = store.get(table_name, {})
    return sla.get("check_history", [])
