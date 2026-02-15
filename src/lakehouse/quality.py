"""Data quality scoring and anomaly detection."""

import datetime
import json
from pathlib import Path
from typing import Optional

DEFAULT_QUALITY_PATH = Path.home() / ".lakehouse" / "quality.json"
MAX_HISTORY = 50


def _load_store(store_path: Optional[Path] = None) -> dict:
    path = store_path or DEFAULT_QUALITY_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_store(data: dict, store_path: Optional[Path] = None) -> None:
    path = store_path or DEFAULT_QUALITY_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def compute_quality_score(
    catalog,
    table_name: str,
    stats_path: Optional[Path] = None,
    validation_path: Optional[Path] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Compute a data quality score for a table.

    Score components (weighted):
        - completeness (30%): % of non-null values across columns
        - uniqueness (25%): average uniqueness ratio across columns
        - freshness (20%): penalized if last modified > 1 hour ago
        - rule_compliance (25%): % of validation rules passing

    Returns:
        Dict with overall_score (0-100), component scores, and details.
    """
    from .stats import compute_table_stats
    from .validation import list_validation_rules, validate_rows

    if "." not in table_name:
        table_name = f"default.{table_name}"

    # Compute fresh stats
    stats = compute_table_stats(catalog, table_name, store_path=stats_path)
    row_count = stats["row_count"]
    columns = stats.get("columns", {})

    # --- Completeness (30%) ---
    if row_count > 0 and columns:
        total_cells = row_count * len(columns)
        null_cells = sum(c.get("nulls", 0) for c in columns.values())
        completeness = round(((total_cells - null_cells) / total_cells) * 100, 1)
    else:
        completeness = 100.0 if row_count == 0 else 0.0

    # --- Uniqueness (25%) ---
    if row_count > 0 and columns:
        uniqueness_ratios = []
        for col_info in columns.values():
            unique = col_info.get("unique", 0)
            if row_count > 0:
                uniqueness_ratios.append(unique / row_count)
        uniqueness = round((sum(uniqueness_ratios) / len(uniqueness_ratios)) * 100, 1) if uniqueness_ratios else 100.0
    else:
        uniqueness = 100.0

    # --- Freshness (20%) ---
    last_modified = stats.get("last_modified")
    if last_modified:
        try:
            mod_time = datetime.datetime.fromisoformat(last_modified)
            now = datetime.datetime.now(datetime.timezone.utc)
            age_hours = (now - mod_time).total_seconds() / 3600
            if age_hours <= 1:
                freshness = 100.0
            elif age_hours <= 24:
                freshness = round(max(0, 100 - (age_hours - 1) * (50 / 23)), 1)
            else:
                freshness = round(max(0, 50 - (age_hours - 24) * (50 / 168)), 1)
        except (ValueError, TypeError):
            freshness = 50.0
    else:
        freshness = 0.0

    # --- Rule compliance (25%) ---
    rules = list_validation_rules(table_name, store_path=validation_path)
    if rules and row_count > 0:
        # Load table data for validation
        try:
            table = catalog.load_table(table_name)
            arrow = table.scan().to_arrow()
            rows = arrow.to_pydict()
            row_list = [dict(zip(rows.keys(), vals)) for vals in zip(*rows.values())]
            result = validate_rows(row_list, rules)
            total_checks = len(rules) * len(row_list)
            failures = len(result["failures"])
            rule_compliance = round(((total_checks - failures) / total_checks) * 100, 1) if total_checks > 0 else 100.0
        except Exception:
            rule_compliance = 50.0
    else:
        rule_compliance = 100.0  # No rules = compliant

    # --- Overall score ---
    overall = round(
        completeness * 0.30
        + uniqueness * 0.25
        + freshness * 0.20
        + rule_compliance * 0.25,
        1,
    )

    # --- Recommendations ---
    recommendations = []
    if completeness < 80:
        null_cols = [name for name, info in columns.items() if info.get("nulls", 0) > row_count * 0.1]
        if null_cols:
            recommendations.append(f"Add NOT NULL constraints to columns with many nulls: {', '.join(null_cols[:3])}")
    if uniqueness < 50:
        recommendations.append("Review columns for unexpected duplicates")
    if freshness < 50:
        recommendations.append("Table data may be stale — consider refreshing source data")
    if rule_compliance < 80:
        recommendations.append("Review failing validation rules and fix data quality issues")

    score_entry = {
        "overall_score": overall,
        "completeness": completeness,
        "uniqueness": uniqueness,
        "freshness": freshness,
        "rule_compliance": rule_compliance,
        "row_count": row_count,
        "column_count": len(columns),
        "recommendations": recommendations,
        "computed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    # Save to history
    store = _load_store(store_path)
    if table_name not in store:
        store[table_name] = {"history": []}
    store[table_name]["history"].append(score_entry)
    # Keep only last N entries
    store[table_name]["history"] = store[table_name]["history"][-MAX_HISTORY:]
    _save_store(store, store_path)

    return {
        "table": table_name,
        **score_entry,
        "message": f"Quality score for '{table_name}': {overall}/100",
    }


def detect_anomalies(
    catalog,
    table_name: str,
    stats_path: Optional[Path] = None,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Detect anomalies by comparing current data with cached stats.

    Checks for:
        - Row count change > 50%
        - NULL count spike in any column
        - Numeric column min/max drift

    Returns:
        List of anomaly dicts with column, type, severity, description.
    """
    from .stats import get_cached_stats, compute_table_stats

    if "." not in table_name:
        table_name = f"default.{table_name}"

    # Get cached (old) stats
    old_stats = get_cached_stats(table_name, store_path=stats_path)

    # Compute fresh stats
    new_stats = compute_table_stats(catalog, table_name, store_path=stats_path)

    anomalies = []

    if old_stats is None:
        return anomalies  # No baseline to compare

    old_rows = old_stats.get("row_count", 0)
    new_rows = new_stats.get("row_count", 0)

    # Row count anomaly
    if old_rows > 0:
        change_pct = abs(new_rows - old_rows) / old_rows * 100
        if change_pct > 50:
            severity = "critical" if change_pct > 90 else "warning"
            direction = "increase" if new_rows > old_rows else "decrease"
            anomalies.append({
                "type": "row_count_change",
                "column": None,
                "severity": severity,
                "description": f"Row count {direction}: {old_rows} → {new_rows} ({change_pct:.0f}% change)",
                "old_value": old_rows,
                "new_value": new_rows,
            })

    # Column-level anomalies
    old_columns = old_stats.get("columns", {})
    new_columns = new_stats.get("columns", {})

    for col_name, new_info in new_columns.items():
        old_info = old_columns.get(col_name)
        if old_info is None:
            continue

        # NULL spike
        old_nulls = old_info.get("nulls", 0)
        new_nulls = new_info.get("nulls", 0)
        if new_rows > 0 and old_rows > 0:
            old_null_pct = old_nulls / old_rows * 100
            new_null_pct = new_nulls / new_rows * 100
            if new_null_pct - old_null_pct > 10:
                severity = "critical" if new_null_pct - old_null_pct > 30 else "warning"
                anomalies.append({
                    "type": "null_spike",
                    "column": col_name,
                    "severity": severity,
                    "description": f"NULL rate for '{col_name}' increased: {old_null_pct:.1f}% → {new_null_pct:.1f}%",
                    "old_value": old_null_pct,
                    "new_value": new_null_pct,
                })

        # Numeric drift
        if "min" in old_info and "min" in new_info:
            old_min = old_info.get("min")
            new_min = new_info.get("min")
            old_max = old_info.get("max")
            new_max = new_info.get("max")

            if old_min is not None and new_min is not None:
                try:
                    if float(new_min) < float(old_min) * 0.5 and float(old_min) != 0:
                        anomalies.append({
                            "type": "min_drift",
                            "column": col_name,
                            "severity": "warning",
                            "description": f"Min value for '{col_name}' dropped: {old_min} → {new_min}",
                            "old_value": old_min,
                            "new_value": new_min,
                        })
                except (TypeError, ValueError):
                    pass

            if old_max is not None and new_max is not None:
                try:
                    if float(new_max) > float(old_max) * 2 and float(old_max) != 0:
                        anomalies.append({
                            "type": "max_drift",
                            "column": col_name,
                            "severity": "warning",
                            "description": f"Max value for '{col_name}' spiked: {old_max} → {new_max}",
                            "old_value": old_max,
                            "new_value": new_max,
                        })
                except (TypeError, ValueError):
                    pass

    return anomalies


def get_quality_report(
    catalog,
    table_name: Optional[str] = None,
    stats_path: Optional[Path] = None,
    validation_path: Optional[Path] = None,
    store_path: Optional[Path] = None,
) -> dict:
    """Generate a quality report for one or all tables."""
    from .catalog import list_tables

    if table_name:
        tables = [table_name if "." in table_name else f"default.{table_name}"]
    else:
        tables = list_tables(catalog, namespace="*")

    report = {
        "tables": [],
        "total_tables": len(tables),
        "average_score": 0,
    }

    scores = []
    for tbl in tables:
        try:
            score = compute_quality_score(
                catalog, tbl,
                stats_path=stats_path,
                validation_path=validation_path,
                store_path=store_path,
            )
            anomalies = detect_anomalies(catalog, tbl, stats_path=stats_path)
            report["tables"].append({
                "table": tbl,
                "overall_score": score["overall_score"],
                "completeness": score["completeness"],
                "uniqueness": score["uniqueness"],
                "freshness": score["freshness"],
                "rule_compliance": score["rule_compliance"],
                "anomalies": len(anomalies),
                "anomaly_details": anomalies,
                "recommendations": score["recommendations"],
            })
            scores.append(score["overall_score"])
        except Exception:
            report["tables"].append({
                "table": tbl,
                "overall_score": None,
                "error": "Could not compute score",
            })

    report["average_score"] = round(sum(scores) / len(scores), 1) if scores else 0

    return report


def get_quality_history(
    table_name: str,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Get historical quality scores for a table."""
    if "." not in table_name:
        table_name = f"default.{table_name}"

    store = _load_store(store_path)
    entry = store.get(table_name, {})
    return entry.get("history", [])
