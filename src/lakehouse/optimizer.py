"""Query optimization advisor — partition, materialization, and cost suggestions."""

import re
from collections import Counter
from pathlib import Path
from typing import Optional


def _normalize(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extract table names from SQL (simple heuristic)."""
    sql_upper = sql.upper()
    tables = []
    # FROM clause
    from_match = re.findall(r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)', sql, re.IGNORECASE)
    tables.extend(from_match)
    # JOIN clause
    join_match = re.findall(r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)', sql, re.IGNORECASE)
    tables.extend(join_match)
    return [t.lower() for t in tables]


def _extract_filters_from_sql(sql: str) -> list[str]:
    """Extract filter column names from WHERE clauses (simple heuristic)."""
    where_match = re.search(r'\bWHERE\b(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|$)', sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return []
    where_clause = where_match.group(1)
    # Extract column names before comparison operators
    columns = re.findall(r'([a-zA-Z_][a-zA-Z0-9_.]*)\s*(?:=|!=|<>|>=|<=|>|<|IN|LIKE|IS)', where_clause, re.IGNORECASE)
    return [c.lower() for c in columns if c.upper() not in ('AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE')]


def _has_aggregation(sql: str) -> bool:
    """Check if query has aggregation."""
    return bool(re.search(r'\b(SUM|AVG|COUNT|MIN|MAX|GROUP\s+BY)\b', sql, re.IGNORECASE))


def _has_join(sql: str) -> bool:
    """Check if query has a JOIN."""
    return bool(re.search(r'\bJOIN\b', sql, re.IGNORECASE))


def analyze_query_patterns(
    limit: int = 100,
    store_path: Optional[Path] = None,
) -> dict:
    """Analyze recent query history to find patterns."""
    from .queries import get_history

    history = get_history(limit=limit, store_path=store_path)

    if not history:
        return {
            "total_queries": 0,
            "frequent_tables": [],
            "frequent_filters": [],
            "slow_queries": [],
            "repeated_queries": [],
            "message": "No query history available",
        }

    # Count table references
    table_counter = Counter()
    filter_counter = Counter()
    sql_counter = Counter()
    durations = []

    for entry in history:
        sql = entry.get("sql", "")
        duration = entry.get("duration_ms", 0)
        durations.append(duration)

        tables = _extract_tables_from_sql(sql)
        for t in tables:
            table_counter[t] += 1

        filters = _extract_filters_from_sql(sql)
        for f in filters:
            filter_counter[f] += 1

        # Normalize SQL for counting repeats (strip whitespace)
        normalized = " ".join(sql.split()).strip().rstrip(";").upper()
        sql_counter[normalized] += 1

    # Frequent tables
    frequent_tables = [
        {"table": t, "count": c}
        for t, c in table_counter.most_common(10)
    ]

    # Frequent filters
    frequent_filters = [
        {"column": f, "count": c}
        for f, c in filter_counter.most_common(10)
    ]

    # Slow queries (above p90)
    if durations:
        sorted_durations = sorted(durations)
        p90_idx = int(len(sorted_durations) * 0.9)
        p90 = sorted_durations[p90_idx] if p90_idx < len(sorted_durations) else sorted_durations[-1]
        slow_queries = [
            {"sql": e["sql"], "duration_ms": e.get("duration_ms", 0), "rows": e.get("rows_returned", 0)}
            for e in history
            if e.get("duration_ms", 0) > p90 and p90 > 0
        ][:10]
    else:
        slow_queries = []

    # Repeated queries
    repeated_queries = [
        {"sql_pattern": sql, "count": c}
        for sql, c in sql_counter.most_common(10)
        if c > 1
    ]

    return {
        "total_queries": len(history),
        "frequent_tables": frequent_tables,
        "frequent_filters": frequent_filters,
        "slow_queries": slow_queries,
        "repeated_queries": repeated_queries,
        "message": f"Analyzed {len(history)} queries: {len(frequent_tables)} tables, {len(repeated_queries)} repeated patterns",
    }


def suggest_partitions(
    catalog,
    table_name: str,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Suggest partitioning strategies based on query patterns and data distribution."""
    from .stats import get_cached_stats

    table_name = _normalize(table_name)
    patterns = analyze_query_patterns(store_path=store_path)

    suggestions = []

    # Find frequently filtered columns for this table
    short_name = table_name.split(".")[-1]
    filter_cols = patterns.get("frequent_filters", [])

    # Get column stats for cardinality analysis
    stats = get_cached_stats(table_name, store_path=store_path)
    if stats is None:
        # Try computing fresh
        try:
            from .stats import compute_table_stats
            stats = compute_table_stats(catalog, table_name)
        except Exception:
            stats = None

    columns = stats.get("columns", {}) if stats else {}
    row_count = stats.get("row_count", 0) if stats else 0

    # Check current partition spec
    try:
        table = catalog.load_table(table_name)
        current_spec = table.spec()
        current_partition_fields = [f.name for f in current_spec.fields] if current_spec and current_spec.fields else []
    except Exception:
        current_partition_fields = []

    for fc in filter_cols:
        col_name = fc["column"]
        if col_name in current_partition_fields:
            continue  # Already partitioned on this column

        if col_name not in columns:
            continue

        col_info = columns[col_name]
        unique_count = col_info.get("unique", 0)

        # Good partition candidate: moderate cardinality (10-1000 unique values)
        if row_count > 0 and 2 <= unique_count <= 1000:
            ratio = unique_count / row_count
            benefit = "high" if ratio < 0.1 else "medium" if ratio < 0.5 else "low"
            suggestions.append({
                "table": table_name,
                "column": col_name,
                "unique_values": unique_count,
                "filter_frequency": fc["count"],
                "benefit": benefit,
                "rationale": f"Column '{col_name}' is filtered {fc['count']} times with {unique_count} unique values ({benefit} partition benefit)",
            })

    return suggestions


def suggest_materializations(
    catalog,
    limit: int = 100,
    store_path: Optional[Path] = None,
) -> list[dict]:
    """Suggest materialized views based on repeated expensive queries."""
    patterns = analyze_query_patterns(limit=limit, store_path=store_path)
    suggestions = []

    for rq in patterns.get("repeated_queries", []):
        sql = rq["sql_pattern"]
        count = rq["count"]

        # Only suggest for aggregation or join queries run 2+ times
        if count >= 2 and (_has_aggregation(sql) or _has_join(sql)):
            tables = _extract_tables_from_sql(sql)
            suggestions.append({
                "sql": sql,
                "run_count": count,
                "has_aggregation": _has_aggregation(sql),
                "has_join": _has_join(sql),
                "tables_involved": tables,
                "rationale": f"Query run {count} times with {'aggregation' if _has_aggregation(sql) else 'join'} — good materialization candidate",
            })

    return suggestions


def get_optimization_report(
    catalog,
    store_path: Optional[Path] = None,
) -> dict:
    """Generate a comprehensive optimization report."""
    from .catalog import list_tables

    patterns = analyze_query_patterns(store_path=store_path)

    all_tables = list_tables(catalog, namespace="*")
    partition_suggestions = []
    for tbl in all_tables:
        suggestions = suggest_partitions(catalog, tbl, store_path=store_path)
        partition_suggestions.extend(suggestions)

    mat_suggestions = suggest_materializations(catalog, store_path=store_path)

    # Compute optimization score
    total_issues = len(partition_suggestions) + len(mat_suggestions) + len(patterns.get("slow_queries", []))
    if total_issues == 0:
        score = 100
    else:
        score = max(0, 100 - total_issues * 10)

    return {
        "query_patterns": patterns,
        "partition_suggestions": partition_suggestions,
        "materialization_suggestions": mat_suggestions,
        "slow_queries": patterns.get("slow_queries", []),
        "optimization_score": score,
        "total_suggestions": len(partition_suggestions) + len(mat_suggestions),
        "message": f"Optimization report: score {score}/100, {len(partition_suggestions)} partition and {len(mat_suggestions)} materialization suggestions",
    }


def estimate_query_cost(
    catalog,
    sql: str,
    store_path: Optional[Path] = None,
) -> dict:
    """Estimate the cost of a query based on table sizes."""
    from .stats import get_cached_stats, compute_table_stats

    tables = _extract_tables_from_sql(sql)
    has_filter = bool(_extract_filters_from_sql(sql))
    has_join_flag = _has_join(sql)
    has_agg = _has_aggregation(sql)

    total_rows = 0
    table_details = []

    for tbl in tables:
        tbl_name = _normalize(tbl)
        stats = get_cached_stats(tbl_name, store_path=store_path)
        if stats is None:
            try:
                stats = compute_table_stats(catalog, tbl_name)
            except Exception:
                stats = None

        rows = stats.get("row_count", 0) if stats else 0
        size = stats.get("total_size_bytes", 0) if stats else 0
        total_rows += rows
        table_details.append({
            "table": tbl_name,
            "estimated_rows": rows,
            "size_bytes": size,
        })

    # Estimate rows scanned (filters reduce, joins may increase)
    if has_filter:
        estimated_scan = int(total_rows * 0.3)  # Assume filter reduces to ~30%
    else:
        estimated_scan = total_rows

    complexity = "simple"
    if has_join_flag and has_agg:
        complexity = "complex"
    elif has_join_flag or has_agg:
        complexity = "moderate"

    return {
        "sql": sql,
        "tables_involved": table_details,
        "estimated_rows_scanned": estimated_scan,
        "total_source_rows": total_rows,
        "has_filter": has_filter,
        "has_join": has_join_flag,
        "has_aggregation": has_agg,
        "complexity": complexity,
        "message": f"Query cost estimate: ~{estimated_scan:,} rows scanned across {len(tables)} table(s) ({complexity})",
    }
