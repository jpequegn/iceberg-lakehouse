"""Query result caching — cache expensive queries with TTL and invalidation."""

import datetime
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Optional

DEFAULT_CACHE_META_PATH = Path.home() / ".lakehouse" / "query_cache.json"
MAX_CACHE_ENTRIES = 100

# In-memory result store
_result_cache: dict[str, dict] = {}

# Stats counters
_stats = {"hits": 0, "misses": 0}


def _normalize_sql(sql: str) -> str:
    """Normalize SQL for cache key: uppercase, collapse whitespace, strip semicolons."""
    sql = sql.strip().rstrip(";")
    sql = re.sub(r"\s+", " ", sql)
    return sql.upper()


def _cache_key(sql: str) -> str:
    """Generate cache key from normalized SQL."""
    return hashlib.sha256(_normalize_sql(sql).encode()).hexdigest()[:16]


def _normalize_table(table_name: str) -> str:
    if "." not in table_name:
        return f"default.{table_name}"
    return table_name


def _load_meta(meta_path: Optional[Path] = None) -> dict:
    path = meta_path or DEFAULT_CACHE_META_PATH
    if not path.exists():
        return {"entries": {}, "policies": {}}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {"entries": {}, "policies": {}}


def _save_meta(data: dict, meta_path: Optional[Path] = None) -> None:
    path = meta_path or DEFAULT_CACHE_META_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _extract_tables(sql: str) -> list[str]:
    """Extract table names from SQL (simple heuristic, reuses optimizer logic)."""
    tables = []
    from_match = re.findall(r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)", sql, re.IGNORECASE)
    tables.extend(from_match)
    join_match = re.findall(r"\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)", sql, re.IGNORECASE)
    tables.extend(join_match)
    return [_normalize_table(t.lower()) for t in tables]


def cache_query(
    sql: str,
    result: list[dict],
    ttl_seconds: int = 300,
    meta_path: Optional[Path] = None,
) -> dict:
    """Cache a query result with TTL."""
    key = _cache_key(sql)
    now = time.time()
    tables = _extract_tables(sql)

    # Store result in memory
    _result_cache[key] = {
        "result": result,
        "cached_at": now,
        "expires_at": now + ttl_seconds,
    }

    # Store metadata
    meta = _load_meta(meta_path)
    meta.setdefault("entries", {})[key] = {
        "sql": sql.strip(),
        "normalized": _normalize_sql(sql),
        "tables": tables,
        "cached_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ttl_seconds": ttl_seconds,
        "expires_at": (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=ttl_seconds)).isoformat(),
        "hit_count": 0,
        "row_count": len(result),
    }

    # Evict oldest if over max
    entries = meta["entries"]
    if len(entries) > MAX_CACHE_ENTRIES:
        oldest_key = min(entries, key=lambda k: entries[k].get("cached_at", ""))
        entries.pop(oldest_key, None)
        _result_cache.pop(oldest_key, None)

    _save_meta(meta, meta_path)

    return {
        "cache_key": key,
        "sql": sql.strip(),
        "ttl_seconds": ttl_seconds,
        "tables": tables,
        "message": f"Cached query result ({len(result)} rows, TTL {ttl_seconds}s)",
    }


def get_cached(
    sql: str,
    catalog=None,
    meta_path: Optional[Path] = None,
) -> Optional[list[dict]]:
    """Get cached result if valid. Returns None on miss."""
    global _stats
    key = _cache_key(sql)

    # Check in-memory cache
    entry = _result_cache.get(key)
    if entry is None:
        _stats["misses"] += 1
        return None

    now = time.time()

    # Check TTL expiration
    if now > entry["expires_at"]:
        _result_cache.pop(key, None)
        meta = _load_meta(meta_path)
        meta.get("entries", {}).pop(key, None)
        _save_meta(meta, meta_path)
        _stats["misses"] += 1
        return None

    # Check per-table cache policy
    meta = _load_meta(meta_path)
    meta_entry = meta.get("entries", {}).get(key)
    if meta_entry:
        tables = meta_entry.get("tables", [])
        policies = meta.get("policies", {})
        for table in tables:
            policy = policies.get(table, {})
            if not policy.get("enabled", True):
                _stats["misses"] += 1
                return None

    # Cache hit
    _stats["hits"] += 1

    # Update hit count in metadata
    if meta_entry:
        meta_entry["hit_count"] = meta_entry.get("hit_count", 0) + 1
        _save_meta(meta, meta_path)

    return entry["result"]


def invalidate(
    table_name: Optional[str] = None,
    meta_path: Optional[Path] = None,
) -> dict:
    """Invalidate cache entries for a table, or all if no table specified."""
    meta = _load_meta(meta_path)
    entries = meta.get("entries", {})

    if table_name is None:
        # Clear all
        count = len(entries)
        meta["entries"] = {}
        _result_cache.clear()
        _save_meta(meta, meta_path)
        return {"invalidated": count, "message": f"Cleared all {count} cache entries"}

    table_name = _normalize_table(table_name)
    to_remove = []
    for key, entry in entries.items():
        if table_name in entry.get("tables", []):
            to_remove.append(key)

    for key in to_remove:
        entries.pop(key, None)
        _result_cache.pop(key, None)

    _save_meta(meta, meta_path)
    return {
        "table": table_name,
        "invalidated": len(to_remove),
        "message": f"Invalidated {len(to_remove)} cache entries for '{table_name}'",
    }


def get_cache_stats(meta_path: Optional[Path] = None) -> dict:
    """Get cache statistics."""
    meta = _load_meta(meta_path)
    entries = meta.get("entries", {})
    total = _stats["hits"] + _stats["misses"]
    hit_rate = (_stats["hits"] / total * 100) if total > 0 else 0.0

    return {
        "total_entries": len(entries),
        "in_memory_entries": len(_result_cache),
        "hits": _stats["hits"],
        "misses": _stats["misses"],
        "hit_rate": round(hit_rate, 2),
        "message": f"Cache: {len(entries)} entries, {_stats['hits']} hits, {_stats['misses']} misses ({hit_rate:.1f}% hit rate)",
    }


def list_cached_queries(
    limit: int = 20,
    meta_path: Optional[Path] = None,
) -> list[dict]:
    """List cached queries with TTL remaining and hit count."""
    meta = _load_meta(meta_path)
    entries = meta.get("entries", {})
    now = datetime.datetime.now(datetime.timezone.utc)

    result = []
    for key, entry in entries.items():
        try:
            expires = datetime.datetime.fromisoformat(entry["expires_at"])
            ttl_remaining = max(0, (expires - now).total_seconds())
        except (KeyError, ValueError):
            ttl_remaining = 0

        result.append({
            "cache_key": key,
            "sql": entry.get("sql", "")[:100],
            "tables": entry.get("tables", []),
            "row_count": entry.get("row_count", 0),
            "hit_count": entry.get("hit_count", 0),
            "ttl_remaining_seconds": int(ttl_remaining),
            "cached_at": entry.get("cached_at", ""),
        })

    # Sort by most recent
    result.sort(key=lambda x: x["cached_at"], reverse=True)
    return result[:limit]


def set_cache_policy(
    table_name: str,
    ttl_seconds: Optional[int] = None,
    enabled: bool = True,
    meta_path: Optional[Path] = None,
) -> dict:
    """Set per-table cache policy."""
    table_name = _normalize_table(table_name)
    meta = _load_meta(meta_path)
    policies = meta.setdefault("policies", {})

    policy = {"enabled": enabled}
    if ttl_seconds is not None:
        policy["ttl_seconds"] = ttl_seconds

    policies[table_name] = policy
    _save_meta(meta, meta_path)

    status = "enabled" if enabled else "disabled"
    ttl_msg = f" (TTL: {ttl_seconds}s)" if ttl_seconds is not None else ""
    return {
        "table": table_name,
        "policy": policy,
        "message": f"Cache policy for '{table_name}': {status}{ttl_msg}",
    }


def reset_stats():
    """Reset hit/miss counters (for testing)."""
    global _stats
    _stats = {"hits": 0, "misses": 0}
    _result_cache.clear()
