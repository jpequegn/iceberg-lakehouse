"""Tests for query result caching."""

import time
import pytest

from lakehouse.query_cache import (
    cache_query,
    get_cached,
    invalidate,
    get_cache_stats,
    list_cached_queries,
    set_cache_policy,
    reset_stats,
)


@pytest.fixture(autouse=True)
def clean_cache(tmp_path):
    """Reset in-memory cache and stats before each test."""
    reset_stats()
    return tmp_path / "query_cache.json"


@pytest.fixture
def meta(clean_cache):
    return clean_cache


# --- cache_query / get_cached ---


class TestCacheHitMiss:
    def test_cache_hit(self, meta):
        sql = "SELECT * FROM test_table"
        rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        cache_query(sql, rows, ttl_seconds=60, meta_path=meta)

        result = get_cached(sql, meta_path=meta)
        assert result is not None
        assert len(result) == 2
        assert result[0]["id"] == 1

    def test_cache_miss(self, meta):
        result = get_cached("SELECT * FROM unknown", meta_path=meta)
        assert result is None

    def test_ttl_expiration(self, meta):
        sql = "SELECT * FROM ttl_test"
        cache_query(sql, [{"id": 1}], ttl_seconds=1, meta_path=meta)
        time.sleep(1.1)
        result = get_cached(sql, meta_path=meta)
        assert result is None

    def test_normalized_sql_matching(self, meta):
        """Whitespace and case differences should still hit cache."""
        sql1 = "SELECT * FROM  test_table"
        sql2 = "select *  from test_table"
        cache_query(sql1, [{"id": 1}], ttl_seconds=60, meta_path=meta)
        result = get_cached(sql2, meta_path=meta)
        assert result is not None

    def test_semicolons_stripped(self, meta):
        sql1 = "SELECT * FROM tbl;"
        sql2 = "SELECT * FROM tbl"
        cache_query(sql1, [{"id": 1}], ttl_seconds=60, meta_path=meta)
        result = get_cached(sql2, meta_path=meta)
        assert result is not None


# --- invalidate ---


class TestInvalidation:
    def test_invalidate_by_table(self, meta):
        cache_query("SELECT * FROM t1", [{"a": 1}], meta_path=meta)
        cache_query("SELECT * FROM t2", [{"b": 2}], meta_path=meta)

        result = invalidate("t1", meta_path=meta)
        assert result["invalidated"] == 1

        assert get_cached("SELECT * FROM t1", meta_path=meta) is None
        assert get_cached("SELECT * FROM t2", meta_path=meta) is not None

    def test_invalidate_all(self, meta):
        cache_query("SELECT * FROM t1", [{"a": 1}], meta_path=meta)
        cache_query("SELECT * FROM t2", [{"b": 2}], meta_path=meta)

        result = invalidate(meta_path=meta)
        assert result["invalidated"] == 2

        assert get_cached("SELECT * FROM t1", meta_path=meta) is None
        assert get_cached("SELECT * FROM t2", meta_path=meta) is None


# --- get_cache_stats ---


class TestCacheStats:
    def test_stats_hits_misses(self, meta):
        cache_query("SELECT * FROM tbl", [{"id": 1}], meta_path=meta)
        get_cached("SELECT * FROM tbl", meta_path=meta)  # hit
        get_cached("SELECT * FROM tbl", meta_path=meta)  # hit
        get_cached("SELECT * FROM unknown", meta_path=meta)  # miss

        stats = get_cache_stats(meta_path=meta)
        assert stats["hits"] == 2
        assert stats["misses"] == 1
        assert stats["hit_rate"] == pytest.approx(66.67, abs=0.1)
        assert stats["total_entries"] == 1

    def test_stats_empty(self, meta):
        stats = get_cache_stats(meta_path=meta)
        assert stats["total_entries"] == 0
        assert stats["hits"] == 0
        assert stats["misses"] == 0


# --- list_cached_queries ---


class TestListCachedQueries:
    def test_list_queries(self, meta):
        cache_query("SELECT * FROM t1", [{"a": 1}], meta_path=meta)
        cache_query("SELECT * FROM t2", [{"b": 2}], meta_path=meta)

        entries = list_cached_queries(meta_path=meta)
        assert len(entries) == 2
        sqls = [e["sql"] for e in entries]
        assert any("t1" in s for s in sqls)
        assert any("t2" in s for s in sqls)

    def test_list_respects_limit(self, meta):
        for i in range(10):
            cache_query(f"SELECT * FROM t{i}", [{"id": i}], meta_path=meta)
        entries = list_cached_queries(limit=3, meta_path=meta)
        assert len(entries) == 3

    def test_list_shows_hit_count(self, meta):
        cache_query("SELECT * FROM tbl", [{"id": 1}], meta_path=meta)
        get_cached("SELECT * FROM tbl", meta_path=meta)
        get_cached("SELECT * FROM tbl", meta_path=meta)

        entries = list_cached_queries(meta_path=meta)
        assert entries[0]["hit_count"] == 2


# --- set_cache_policy ---


class TestCachePolicy:
    def test_disable_cache_for_table(self, meta):
        cache_query("SELECT * FROM tbl", [{"id": 1}], meta_path=meta)
        set_cache_policy("tbl", enabled=False, meta_path=meta)

        result = get_cached("SELECT * FROM tbl", meta_path=meta)
        assert result is None

    def test_enable_cache_for_table(self, meta):
        set_cache_policy("tbl", enabled=True, ttl_seconds=120, meta_path=meta)
        cache_query("SELECT * FROM tbl", [{"id": 1}], meta_path=meta)
        result = get_cached("SELECT * FROM tbl", meta_path=meta)
        assert result is not None

    def test_custom_ttl_policy(self, meta):
        result = set_cache_policy("tbl", ttl_seconds=600, meta_path=meta)
        assert result["policy"]["ttl_seconds"] == 600
        assert result["policy"]["enabled"] is True


# --- Multi-table queries ---


class TestMultiTable:
    def test_multi_table_extraction(self, meta):
        sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
        cache_query(sql, [{"id": 1}], meta_path=meta)

        entries = list_cached_queries(meta_path=meta)
        assert "default.t1" in entries[0]["tables"]
        assert "default.t2" in entries[0]["tables"]

    def test_invalidate_one_table_clears_join(self, meta):
        sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
        cache_query(sql, [{"id": 1}], meta_path=meta)

        invalidate("t1", meta_path=meta)
        result = get_cached(sql, meta_path=meta)
        assert result is None
