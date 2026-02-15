"""Tests for data sampling."""

import pytest

from lakehouse.sampling import (
    random_sample,
    stratified_sample,
    systematic_sample,
    sample_to_table,
    get_sample_stats,
)
from lakehouse.catalog import create_table, insert_rows


@pytest.fixture
def sample_table(test_catalog):
    """Create a table with enough rows for sampling."""
    create_table(test_catalog, "sample_src", columns={"id": "long", "category": "string", "value": "double"})
    rows = []
    for i in range(100):
        cat = ["A", "B", "C"][i % 3]
        rows.append({"id": i, "category": cat, "value": float(i * 10)})
    insert_rows(test_catalog, "default.sample_src", rows)
    return test_catalog


@pytest.fixture
def small_table(test_catalog):
    """Create a small table for edge cases."""
    create_table(test_catalog, "small_tbl", columns={"id": "long", "name": "string"})
    insert_rows(test_catalog, "default.small_tbl", [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ])
    return test_catalog


# --- random_sample ---


class TestRandomSample:
    def test_approximate_fraction(self, sample_table):
        result = random_sample(sample_table, "sample_src", fraction=0.5, seed=42)
        # With 100 rows and 0.5 fraction, expect roughly 50 (allow wide margin for randomness)
        assert 20 <= result["sample_size"] <= 80
        assert result["total_rows"] == 100

    def test_seed_reproducible(self, sample_table):
        r1 = random_sample(sample_table, "sample_src", fraction=0.3, seed=42)
        r2 = random_sample(sample_table, "sample_src", fraction=0.3, seed=42)
        assert r1["sample_size"] == r2["sample_size"]

    def test_fraction_one_returns_all(self, sample_table):
        result = random_sample(sample_table, "sample_src", fraction=1.0, seed=42)
        assert result["sample_size"] == 100

    def test_empty_table(self, test_catalog):
        create_table(test_catalog, "empty_sample", columns={"id": "long"})
        result = random_sample(test_catalog, "empty_sample")
        assert result["sample_size"] == 0

    def test_small_table(self, small_table):
        result = random_sample(small_table, "small_tbl", fraction=0.5, seed=1)
        assert result["sample_size"] <= 3

    def test_with_limit(self, sample_table):
        result = random_sample(sample_table, "sample_src", fraction=1.0, limit=5)
        assert result["sample_size"] <= 5


# --- stratified_sample ---


class TestStratifiedSample:
    def test_maintains_distribution(self, sample_table):
        result = stratified_sample(sample_table, "sample_src", column="category", fraction=0.3, seed=42)
        # Should have rows from all 3 categories
        categories = set(r["category"] for r in result["rows"])
        assert categories == {"A", "B", "C"}
        assert "A" in result["strata"]
        assert "B" in result["strata"]
        assert "C" in result["strata"]

    def test_strata_proportional(self, sample_table):
        result = stratified_sample(sample_table, "sample_src", column="category", fraction=0.3, seed=42)
        # Each category has ~33 rows, 30% = ~10 per stratum
        for val in result["strata"].values():
            assert val["sampled"] >= 1
            assert val["sampled"] <= val["total"]

    def test_empty_table(self, test_catalog):
        create_table(test_catalog, "empty_strat", columns={"id": "long", "cat": "string"})
        result = stratified_sample(test_catalog, "empty_strat", column="cat")
        assert result["sample_size"] == 0


# --- systematic_sample ---


class TestSystematicSample:
    def test_every_10th(self, sample_table):
        result = systematic_sample(sample_table, "sample_src", every_nth=10)
        assert result["sample_size"] == 10  # 100 rows / 10 = 10

    def test_every_5th(self, sample_table):
        result = systematic_sample(sample_table, "sample_src", every_nth=5)
        assert result["sample_size"] == 20  # 100 rows / 5 = 20

    def test_empty_table(self, test_catalog):
        create_table(test_catalog, "empty_sys", columns={"id": "long"})
        result = systematic_sample(test_catalog, "empty_sys")
        assert result["sample_size"] == 0


# --- sample_to_table ---


class TestSampleToTable:
    def test_materialize_random(self, sample_table):
        result = sample_to_table(sample_table, "sample_src", "random_copy", method="random", fraction=0.5, seed=42)
        assert result["rows_sampled"] > 0
        assert result["sample_table"] == "default.random_copy"

        # Verify table exists
        tbl = sample_table.load_table("default.random_copy")
        assert tbl.scan().to_arrow().num_rows == result["rows_sampled"]

    def test_materialize_stratified(self, sample_table):
        result = sample_to_table(
            sample_table, "sample_src", "strat_copy",
            method="stratified", column="category", fraction=0.3, seed=42,
        )
        assert result["rows_sampled"] > 0

    def test_materialize_systematic(self, sample_table):
        result = sample_to_table(
            sample_table, "sample_src", "sys_copy",
            method="systematic", every_nth=10,
        )
        assert result["rows_sampled"] == 10

    def test_invalid_method(self, sample_table):
        with pytest.raises(ValueError, match="Unknown method"):
            sample_to_table(sample_table, "sample_src", "bad", method="invalid")

    def test_stratified_without_column(self, sample_table):
        with pytest.raises(ValueError, match="column"):
            sample_to_table(sample_table, "sample_src", "bad", method="stratified")


# --- get_sample_stats ---


class TestGetSampleStats:
    def test_stats_comparison(self, sample_table):
        sample_to_table(sample_table, "sample_src", "stats_sample", method="systematic", every_nth=5)
        result = get_sample_stats(sample_table, "sample_src", "stats_sample")

        assert result["full_rows"] == 100
        assert result["sample_rows"] == 20
        assert result["coverage"] == 20.0
        assert len(result["column_comparison"]) == 3  # id, category, value

        # Check numeric stats
        numeric_cols = [c for c in result["column_comparison"] if c["type"] == "numeric"]
        assert len(numeric_cols) >= 1  # at least id or value
        for col in numeric_cols:
            assert "mean" in col["full"]
            assert "mean" in col["sample"]
