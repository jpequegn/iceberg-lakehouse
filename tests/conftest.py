"""Shared test fixtures for lakehouse tests."""

import pytest
from pathlib import Path

from lakehouse.catalog import (
    get_catalog,
    init_catalog,
    create_sample_tables,
)
from lakehouse.query import QueryEngine


@pytest.fixture
def test_catalog(tmp_path):
    """Create isolated catalog for testing.
    
    Creates a fresh catalog in a temporary directory with sample tables.
    """
    catalog = get_catalog(
        warehouse_path=tmp_path / "warehouse",
        catalog_db=tmp_path / "catalog.db",
    )
    init_catalog(catalog)
    create_sample_tables(catalog)
    return catalog


@pytest.fixture
def query_engine(test_catalog):
    """Create query engine for test catalog."""
    return QueryEngine(catalog=test_catalog)
