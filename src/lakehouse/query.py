"""DuckDB query execution with Iceberg integration."""

from typing import Optional
from pathlib import Path

import duckdb
import pandas as pd
from pyiceberg.catalog import Catalog

from .catalog import get_catalog, DEFAULT_WAREHOUSE


class QueryEngine:
    """Execute SQL queries against Iceberg tables using DuckDB."""

    def __init__(
        self,
        catalog: Optional[Catalog] = None,
        warehouse_path: Optional[Path] = None,
    ):
        self.catalog = catalog or get_catalog()
        self.warehouse = warehouse_path or DEFAULT_WAREHOUSE
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection with Iceberg extension."""
        if self._conn is None:
            self._conn = duckdb.connect(":memory:")
            self._conn.execute("INSTALL iceberg; LOAD iceberg;")
            self._register_tables()
        return self._conn

    def _register_tables(self) -> None:
        """Register all Iceberg tables as DuckDB views."""
        conn = self._conn
        if conn is None:
            return

        # List all tables from catalog
        for namespace in self.catalog.list_namespaces():
            ns_name = namespace[0] if isinstance(namespace, tuple) else namespace
            for table_id in self.catalog.list_tables(ns_name):
                table_name = table_id[1] if isinstance(table_id, tuple) else str(table_id)
                full_name = f"{ns_name}.{table_name}"

                try:
                    # Load table via PyIceberg and register with DuckDB
                    table = self.catalog.load_table(full_name)
                    arrow_table = table.scan().to_arrow()

                    # Register as view (use just table name for simpler queries)
                    conn.register(table_name, arrow_table)

                except Exception as e:
                    # Skip tables that can't be loaded (empty, etc.)
                    print(f"Warning: Could not register table {full_name}: {e}")

    def refresh(self) -> None:
        """Refresh table registrations (call after data changes)."""
        if self._conn:
            self._conn.close()
        self._conn = None

    def execute(
        self,
        sql: str,
        max_rows: int = 1000,
    ) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        conn = self._get_connection()

        # Add LIMIT if not present and query is a SELECT
        sql_upper = sql.strip().upper()
        if sql_upper.startswith("SELECT") and "LIMIT" not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {max_rows}"

        result = conn.execute(sql)
        return result.fetchdf()

    def execute_as_of(
        self,
        sql: str,
        table_name: str,
        as_of: str,
        max_rows: int = 1000,
    ) -> pd.DataFrame:
        """Execute SQL query against a historical snapshot of a table.

        Loads the table at the given snapshot/timestamp and registers it
        temporarily for the query, then restores the current version.
        """
        from .catalog import get_catalog, scan_as_of

        catalog = get_catalog()
        arrow_table = scan_as_of(catalog, table_name, as_of)

        # Use a temporary connection to avoid polluting the main one
        conn = duckdb.connect(":memory:")
        short_name = table_name.split(".")[-1] if "." in table_name else table_name
        conn.register(short_name, arrow_table)

        sql_upper = sql.strip().upper()
        if sql_upper.startswith("SELECT") and "LIMIT" not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {max_rows}"

        result = conn.execute(sql).fetchdf()
        conn.close()
        return result

    def execute_raw(self, sql: str) -> duckdb.DuckDBPyRelation:
        """Execute SQL and return raw DuckDB relation."""
        conn = self._get_connection()
        return conn.execute(sql)

    def get_schema(self, table_name: str) -> pd.DataFrame:
        """Get schema for a table."""
        conn = self._get_connection()
        return conn.execute(f"DESCRIBE {table_name}").fetchdf()

    def list_registered_tables(self) -> list[str]:
        """List all registered tables/views."""
        conn = self._get_connection()
        result = conn.execute("SHOW TABLES").fetchdf()
        return result["name"].tolist() if "name" in result.columns else []


def execute_query(
    sql: str,
    max_rows: int = 1000,
    catalog: Optional[Catalog] = None,
) -> pd.DataFrame:
    """Convenience function to execute a query."""
    engine = QueryEngine(catalog=catalog)
    return engine.execute(sql, max_rows=max_rows)
