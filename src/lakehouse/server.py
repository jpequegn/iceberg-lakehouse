"""MCP Server for Iceberg Lakehouse."""

import asyncio
import json
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool,
    TextContent,
    INVALID_PARAMS,
    INTERNAL_ERROR,
)

from .catalog import get_catalog, list_tables, get_table_schema, insert_rows, update_rows, delete_rows, upsert_rows, alter_table, get_snapshots, snapshot_diff, rollback_table, expire_snapshots, execute_batch, get_table_property, set_table_property, import_file, export_table, profile_table, compact_table, maintenance_status, cleanup_orphans, create_table, get_partitions, get_partition_stats, list_namespaces, create_namespace, drop_namespace, get_namespace_properties
from .query import QueryEngine
from .queries import save_query, list_saved_queries, get_saved_query, delete_saved_query, add_history_entry, get_history, clear_history
from .validation import add_validation_rule, list_validation_rules, remove_validation_rule, validate_rows
from .audit import get_audit_log, clear_audit_log
from .stats import compute_table_stats, get_cached_stats, get_all_cached_stats, refresh_stats, is_stats_stale
from .dashboard import get_dashboard
from .maintenance import set_maintenance_policy, get_maintenance_policy, remove_maintenance_policy, run_maintenance, check_maintenance_needed
from .views import create_view, list_views, get_view, drop_view, query_view
from .tagging import tag_table, untag_table, get_tags, search_by_tag, set_table_description, get_table_description, bookmark_table, unbookmark_table, list_bookmarks, search_tables
from .lineage import record_lineage, get_upstream, get_downstream, get_lineage_graph, remove_lineage, get_impact_analysis
from .cloning import clone_table, list_clones, promote_clone, discard_clone
from .joins import execute_join, join_to_table, suggest_joins
from .matviews import create_materialized_view, refresh_materialized_view, list_materialized_views, drop_materialized_view, query_materialized_view, check_materialized_view_freshness


# Initialize server
server = Server("lakehouse")

# Global query engine (initialized on first use)
_engine: QueryEngine | None = None


def get_engine() -> QueryEngine:
    """Get or create the query engine."""
    global _engine
    if _engine is None:
        catalog = get_catalog()
        _engine = QueryEngine(catalog=catalog)
    return _engine


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="query",
            description=(
                "Execute a SQL query against the lakehouse. "
                "Available tables: expenses, health, notes. "
                "Use standard SQL syntax. Results limited to 1000 rows by default. "
                "Supports time travel: provide as_of with an ISO timestamp or snapshot ID "
                "and table_name to query historical data."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query to execute",
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Maximum rows to return (default: 1000)",
                        "default": 1000,
                    },
                    "as_of": {
                        "type": "string",
                        "description": "ISO timestamp or snapshot ID for time travel queries (e.g., '2025-12-01T00:00:00')",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Table name for time travel queries (required when as_of is set)",
                    },
                },
                "required": ["sql"],
            },
        ),
        Tool(
            name="list_snapshots",
            description=(
                "List all available snapshots for a table, showing snapshot IDs, "
                "timestamps, and operations. Use snapshot IDs with the query tool's "
                "as_of parameter for time travel queries."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="snapshot_diff",
            description=(
                "Compare two snapshots of a table to see what rows were added or deleted. "
                "Provide from_snapshot (older) and optionally to_snapshot (newer, defaults to current). "
                "Use snapshot IDs or ISO timestamps. Use list_snapshots to find available snapshots."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                    "from_snapshot": {
                        "type": "string",
                        "description": "Older snapshot ID or ISO timestamp",
                    },
                    "to_snapshot": {
                        "type": "string",
                        "description": "Newer snapshot ID or ISO timestamp (default: current)",
                    },
                },
                "required": ["table_name", "from_snapshot"],
            },
        ),
        Tool(
            name="list_tables",
            description="List all available tables in the lakehouse.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="describe_table",
            description="Get schema and metadata for a specific table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to describe",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="refresh",
            description="Refresh table data after changes. Call this if you've added new data.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="insert",
            description=(
                "Insert rows into an Iceberg table. "
                "Provide table name and an array of row objects. "
                "Each row object should have keys matching the table's column names. "
                "Missing optional fields will be set to null."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to insert into (e.g., 'expenses' or 'default.expenses')",
                    },
                    "rows": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Array of row objects to insert",
                    },
                },
                "required": ["table_name", "rows"],
            },
        ),
        Tool(
            name="update",
            description=(
                "Update rows in an Iceberg table that match a filter condition. "
                "Provide table name, a SQL WHERE clause filter, and column updates. "
                "Example filter: \"category = 'groceries'\" or \"id = 5\". "
                "Example updates: {\"amount\": 100.0, \"currency\": \"EUR\"}"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to update (e.g., 'expenses')",
                    },
                    "filter": {
                        "type": "string",
                        "description": "SQL WHERE clause to identify rows to update (e.g., \"id = 5\")",
                    },
                    "updates": {
                        "type": "object",
                        "description": "Object with column names as keys and new values as values",
                    },
                },
                "required": ["table_name", "filter", "updates"],
            },
        ),
        Tool(
            name="delete",
            description=(
                "Delete rows from an Iceberg table that match a filter condition. "
                "Provide table name and a SQL WHERE clause filter. "
                "A filter is always required to prevent accidental full table deletes. "
                "Example filter: \"category = 'groceries'\" or \"id = 5\"."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to delete from (e.g., 'expenses')",
                    },
                    "filter": {
                        "type": "string",
                        "description": "SQL WHERE clause to identify rows to delete (e.g., \"id = 5\")",
                    },
                },
                "required": ["table_name", "filter"],
            },
        ),
        Tool(
            name="upsert",
            description=(
                "Upsert (insert or update) rows in an Iceberg table. "
                "Matches rows on key columns - updates existing rows if keys match, "
                "inserts new rows if no match found. Useful for data synchronization "
                "and idempotent data loading. "
                "Example: upsert into expenses with key_columns=['id'], "
                "rows=[{'id': 1, 'amount': 90.0}]"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                    "key_columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Columns to match on for determining insert vs update (e.g., ['id'])",
                    },
                    "rows": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Array of row objects to upsert",
                    },
                },
                "required": ["table_name", "key_columns", "rows"],
            },
        ),
        Tool(
            name="alter_table",
            description=(
                "Alter a table's schema: add, drop, or rename columns. "
                "Iceberg schema evolution is safe - no data rewrite required. "
                "Supported operations: add_column, drop_column, rename_column. "
                "For add_column, provide column_type (string, long, double, date, timestamp). "
                "For rename_column, provide new_name."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                    "operation": {
                        "type": "string",
                        "enum": ["add_column", "drop_column", "rename_column"],
                        "description": "Schema operation to perform",
                    },
                    "column_name": {
                        "type": "string",
                        "description": "Target column name",
                    },
                    "new_name": {
                        "type": "string",
                        "description": "New column name (required for rename_column)",
                    },
                    "column_type": {
                        "type": "string",
                        "description": "Column type for add_column (string, long, double, date, timestamp)",
                    },
                },
                "required": ["table_name", "operation", "column_name"],
            },
        ),
        Tool(
            name="rollback",
            description=(
                "Rollback a table to a previous snapshot. "
                "Provide either a snapshot_id or a timestamp (ISO format). "
                "Use list_snapshots to find available snapshot IDs. "
                "This restores the table data to the state at that snapshot."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                    "snapshot_id": {
                        "type": "integer",
                        "description": "Snapshot ID to rollback to",
                    },
                    "timestamp": {
                        "type": "string",
                        "description": "ISO timestamp to rollback to (e.g., '2025-12-01T00:00:00')",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="expire_snapshots",
            description=(
                "Expire old snapshots to clean up storage. "
                "Provide older_than (ISO timestamp or duration like '30d', '24h') "
                "and/or retain_last (minimum snapshots to keep)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                    "older_than": {
                        "type": "string",
                        "description": "Expire snapshots older than this (ISO timestamp or duration like '30d')",
                    },
                    "retain_last": {
                        "type": "integer",
                        "description": "Minimum number of recent snapshots to keep",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="batch",
            description=(
                "Execute multiple write operations as a batch. "
                "Operations run sequentially and stop on first failure. "
                "Each operation needs: action (insert/update/delete), table_name, "
                "and action-specific fields (rows for insert, filter+updates for update, "
                "filter for delete). Note: cross-table atomicity is best-effort."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "operations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "action": {
                                    "type": "string",
                                    "enum": ["insert", "update", "delete"],
                                    "description": "The write operation to perform",
                                },
                                "table_name": {
                                    "type": "string",
                                    "description": "Target table name",
                                },
                                "rows": {
                                    "type": "array",
                                    "items": {"type": "object"},
                                    "description": "Rows to insert (for insert action)",
                                },
                                "filter": {
                                    "type": "string",
                                    "description": "SQL WHERE clause (for update/delete actions)",
                                },
                                "updates": {
                                    "type": "object",
                                    "description": "Column updates (for update action)",
                                },
                            },
                            "required": ["action", "table_name"],
                        },
                        "description": "Array of operations to execute",
                    },
                },
                "required": ["operations"],
            },
        ),
        Tool(
            name="query_vortex",
            description=(
                "Execute a SQL query directly against a Vortex file. "
                "Uses the DuckDB Vortex extension if available, otherwise "
                "falls back to Arrow bridge. The file is registered as a "
                "table with the given name (default: 'data')."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query to execute (use the table_name in FROM clause)",
                    },
                    "vortex_path": {
                        "type": "string",
                        "description": "Path to the Vortex file",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Table name for the Vortex file in SQL (default: 'data')",
                        "default": "data",
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Maximum rows to return (default: 1000)",
                        "default": 1000,
                    },
                },
                "required": ["sql", "vortex_path"],
            },
        ),
        Tool(
            name="convert_format",
            description=(
                "Convert an Iceberg table's data to Vortex format for faster reads. "
                "Exports the current table data as a .vortex file. "
                "The Iceberg table itself remains in Parquet format."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to export (e.g., 'expenses')",
                    },
                    "output_dir": {
                        "type": "string",
                        "description": "Directory to write the Vortex file (default: current directory)",
                        "default": ".",
                    },
                    "compact": {
                        "type": "boolean",
                        "description": "Optimize for smaller file size over read speed",
                        "default": False,
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="get_format_config",
            description=(
                "Get the configured file format for a table or the global default. "
                "Shows the effective format considering config overrides and table properties."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table name to get format for (optional, returns global default if omitted)",
                    },
                },
            },
        ),
        Tool(
            name="set_format_config",
            description=(
                "Set the file format for a table or the global default. "
                "Valid formats: 'parquet', 'vortex'. "
                "Use table_name to set per-table format, or omit for global default."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "format": {
                        "type": "string",
                        "enum": ["parquet", "vortex"],
                        "description": "File format to set",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Table name for per-table override (optional, sets global default if omitted)",
                    },
                },
                "required": ["format"],
            },
        ),
        Tool(
            name="set_table_property",
            description=(
                "Set a property on an Iceberg table. "
                "Common properties: 'write.format.default' (parquet/vortex). "
                "Properties are stored in the Iceberg metadata."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table",
                    },
                    "key": {
                        "type": "string",
                        "description": "Property key (e.g., 'write.format.default')",
                    },
                    "value": {
                        "type": "string",
                        "description": "Property value",
                    },
                },
                "required": ["table_name", "key", "value"],
            },
        ),
        Tool(
            name="import_file",
            description=(
                "Import data from a CSV or JSON file into an Iceberg table. "
                "Auto-detects format from file extension (.csv, .json, .ndjson, .jsonl). "
                "Creates a new table if it doesn't exist. "
                "Use if_exists to control behavior when the table already exists: "
                "'fail' (default), 'append', or 'replace'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the file to import (CSV, JSON, or NDJSON)",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Target table name (e.g., 'expenses')",
                    },
                    "format": {
                        "type": "string",
                        "enum": ["csv", "json", "ndjson"],
                        "description": "File format (auto-detected from extension if omitted)",
                    },
                    "if_exists": {
                        "type": "string",
                        "enum": ["fail", "append", "replace"],
                        "description": "What to do if table exists: fail (default), append, replace",
                        "default": "fail",
                    },
                    "delimiter": {
                        "type": "string",
                        "description": "CSV delimiter character (default: ',')",
                        "default": ",",
                    },
                    "has_header": {
                        "type": "boolean",
                        "description": "Whether CSV has a header row (default: true)",
                        "default": True,
                    },
                },
                "required": ["file_path", "table_name"],
            },
        ),
        Tool(
            name="export_table",
            description=(
                "Export an Iceberg table to CSV, JSON, NDJSON, or Parquet format. "
                "Supports filtering with a WHERE clause, column selection, "
                "and row limits. Auto-detects format from output path extension."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to export (e.g., 'expenses')",
                    },
                    "format": {
                        "type": "string",
                        "enum": ["csv", "json", "ndjson", "parquet"],
                        "description": "Output format (auto-detected from output_path extension if omitted)",
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Output file path (default: <table>.<format>)",
                    },
                    "where": {
                        "type": "string",
                        "description": "SQL WHERE clause for filtering (e.g., \"amount > 100\")",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column names to include (default: all columns)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to export",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="profile_table",
            description=(
                "Profile an Iceberg table's data: row count, null counts, "
                "unique values, min/max, mean/stddev for numeric columns, "
                "and top values for string columns."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to profile (e.g., 'expenses')",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column names to profile (default: all columns)",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="compact_table",
            description=(
                "Compact a table by rewriting many small data files into fewer large files. "
                "Improves query performance after many INSERT/UPDATE/DELETE operations. "
                "After compaction, run expire_snapshots and cleanup_orphans to free disk space."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to compact (e.g., 'expenses')",
                    },
                    "target_size_mb": {
                        "type": "integer",
                        "description": "Target file size in MB (default: 128)",
                        "default": 128,
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="maintenance_status",
            description=(
                "Show maintenance status for a table: data file count, sizes, "
                "snapshot count, and orphan file count. Use this to determine "
                "if compaction or cleanup is needed."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="cleanup_orphans",
            description=(
                "Clean up orphan data files not referenced by any snapshot. "
                "Orphans accumulate after compaction and snapshot expiration. "
                "Use dry_run=true to preview without deleting."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, report orphans without deleting (default: true)",
                        "default": True,
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="create_table",
            description=(
                "Create a new Iceberg table with columns and optional partitioning. "
                "Columns are specified as a dict mapping names to types "
                "(string, long, int, double, float, date, timestamp, boolean). "
                "Partitions use transform syntax: identity(col), year(col), month(col), "
                "day(col), hour(col), bucket(n, col), truncate(n, col)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the new table (e.g., 'events')",
                    },
                    "columns": {
                        "type": "object",
                        "description": "Column definitions as {name: type} (e.g., {\"id\": \"long\", \"name\": \"string\"})",
                    },
                    "partitions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Partition transforms (e.g., ['month(event_date)', 'identity(category)'])",
                    },
                },
                "required": ["table_name", "columns"],
            },
        ),
        Tool(
            name="get_partitions",
            description=(
                "Get the partition spec for a table, showing partition fields, "
                "source columns, and transforms."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="get_partition_stats",
            description=(
                "Get partition data distribution for a table, showing per-partition "
                "file counts and sizes. Useful for identifying data skew."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'expenses')",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="list_namespaces",
            description="List all namespaces in the catalog.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="create_namespace",
            description=(
                "Create a new namespace for organizing tables. "
                "Optionally set properties like owner or description."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "namespace": {
                        "type": "string",
                        "description": "Name of the namespace to create (e.g., 'staging')",
                    },
                    "properties": {
                        "type": "object",
                        "description": "Optional properties (e.g., {\"owner\": \"data-team\"})",
                    },
                },
                "required": ["namespace"],
            },
        ),
        Tool(
            name="drop_namespace",
            description=(
                "Drop an empty namespace. The namespace must not contain any tables."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "namespace": {
                        "type": "string",
                        "description": "Name of the namespace to drop (e.g., 'staging')",
                    },
                },
                "required": ["namespace"],
            },
        ),
        Tool(
            name="get_namespace_properties",
            description="Get properties for a namespace.",
            inputSchema={
                "type": "object",
                "properties": {
                    "namespace": {
                        "type": "string",
                        "description": "Name of the namespace",
                    },
                },
                "required": ["namespace"],
            },
        ),
        Tool(
            name="save_query",
            description=(
                "Save a named query for later use. "
                "Queries can be recalled and executed by name."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Unique name for the query (e.g., 'monthly_totals')",
                    },
                    "sql": {
                        "type": "string",
                        "description": "SQL query to save",
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional description of what the query does",
                    },
                },
                "required": ["name", "sql"],
            },
        ),
        Tool(
            name="list_saved_queries",
            description="List all saved queries with their names, SQL, and descriptions.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="run_saved_query",
            description=(
                "Run a previously saved query by name. "
                "Returns the query results as a markdown table."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the saved query to run",
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Maximum rows to return (default: 1000)",
                        "default": 1000,
                    },
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="delete_saved_query",
            description="Delete a saved query by name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the saved query to delete",
                    },
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="get_query_history",
            description=(
                "Get recent query execution history. "
                "Shows SQL, rows returned, duration, and execution time."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum history entries to return (default: 20)",
                        "default": 20,
                    },
                },
            },
        ),
        Tool(
            name="clear_query_history",
            description="Clear all query execution history.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="add_validation_rule",
            description=(
                "Add a validation rule to a table. Rule types: not_null (column must have value), "
                "unique (column values must be unique), range (numeric min/max), "
                "regex (string pattern match), expression (SQL predicate). "
                "Rules are enforced on insert, update, and upsert operations."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table",
                    },
                    "rule": {
                        "type": "object",
                        "description": "Rule definition with 'type' and type-specific fields",
                    },
                },
                "required": ["table_name", "rule"],
            },
        ),
        Tool(
            name="list_validation_rules",
            description="List all validation rules for a table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="remove_validation_rule",
            description="Remove a validation rule by its ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table",
                    },
                    "rule_id": {
                        "type": "string",
                        "description": "ID of the rule to remove",
                    },
                },
                "required": ["table_name", "rule_id"],
            },
        ),
        Tool(
            name="validate_data",
            description=(
                "Validate rows against a table's validation rules without writing. "
                "Returns pass/fail with details of which rules failed."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table",
                    },
                    "rows": {
                        "type": "array",
                        "description": "Rows to validate (array of objects)",
                        "items": {"type": "object"},
                    },
                },
                "required": ["table_name", "rows"],
            },
        ),
        Tool(
            name="get_audit_log",
            description=(
                "Get the audit log of write operations. "
                "Optionally filter by table name, operation type, or time range."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Filter by table name",
                    },
                    "operation": {
                        "type": "string",
                        "description": "Filter by operation (insert, update, delete, etc.)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum entries to return (default: 50)",
                        "default": 50,
                    },
                    "since": {
                        "type": "string",
                        "description": "Only entries after this ISO timestamp",
                    },
                },
            },
        ),
        Tool(
            name="clear_audit_log",
            description="Clear audit log entries, optionally only those older than a given duration.",
            inputSchema={
                "type": "object",
                "properties": {
                    "older_than": {
                        "type": "string",
                        "description": "Clear entries older than this (ISO timestamp or duration like '30d', '24h')",
                    },
                },
            },
        ),
        Tool(
            name="get_table_stats",
            description=(
                "Get cached statistics for a table including row count, column stats, "
                "size, snapshots. Auto-refreshes if stale. Fast response for LLM queries."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table",
                    },
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="refresh_table_stats",
            description="Refresh cached statistics for a table or all tables.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table to refresh (omit for all tables)",
                    },
                },
            },
        ),
        Tool(
            name="get_all_stats",
            description="Get cached statistics for all tables in the lakehouse.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="schema_history",
            description="Get the full schema evolution history for a table, showing how its schema changed across snapshots.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="schema_diff",
            description="Compare schemas between two snapshots to see what columns were added, dropped, renamed, or had type changes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "from_snapshot": {"type": "integer", "description": "Starting snapshot ID (optional)"},
                    "to_snapshot": {"type": "integer", "description": "Ending snapshot ID (optional)"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="schema_migration",
            description="Generate migration steps (alter_table operations) between two schema versions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "from_snapshot": {"type": "integer", "description": "Starting snapshot ID (optional)"},
                    "to_snapshot": {"type": "integer", "description": "Ending snapshot ID (optional)"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="schema_compatibility",
            description="Check if proposed schema changes are backward-compatible with the current table schema.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "changes": {
                        "type": "array",
                        "description": "List of proposed changes",
                        "items": {
                            "type": "object",
                            "properties": {
                                "op": {"type": "string", "enum": ["add_column", "drop_column", "rename_column"]},
                                "column": {"type": "string"},
                                "type": {"type": "string"},
                                "new_name": {"type": "string"},
                            },
                            "required": ["op", "column"],
                        },
                    },
                },
                "required": ["table_name", "changes"],
            },
        ),
        Tool(
            name="set_retention_policy",
            description="Set a snapshot retention policy for a table (max age, max count, min keep).",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "max_snapshot_age_hours": {"type": "number", "description": "Max snapshot age in hours (optional)"},
                    "max_snapshot_count": {"type": "integer", "description": "Max number of snapshots (optional)"},
                    "min_snapshots_to_keep": {"type": "integer", "description": "Min snapshots to always keep (default: 1)"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="list_retention_policies",
            description="List all snapshot retention policies.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="evaluate_retention",
            description="Evaluate and enforce snapshot retention policies. Use dry_run to preview without acting.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name (optional, omit for all)"},
                    "dry_run": {"type": "boolean", "description": "Preview without acting (default: false)"},
                },
            },
        ),
        Tool(
            name="set_column_description",
            description="Set a description for a table column to provide context for queries and governance.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "column_name": {"type": "string", "description": "Column name"},
                    "description": {"type": "string", "description": "Column description"},
                },
                "required": ["table_name", "column_name", "description"],
            },
        ),
        Tool(
            name="classify_column",
            description="Set data classification for a column (pii, financial, public, internal, confidential).",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "column_name": {"type": "string", "description": "Column name"},
                    "classification": {
                        "type": "string",
                        "enum": ["pii", "financial", "public", "internal", "confidential"],
                        "description": "Data classification",
                    },
                },
                "required": ["table_name", "column_name", "classification"],
            },
        ),
        Tool(
            name="get_enriched_schema",
            description="Get table schema enriched with descriptions, classifications, and glossary matches. The primary tool for understanding table context.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="search_glossary",
            description="Search the business glossary for terms by name, alias, or definition text.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="add_masking_policy",
            description="Add a data masking policy to a column (hash, redact, nullify, truncate, expression).",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "column_name": {"type": "string", "description": "Column name"},
                    "strategy": {
                        "type": "string",
                        "enum": ["hash", "redact", "nullify", "truncate", "expression"],
                        "description": "Masking strategy",
                    },
                    "options": {
                        "type": "object",
                        "description": "Strategy options (replacement, length, sql)",
                    },
                },
                "required": ["table_name", "column_name", "strategy"],
            },
        ),
        Tool(
            name="list_masking_policies",
            description="List all data masking policies, optionally filtered by table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Filter by table (optional)"},
                },
            },
        ),
        Tool(
            name="remove_masking_policy",
            description="Remove a data masking policy from a column.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "column_name": {"type": "string", "description": "Column name"},
                },
                "required": ["table_name", "column_name"],
            },
        ),
        Tool(
            name="query_with_masking",
            description="Execute a SQL query with data masking policies applied to sensitive columns.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"},
                },
                "required": ["sql"],
            },
        ),
        Tool(
            name="get_watermark",
            description="Get the last-processed snapshot ID for a pipeline/table pair.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {"type": "string", "description": "Pipeline name"},
                    "table_name": {"type": "string", "description": "Table name"},
                },
                "required": ["pipeline_name", "table_name"],
            },
        ),
        Tool(
            name="list_watermarks",
            description="List all watermarks, optionally filtered by pipeline.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {"type": "string", "description": "Filter by pipeline (optional)"},
                },
            },
        ),
        Tool(
            name="reset_watermark",
            description="Reset watermark to force full reprocessing on next pipeline run.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {"type": "string", "description": "Pipeline name"},
                    "table_name": {"type": "string", "description": "Table name (optional, omit to reset all)"},
                },
                "required": ["pipeline_name"],
            },
        ),
        Tool(
            name="run_pipeline_incremental",
            description="Run a pipeline in incremental mode, processing only new data since last watermark.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {"type": "string", "description": "Pipeline name"},
                },
                "required": ["pipeline_name"],
            },
        ),
        Tool(
            name="dashboard",
            description="Get a comprehensive lakehouse status overview including all tables with row counts, sizes, health indicators, recent activity, and namespace summary. This is the 'home screen' for the lakehouse.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="set_maintenance_policy",
            description="Set a maintenance policy for a table (auto-compact, auto-expire, orphan cleanup).",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "policy": {
                        "type": "object",
                        "description": "Policy settings",
                        "properties": {
                            "auto_compact_threshold": {"type": "integer", "description": "Compact when files >= this (default: 10)"},
                            "auto_expire_retain_last": {"type": "integer", "description": "Keep at least N snapshots (default: 5)"},
                            "auto_expire_older_than": {"type": "string", "description": "Expire snapshots older than this (e.g. '30d')"},
                            "auto_cleanup_orphans": {"type": "boolean", "description": "Cleanup orphans after maintenance (default: true)"},
                        },
                    },
                },
                "required": ["table_name", "policy"],
            },
        ),
        Tool(
            name="run_maintenance",
            description="Run maintenance for a table or all tables with policies. Checks compact threshold, snapshot expiration, and orphan cleanup.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table to maintain (omit for all tables with policies)"},
                    "dry_run": {"type": "boolean", "description": "If true, report what would happen without making changes", "default": False},
                },
            },
        ),
        Tool(
            name="check_maintenance",
            description="Check if a table needs maintenance based on its policy.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table to check"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="create_view",
            description="Create a named SQL view (virtual table). Views store SQL definitions and are resolved at query time.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "View name (e.g. 'recent_expenses')"},
                    "sql": {"type": "string", "description": "SQL SELECT query defining the view"},
                    "description": {"type": "string", "description": "Optional description"},
                },
                "required": ["name", "sql"],
            },
        ),
        Tool(
            name="list_views",
            description="List all SQL views with their names, SQL definitions, and descriptions.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="query_view",
            description="Execute a SQL view and return results.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "View name to query"},
                    "max_rows": {"type": "integer", "description": "Maximum rows to return (default: 1000)", "default": 1000},
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="drop_view",
            description="Drop a SQL view by name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "View name to drop"},
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="tag_table",
            description="Add or remove tags on a table for organization and discovery.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "tags": {"type": "array", "items": {"type": "string"}, "description": "Tags to add"},
                    "remove": {"type": "boolean", "description": "If true, remove the tags instead of adding", "default": False},
                },
                "required": ["table_name", "tags"],
            },
        ),
        Tool(
            name="search_by_tag",
            description="Find all tables with a given tag.",
            inputSchema={
                "type": "object",
                "properties": {
                    "tag": {"type": "string", "description": "Tag to search for"},
                },
                "required": ["tag"],
            },
        ),
        Tool(
            name="set_table_description",
            description="Set a human-readable description for a table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "description": {"type": "string", "description": "Description text"},
                },
                "required": ["table_name", "description"],
            },
        ),
        Tool(
            name="bookmark_table",
            description="Bookmark or unbookmark a table for quick access.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "remove": {"type": "boolean", "description": "If true, remove the bookmark", "default": False},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="search_tables",
            description="Search tables by name, tag, or description. Returns matching tables with metadata.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search string"},
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="record_lineage",
            description="Record a data lineage edge: source tables → target table. Tracks which tables were used to produce other tables.",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_tables": {"type": "array", "items": {"type": "string"}, "description": "Source table names"},
                    "target_table": {"type": "string", "description": "Target table name"},
                    "operation": {"type": "string", "description": "Operation type (manual, insert_from, view, pipeline)", "default": "manual"},
                    "sql": {"type": "string", "description": "Optional SQL that produced this relationship"},
                },
                "required": ["source_tables", "target_table"],
            },
        ),
        Tool(
            name="get_lineage",
            description="Get upstream or downstream lineage for a table. Shows data dependencies and impact.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "direction": {"type": "string", "enum": ["upstream", "downstream", "impact"], "description": "Direction: upstream (what feeds in), downstream (what depends on it), or impact (drop analysis)", "default": "upstream"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="lineage_graph",
            description="Get the full data lineage graph showing all table dependencies.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="clone_table",
            description="Clone an Iceberg table for safe experimentation. Creates a new table with the same data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_table": {"type": "string", "description": "Source table name to clone"},
                    "target_table": {"type": "string", "description": "Target table name for the clone"},
                    "as_of": {"type": "string", "description": "Optional snapshot ID or ISO timestamp for point-in-time clone"},
                },
                "required": ["source_table", "target_table"],
            },
        ),
        Tool(
            name="list_clones",
            description="List all active table clones.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="promote_clone",
            description="Replace the original table's data with the clone's data, then remove the clone.",
            inputSchema={
                "type": "object",
                "properties": {
                    "clone_table": {"type": "string", "description": "Clone table name"},
                    "original_table": {"type": "string", "description": "Original table to overwrite"},
                },
                "required": ["clone_table", "original_table"],
            },
        ),
        Tool(
            name="discard_clone",
            description="Drop a cloned table and remove its metadata.",
            inputSchema={
                "type": "object",
                "properties": {
                    "clone_table": {"type": "string", "description": "Clone table name to discard"},
                },
                "required": ["clone_table"],
            },
        ),
        Tool(
            name="execute_join",
            description="Execute a cross-table join query. Supports namespace-qualified table references like 'default.expenses'.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL join query"},
                    "max_rows": {"type": "integer", "description": "Maximum rows to return (default: 1000)", "default": 1000},
                },
                "required": ["sql"],
            },
        ),
        Tool(
            name="join_to_table",
            description="Execute a join query and save results to a new or existing table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL join query"},
                    "target_table": {"type": "string", "description": "Target table name for results"},
                    "mode": {"type": "string", "enum": ["overwrite", "append"], "description": "Write mode", "default": "overwrite"},
                },
                "required": ["sql", "target_table"],
            },
        ),
        Tool(
            name="suggest_joins",
            description="Suggest possible joins for a table by finding matching column names across other tables.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table to find join partners for"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="create_materialized_view",
            description="Create a materialized view — execute SQL and cache results as an Iceberg table for fast repeated access.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "View name"},
                    "sql": {"type": "string", "description": "SQL SELECT query"},
                    "description": {"type": "string", "description": "Optional description"},
                },
                "required": ["name", "sql"],
            },
        ),
        Tool(
            name="list_materialized_views",
            description="List all materialized views with metadata and freshness info.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="query_materialized_view",
            description="Query the cached results of a materialized view (fast, no re-execution).",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "View name"},
                    "max_rows": {"type": "integer", "description": "Maximum rows", "default": 1000},
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="refresh_materialized_view",
            description="Refresh a materialized view by re-executing its SQL and updating the cached data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "View name to refresh"},
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="drop_materialized_view",
            description="Drop a materialized view and its backing Iceberg table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "View name to drop"},
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="create_pipeline",
            description="Create a multi-step data pipeline — ordered SQL transformations that can be run repeatedly.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Pipeline name"},
                    "steps": {
                        "type": "array",
                        "description": "List of steps, each with sql (required), target_table (optional), mode (optional: overwrite/append)",
                        "items": {
                            "type": "object",
                            "properties": {
                                "sql": {"type": "string"},
                                "target_table": {"type": "string"},
                                "mode": {"type": "string", "enum": ["overwrite", "append"]},
                            },
                            "required": ["sql"],
                        },
                    },
                    "description": {"type": "string", "description": "Optional description"},
                },
                "required": ["name", "steps"],
            },
        ),
        Tool(
            name="list_pipelines",
            description="List all defined data pipelines.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="run_pipeline",
            description="Execute a data pipeline — runs all steps sequentially. Use dry_run=true to validate without executing.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Pipeline name"},
                    "dry_run": {"type": "boolean", "description": "Validate SQL without executing", "default": False},
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="drop_pipeline",
            description="Drop a data pipeline definition.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Pipeline name to drop"},
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="quality_score",
            description="Compute a data quality score (0-100) for a table based on completeness, uniqueness, freshness, and rule compliance.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table to score"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="detect_anomalies",
            description="Detect data anomalies by comparing current data with cached statistics (row count changes, NULL spikes, value drift).",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table to check for anomalies"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="quality_report",
            description="Generate a quality report for all tables with scores, anomalies, and recommendations.",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    try:
        if name == "query":
            sql = arguments.get("sql")
            if not sql:
                return [TextContent(
                    type="text",
                    text="Error: 'sql' parameter is required",
                )]

            max_rows = arguments.get("max_rows", 1000)
            as_of = arguments.get("as_of")
            table_name = arguments.get("table_name")
            engine = get_engine()

            try:
                if as_of:
                    if not table_name:
                        return [TextContent(
                            type="text",
                            text="Error: 'table_name' is required when using 'as_of' for time travel queries",
                        )]
                    result = engine.execute_as_of(sql, table_name, as_of, max_rows=max_rows)
                else:
                    result = engine.execute(sql, max_rows=max_rows)

                # Format as markdown table for readability
                if result.empty:
                    return [TextContent(type="text", text="Query returned no results.")]

                # Convert to markdown
                markdown = result.to_markdown(index=False)
                row_count = len(result)

                header = f"**Results ({row_count} rows)"
                if as_of:
                    header += f" as of {as_of}"
                header += ":**"

                return [TextContent(
                    type="text",
                    text=f"{header}\n\n{markdown}",
                )]

            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Query error: {str(e)}",
                )]

        elif name == "list_snapshots":
            table_name = arguments.get("table_name")
            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            try:
                catalog = get_catalog()
                snapshots = get_snapshots(catalog, table_name)

                if not snapshots:
                    return [TextContent(
                        type="text",
                        text=f"No snapshots found for `{table_name}`.",
                    )]

                lines = [f"**Snapshots for `{table_name}` ({len(snapshots)}):**\n"]
                for s in snapshots:
                    lines.append(
                        f"- **{s['snapshot_id']}** | {s['timestamp']} | {s['operation'] or 'unknown'}"
                    )

                return [TextContent(
                    type="text",
                    text="\n".join(lines),
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Error: {str(e)}",
                )]

        elif name == "snapshot_diff":
            table_name = arguments.get("table_name")
            from_snap = arguments.get("from_snapshot")
            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]
            if not from_snap:
                return [TextContent(type="text", text="Error: 'from_snapshot' parameter is required")]

            to_snap = arguments.get("to_snapshot")

            try:
                catalog = get_catalog()
                result = snapshot_diff(catalog, table_name, from_snap, to_snap)

                s = result["summary"]
                lines = [
                    f"**Diff for `{table_name}` (snapshot {result['from_snapshot_id']} → {result['to_snapshot_id']}):**\n",
                    f"- **Added:** {s['added']} rows",
                    f"- **Deleted:** {s['deleted']} rows",
                    f"- **Modified:** {s['modified']} rows",
                ]

                if result["added"]:
                    lines.append(f"\n**Added rows ({s['added']}):**")
                    cols = list(result["added"][0].keys())
                    lines.append("| " + " | ".join(cols) + " |")
                    lines.append("| " + " | ".join("---" for _ in cols) + " |")
                    for row in result["added"][:50]:
                        lines.append("| " + " | ".join(str(v) for v in row.values()) + " |")

                if result["deleted"]:
                    lines.append(f"\n**Deleted rows ({s['deleted']}):**")
                    cols = list(result["deleted"][0].keys())
                    lines.append("| " + " | ".join(cols) + " |")
                    lines.append("| " + " | ".join("---" for _ in cols) + " |")
                    for row in result["deleted"][:50]:
                        lines.append("| " + " | ".join(str(v) for v in row.values()) + " |")

                return [TextContent(type="text", text="\n".join(lines))]

            except ValueError as e:
                return [TextContent(type="text", text=f"Diff error: {str(e)}")]

            except Exception as e:
                return [TextContent(type="text", text=f"Diff failed: {str(e)}")]

        elif name == "list_tables":
            engine = get_engine()
            tables = engine.list_registered_tables()

            if not tables:
                return [TextContent(
                    type="text",
                    text="No tables found. Run `lakehouse init` to create sample tables.",
                )]

            table_list = "\n".join([f"- {t}" for t in tables])
            return [TextContent(
                type="text",
                text=f"**Available Tables:**\n\n{table_list}",
            )]

        elif name == "describe_table":
            table_name = arguments.get("table_name")
            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            engine = get_engine()

            try:
                schema_df = engine.get_schema(table_name)
                markdown = schema_df.to_markdown(index=False)

                return [TextContent(
                    type="text",
                    text=f"**Schema for `{table_name}`:**\n\n{markdown}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Error describing table: {str(e)}",
                )]

        elif name == "refresh":
            engine = get_engine()
            engine.refresh()
            return [TextContent(
                type="text",
                text="Table data refreshed successfully.",
            )]

        elif name == "insert":
            table_name = arguments.get("table_name")
            rows = arguments.get("rows")

            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            if not rows:
                return [TextContent(
                    type="text",
                    text="Error: 'rows' parameter is required and must not be empty",
                )]

            if not isinstance(rows, list):
                return [TextContent(
                    type="text",
                    text="Error: 'rows' must be an array of objects",
                )]

            try:
                catalog = get_catalog()
                row_count = insert_rows(catalog, table_name, rows)

                # Refresh the query engine to pick up new data
                engine = get_engine()
                engine.refresh()

                return [TextContent(
                    type="text",
                    text=f"✓ Successfully inserted {row_count} row(s) into `{table_name}`.",
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Insert error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Insert failed: {str(e)}",
                )]

        elif name == "update":
            table_name = arguments.get("table_name")
            filter_expr = arguments.get("filter")
            updates = arguments.get("updates")

            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            if not filter_expr:
                return [TextContent(
                    type="text",
                    text="Error: 'filter' parameter is required",
                )]

            if not updates or not isinstance(updates, dict):
                return [TextContent(
                    type="text",
                    text="Error: 'updates' parameter is required and must be an object",
                )]

            try:
                catalog = get_catalog()
                row_count = update_rows(catalog, table_name, filter_expr, updates)

                # Refresh the query engine to pick up changes
                engine = get_engine()
                engine.refresh()

                if row_count == 0:
                    return [TextContent(
                        type="text",
                        text=f"No rows matched the filter `{filter_expr}` in `{table_name}`.",
                    )]

                return [TextContent(
                    type="text",
                    text=f"✓ Successfully updated {row_count} row(s) in `{table_name}`.",
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Update error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Update failed: {str(e)}",
                )]

        elif name == "delete":
            table_name = arguments.get("table_name")
            filter_expr = arguments.get("filter")

            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            if not filter_expr:
                return [TextContent(
                    type="text",
                    text="Error: 'filter' parameter is required (use the query tool to inspect data before deleting)",
                )]

            try:
                catalog = get_catalog()
                row_count = delete_rows(catalog, table_name, filter_expr)

                # Refresh the query engine to pick up changes
                engine = get_engine()
                engine.refresh()

                if row_count == 0:
                    return [TextContent(
                        type="text",
                        text=f"No rows matched the filter `{filter_expr}` in `{table_name}`.",
                    )]

                return [TextContent(
                    type="text",
                    text=f"✓ Successfully deleted {row_count} row(s) from `{table_name}`.",
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Delete error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Delete failed: {str(e)}",
                )]

        elif name == "upsert":
            table_name = arguments.get("table_name")
            key_columns = arguments.get("key_columns")
            rows = arguments.get("rows")

            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            if not key_columns or not isinstance(key_columns, list):
                return [TextContent(
                    type="text",
                    text="Error: 'key_columns' parameter is required and must be an array of column names",
                )]

            if not rows or not isinstance(rows, list):
                return [TextContent(
                    type="text",
                    text="Error: 'rows' parameter is required and must be a non-empty array",
                )]

            try:
                catalog = get_catalog()
                result = upsert_rows(catalog, table_name, key_columns, rows)

                # Refresh the query engine to pick up changes
                engine = get_engine()
                engine.refresh()

                return [TextContent(
                    type="text",
                    text=(
                        f"✓ Upsert into `{table_name}` complete: "
                        f"{result['inserted']} inserted, {result['updated']} updated."
                    ),
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Upsert error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Upsert failed: {str(e)}",
                )]

        elif name == "alter_table":
            table_name = arguments.get("table_name")
            operation = arguments.get("operation")
            column_name = arguments.get("column_name")
            new_name = arguments.get("new_name")
            column_type = arguments.get("column_type")

            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            if not operation:
                return [TextContent(
                    type="text",
                    text="Error: 'operation' parameter is required (add_column, drop_column, rename_column)",
                )]

            if not column_name:
                return [TextContent(
                    type="text",
                    text="Error: 'column_name' parameter is required",
                )]

            try:
                catalog = get_catalog()
                result_msg = alter_table(
                    catalog, table_name, operation, column_name,
                    new_name=new_name, column_type=column_type,
                )

                # Refresh the query engine to pick up schema changes
                engine = get_engine()
                engine.refresh()

                return [TextContent(
                    type="text",
                    text=f"✓ {result_msg}",
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Alter table error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Alter table failed: {str(e)}",
                )]

        elif name == "rollback":
            table_name = arguments.get("table_name")
            snapshot_id = arguments.get("snapshot_id")
            timestamp = arguments.get("timestamp")

            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            if not snapshot_id and not timestamp:
                return [TextContent(
                    type="text",
                    text="Error: either 'snapshot_id' or 'timestamp' must be provided",
                )]

            try:
                catalog = get_catalog()
                result = rollback_table(catalog, table_name, snapshot_id=snapshot_id, timestamp=timestamp)

                engine = get_engine()
                engine.refresh()

                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Rollback error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Rollback failed: {str(e)}",
                )]

        elif name == "expire_snapshots":
            table_name = arguments.get("table_name")
            older_than = arguments.get("older_than")
            retain_last = arguments.get("retain_last")

            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            if not older_than and not retain_last:
                return [TextContent(
                    type="text",
                    text="Error: either 'older_than' or 'retain_last' must be provided",
                )]

            try:
                catalog = get_catalog()
                result = expire_snapshots(catalog, table_name, older_than=older_than, retain_last=retain_last)

                engine = get_engine()
                engine.refresh()

                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Expire error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Expire failed: {str(e)}",
                )]

        elif name == "batch":
            operations = arguments.get("operations")

            if not operations or not isinstance(operations, list):
                return [TextContent(
                    type="text",
                    text="Error: 'operations' parameter is required and must be a non-empty array",
                )]

            try:
                catalog = get_catalog()
                results = execute_batch(catalog, operations)

                engine = get_engine()
                engine.refresh()

                # Format results
                ok_count = sum(1 for r in results if r["status"] == "ok")
                err_count = sum(1 for r in results if r["status"] == "error")
                skip_count = sum(1 for r in results if r["status"] == "skipped")

                lines = [f"**Batch complete: {ok_count} succeeded, {err_count} failed, {skip_count} skipped**\n"]
                for r in results:
                    if r["status"] == "ok":
                        lines.append(f"- ✓ [{r['index']}] {r['action']} on `{r['table']}`: {r['rows_affected']} rows")
                    elif r["status"] == "error":
                        lines.append(f"- ✗ [{r['index']}] {r.get('action', '?')}: {r['message']}")
                    else:
                        lines.append(f"- ⊘ [{r['index']}] {r['message']}")

                return [TextContent(
                    type="text",
                    text="\n".join(lines),
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Batch error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Batch failed: {str(e)}",
                )]

        elif name == "query_vortex":
            sql = arguments.get("sql")
            vortex_path = arguments.get("vortex_path")
            table_name = arguments.get("table_name", "data")
            max_rows = arguments.get("max_rows", 1000)

            if not sql:
                return [TextContent(
                    type="text",
                    text="Error: 'sql' parameter is required",
                )]

            if not vortex_path:
                return [TextContent(
                    type="text",
                    text="Error: 'vortex_path' parameter is required",
                )]

            try:
                engine = get_engine()
                result = engine.query_vortex(sql, vortex_path, table_name=table_name, max_rows=max_rows)

                if result.empty:
                    return [TextContent(type="text", text="Query returned no results.")]

                markdown = result.to_markdown(index=False)
                return [TextContent(
                    type="text",
                    text=f"**Results ({len(result)} rows):**\n\n{markdown}",
                )]

            except FileNotFoundError as e:
                return [TextContent(
                    type="text",
                    text=f"File error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Query error: {str(e)}",
                )]

        elif name == "convert_format":
            table_name = arguments.get("table_name")
            output_dir = arguments.get("output_dir", ".")
            compact = arguments.get("compact", False)

            if not table_name:
                return [TextContent(
                    type="text",
                    text="Error: 'table_name' parameter is required",
                )]

            try:
                from .vortex_io import convert_table_to_vortex

                catalog = get_catalog()
                result = convert_table_to_vortex(
                    catalog, table_name, output_dir, compact=compact,
                )

                return [TextContent(
                    type="text",
                    text=(
                        f"✓ Exported `{result['table']}` to Vortex format.\n"
                        f"  Output: {result['output']}\n"
                        f"  Rows: {result['rows']:,}\n"
                        f"  Size: {result['size_bytes']:,} bytes"
                    ),
                )]

            except ValueError as e:
                return [TextContent(
                    type="text",
                    text=f"Convert error: {str(e)}",
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Convert failed: {str(e)}",
                )]

        elif name == "get_format_config":
            table_name = arguments.get("table_name")

            try:
                from .config import get_default_format, get_table_format, resolve_format_with_table

                if table_name:
                    catalog = get_catalog()
                    props = {}
                    try:
                        tbl = catalog.load_table(
                            f"default.{table_name}" if "." not in table_name else table_name
                        )
                        props = dict(tbl.properties)
                    except Exception:
                        pass

                    effective = resolve_format_with_table(table_name, props)
                    table_prop = props.get("write.format.default", "not set")

                    return [TextContent(
                        type="text",
                        text=(
                            f"Format config for `{table_name}`:\n"
                            f"  Effective format: **{effective}**\n"
                            f"  Table property (write.format.default): {table_prop}\n"
                            f"  Config file default: {get_default_format()}"
                        ),
                    )]
                else:
                    from .config import get_config_summary
                    summary = get_config_summary()
                    lines = [f"**Global default format:** {summary['default_format']}"]
                    if summary['table_overrides']:
                        lines.append("\n**Per-table overrides:**")
                        for t, f in summary['table_overrides'].items():
                            lines.append(f"  - {t}: {f}")
                    return [TextContent(type="text", text="\n".join(lines))]

            except Exception as e:
                return [TextContent(type="text", text=f"Config error: {str(e)}")]

        elif name == "set_format_config":
            fmt = arguments.get("format")
            table_name = arguments.get("table_name")

            if not fmt:
                return [TextContent(type="text", text="Error: 'format' parameter is required")]

            try:
                from .config import set_default_format, set_table_format

                if table_name:
                    set_table_format(table_name, fmt)
                    return [TextContent(
                        type="text",
                        text=f"✓ Set format for `{table_name}` to **{fmt}**",
                    )]
                else:
                    set_default_format(fmt)
                    return [TextContent(
                        type="text",
                        text=f"✓ Set global default format to **{fmt}**",
                    )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Config error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Config failed: {str(e)}")]

        elif name == "set_table_property":
            table_name = arguments.get("table_name")
            key = arguments.get("key")
            value = arguments.get("value")

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]
            if not key:
                return [TextContent(type="text", text="Error: 'key' parameter is required")]
            if not value:
                return [TextContent(type="text", text="Error: 'value' parameter is required")]

            try:
                catalog = get_catalog()
                msg = set_table_property(catalog, table_name, key, value)

                engine = get_engine()
                engine.refresh()

                return [TextContent(type="text", text=f"✓ {msg}")]

            except ValueError as e:
                return [TextContent(type="text", text=f"Property error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Property failed: {str(e)}")]

        elif name == "import_file":
            file_path = arguments.get("file_path")
            table_name = arguments.get("table_name")
            file_format = arguments.get("format")
            if_exists = arguments.get("if_exists", "fail")
            delimiter = arguments.get("delimiter", ",")
            has_header = arguments.get("has_header", True)

            if not file_path:
                return [TextContent(type="text", text="Error: 'file_path' parameter is required")]
            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                result = import_file(
                    catalog, file_path, table_name,
                    file_format=file_format,
                    if_exists=if_exists,
                    delimiter=delimiter,
                    has_header=has_header,
                )

                engine = get_engine()
                engine.refresh()

                return [TextContent(
                    type="text",
                    text=(
                        f"✓ Imported {result['rows_imported']:,} rows "
                        f"from `{file_path}` into `{result['table']}` "
                        f"(format: {result['format']})"
                    ),
                )]

            except FileNotFoundError as e:
                return [TextContent(type="text", text=f"File error: {str(e)}")]
            except ValueError as e:
                return [TextContent(type="text", text=f"Import error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Import failed: {str(e)}")]

        elif name == "export_table":
            table_name = arguments.get("table_name")
            file_format = arguments.get("format")
            output_path = arguments.get("output_path")
            where = arguments.get("where")
            columns = arguments.get("columns")
            limit = arguments.get("limit")

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                result = export_table(
                    catalog, table_name, output_path,
                    file_format=file_format,
                    where=where,
                    columns=columns,
                    limit=limit,
                )

                return [TextContent(
                    type="text",
                    text=(
                        f"✓ Exported {result['rows_exported']:,} rows "
                        f"from `{result['table']}` to `{result['output']}` "
                        f"(format: {result['format']}, {result['size_bytes']:,} bytes)"
                    ),
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Export error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Export failed: {str(e)}")]

        elif name == "profile_table":
            table_name = arguments.get("table_name")
            columns = arguments.get("columns")

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                stats = profile_table(catalog, table_name, columns=columns)

                import json
                return [TextContent(
                    type="text",
                    text=f"**Profile for `{stats['table']}` ({stats['row_count']:,} rows):**\n\n```json\n{json.dumps(stats, indent=2, default=str)}\n```",
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Profile error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Profile failed: {str(e)}")]

        elif name == "compact_table":
            table_name = arguments.get("table_name")
            target_size_mb = arguments.get("target_size_mb", 128)

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                result = compact_table(catalog, table_name, target_size_mb=target_size_mb)

                engine = get_engine()
                engine.refresh()

                return [TextContent(
                    type="text",
                    text=(
                        f"✓ {result['message']}\n"
                        f"  Rows: {result['rows']:,}\n"
                        f"  Size: {result['size_before']:,} → {result['size_after']:,} bytes"
                    ),
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Compact error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Compact failed: {str(e)}")]

        elif name == "maintenance_status":
            table_name = arguments.get("table_name")

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                status = maintenance_status(catalog, table_name)

                return [TextContent(
                    type="text",
                    text=(
                        f"**Maintenance status for `{status['table']}`:**\n\n"
                        f"| Metric | Value |\n|--------|-------|\n"
                        f"| Data files | {status['data_files']} |\n"
                        f"| Total size | {status['total_size_bytes']:,} bytes |\n"
                        f"| Avg file size | {status['avg_file_size']:,} bytes |\n"
                        f"| Snapshots | {status['snapshots']} |\n"
                        f"| Orphan files | {status['orphan_files']} |\n"
                        f"| Orphan size | {status['orphan_bytes']:,} bytes |"
                    ),
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Status error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Status failed: {str(e)}")]

        elif name == "cleanup_orphans":
            table_name = arguments.get("table_name")
            dry_run = arguments.get("dry_run", True)

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                result = cleanup_orphans(catalog, table_name, dry_run=dry_run)

                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Cleanup error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Cleanup failed: {str(e)}")]

        elif name == "create_table":
            table_name = arguments.get("table_name")
            columns = arguments.get("columns")
            partitions = arguments.get("partitions")

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]
            if not columns or not isinstance(columns, dict):
                return [TextContent(type="text", text="Error: 'columns' parameter is required and must be an object")]

            try:
                catalog = get_catalog()
                result = create_table(catalog, table_name, columns, partitions=partitions)

                engine = get_engine()
                engine.refresh()

                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Create table error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Create table failed: {str(e)}")]

        elif name == "get_partitions":
            table_name = arguments.get("table_name")

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                info = get_partitions(catalog, table_name)

                if not info["is_partitioned"]:
                    return [TextContent(
                        type="text",
                        text=f"Table `{info['table']}` is not partitioned.",
                    )]

                lines = [f"**Partitions for `{info['table']}`:**\n"]
                lines.append("| Name | Source Column | Transform |")
                lines.append("|------|--------------|-----------|")
                for f in info["fields"]:
                    lines.append(f"| {f['name']} | {f['source_column']} | {f['transform']} |")

                return [TextContent(type="text", text="\n".join(lines))]

            except ValueError as e:
                return [TextContent(type="text", text=f"Partitions error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Partitions failed: {str(e)}")]

        elif name == "get_partition_stats":
            table_name = arguments.get("table_name")

            if not table_name:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                stats = get_partition_stats(catalog, table_name)

                if not stats["is_partitioned"]:
                    return [TextContent(type="text", text=stats["message"])]

                lines = [f"**Partition stats for `{stats['table']}` ({stats['total_partitions']} partition(s)):**\n"]
                lines.append("| Partition | Files | Size |")
                lines.append("|-----------|-------|------|")
                for p in stats["partitions"]:
                    lines.append(f"| {p['partition']} | {p['files']} | {p['size_bytes']:,} bytes |")

                return [TextContent(type="text", text="\n".join(lines))]

            except ValueError as e:
                return [TextContent(type="text", text=f"Partition stats error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Partition stats failed: {str(e)}")]

        elif name == "list_namespaces":
            try:
                catalog = get_catalog()
                namespaces = list_namespaces(catalog)

                if not namespaces:
                    return [TextContent(type="text", text="No namespaces found.")]

                ns_list = "\n".join(f"- {ns}" for ns in namespaces)
                return [TextContent(
                    type="text",
                    text=f"**Namespaces:**\n\n{ns_list}",
                )]

            except Exception as e:
                return [TextContent(type="text", text=f"List namespaces failed: {str(e)}")]

        elif name == "create_namespace":
            namespace = arguments.get("namespace")
            properties = arguments.get("properties")

            if not namespace:
                return [TextContent(type="text", text="Error: 'namespace' parameter is required")]

            try:
                catalog = get_catalog()
                result = create_namespace(catalog, namespace, properties=properties)

                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Create namespace error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Create namespace failed: {str(e)}")]

        elif name == "drop_namespace":
            namespace = arguments.get("namespace")

            if not namespace:
                return [TextContent(type="text", text="Error: 'namespace' parameter is required")]

            try:
                catalog = get_catalog()
                result = drop_namespace(catalog, namespace)

                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Drop namespace error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Drop namespace failed: {str(e)}")]

        elif name == "get_namespace_properties":
            namespace = arguments.get("namespace")

            if not namespace:
                return [TextContent(type="text", text="Error: 'namespace' parameter is required")]

            try:
                catalog = get_catalog()
                result = get_namespace_properties(catalog, namespace)

                if not result["properties"]:
                    return [TextContent(
                        type="text",
                        text=f"Namespace `{namespace}` has no properties set.",
                    )]

                lines = [f"**Properties for namespace `{namespace}`:**\n"]
                for k, v in result["properties"].items():
                    lines.append(f"- **{k}**: {v}")

                return [TextContent(type="text", text="\n".join(lines))]

            except ValueError as e:
                return [TextContent(type="text", text=f"Namespace properties error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Namespace properties failed: {str(e)}")]

        elif name == "save_query":
            query_name = arguments.get("name")
            sql = arguments.get("sql")
            description = arguments.get("description", "")

            if not query_name:
                return [TextContent(type="text", text="Error: 'name' parameter is required")]
            if not sql:
                return [TextContent(type="text", text="Error: 'sql' parameter is required")]

            try:
                result = save_query(query_name, sql, description=description)
                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Save query error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Save query failed: {str(e)}")]

        elif name == "list_saved_queries":
            try:
                queries = list_saved_queries()

                if not queries:
                    return [TextContent(type="text", text="No saved queries.")]

                lines = [f"**Saved Queries ({len(queries)}):**\n"]
                for q in queries:
                    desc = f" - {q['description']}" if q.get("description") else ""
                    lines.append(f"- **{q['name']}**: `{q['sql']}`{desc}")

                return [TextContent(type="text", text="\n".join(lines))]

            except Exception as e:
                return [TextContent(type="text", text=f"List queries failed: {str(e)}")]

        elif name == "run_saved_query":
            query_name = arguments.get("name")
            max_rows = arguments.get("max_rows", 1000)

            if not query_name:
                return [TextContent(type="text", text="Error: 'name' parameter is required")]

            try:
                saved = get_saved_query(query_name)
                sql = saved["sql"]

                engine = get_engine()
                result = engine.execute(sql, max_rows=max_rows)

                if result.empty:
                    return [TextContent(type="text", text=f"Query `{query_name}` returned no results.")]

                markdown = result.to_markdown(index=False)
                return [TextContent(
                    type="text",
                    text=f"**Results for `{query_name}` ({len(result)} rows):**\n\n{markdown}",
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Run query error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Run query failed: {str(e)}")]

        elif name == "delete_saved_query":
            query_name = arguments.get("name")

            if not query_name:
                return [TextContent(type="text", text="Error: 'name' parameter is required")]

            try:
                result = delete_saved_query(query_name)
                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except ValueError as e:
                return [TextContent(type="text", text=f"Delete query error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Delete query failed: {str(e)}")]

        elif name == "get_query_history":
            limit = arguments.get("limit", 20)

            try:
                history = get_history(limit=limit)

                if not history:
                    return [TextContent(type="text", text="No query history.")]

                lines = [f"**Query History (last {len(history)}):**\n"]
                for entry in history:
                    duration = f" ({entry['duration_ms']}ms)" if entry.get("duration_ms") else ""
                    rows = f" → {entry['rows_returned']} rows" if entry.get("rows_returned") else ""
                    lines.append(f"- `{entry['sql']}`{rows}{duration} at {entry.get('executed_at', '')[:19]}")

                return [TextContent(type="text", text="\n".join(lines))]

            except Exception as e:
                return [TextContent(type="text", text=f"History failed: {str(e)}")]

        elif name == "clear_query_history":
            try:
                result = clear_history()
                return [TextContent(
                    type="text",
                    text=f"✓ {result['message']}",
                )]

            except Exception as e:
                return [TextContent(type="text", text=f"Clear history failed: {str(e)}")]

        elif name == "add_validation_rule":
            tbl = arguments.get("table_name")
            rule = arguments.get("rule")
            if not tbl:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]
            if not rule:
                return [TextContent(type="text", text="Error: 'rule' parameter is required")]

            try:
                result = add_validation_rule(tbl, rule)
                return [TextContent(type="text", text=f"✓ {result['message']}")]
            except ValueError as e:
                return [TextContent(type="text", text=f"Validation rule error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Add rule failed: {str(e)}")]

        elif name == "list_validation_rules":
            tbl = arguments.get("table_name")
            if not tbl:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                rules = list_validation_rules(tbl)
                if not rules:
                    return [TextContent(type="text", text=f"No validation rules for `{tbl}`.")]

                lines = [f"**Validation rules for `{tbl}` ({len(rules)}):**\n"]
                for r in rules:
                    lines.append(f"- **{r['id']}** | {r['type']} | {json.dumps({k: v for k, v in r.items() if k not in ('id', 'type', 'created_at')})}")

                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"List rules failed: {str(e)}")]

        elif name == "remove_validation_rule":
            tbl = arguments.get("table_name")
            rule_id = arguments.get("rule_id")
            if not tbl:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]
            if not rule_id:
                return [TextContent(type="text", text="Error: 'rule_id' parameter is required")]

            try:
                result = remove_validation_rule(tbl, rule_id)
                return [TextContent(type="text", text=f"✓ {result['message']}")]
            except ValueError as e:
                return [TextContent(type="text", text=f"Remove rule error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Remove rule failed: {str(e)}")]

        elif name == "validate_data":
            tbl = arguments.get("table_name")
            rows = arguments.get("rows")
            if not tbl:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]
            if not rows:
                return [TextContent(type="text", text="Error: 'rows' parameter is required")]

            try:
                rules = list_validation_rules(tbl)
                if not rules:
                    return [TextContent(type="text", text=f"No validation rules for `{tbl}`. All data passes.")]

                result = validate_rows(rows, rules)
                if result["valid"]:
                    return [TextContent(type="text", text=f"✓ All {result['checked']} rows pass validation")]

                lines = [f"**Validation failed** ({len(result['failures'])} issues):\n"]
                for f in result["failures"]:
                    lines.append(f"- {f['message']}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Validate failed: {str(e)}")]

        elif name == "get_audit_log":
            try:
                entries = get_audit_log(
                    table_name=arguments.get("table_name"),
                    operation=arguments.get("operation"),
                    limit=arguments.get("limit", 50),
                    since=arguments.get("since"),
                )
                if not entries:
                    return [TextContent(type="text", text="No audit log entries found.")]

                lines = [f"**Audit log ({len(entries)} entries):**\n"]
                for e in entries:
                    details = e.get("details", {})
                    detail_str = f" | {json.dumps(details)}" if details else ""
                    lines.append(
                        f"- {e['timestamp'][:19]} | **{e['operation']}** on `{e['table']}` | "
                        f"{e['rows_affected']} rows | {e['source']}{detail_str}"
                    )
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Audit log failed: {str(e)}")]

        elif name == "clear_audit_log":
            try:
                result = clear_audit_log(older_than=arguments.get("older_than"))
                return [TextContent(type="text", text=f"✓ {result['message']}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Clear audit log failed: {str(e)}")]

        elif name == "get_table_stats":
            tbl = arguments.get("table_name")
            if not tbl:
                return [TextContent(type="text", text="Error: 'table_name' parameter is required")]

            try:
                catalog = get_catalog()
                # Auto-refresh if stale
                if is_stats_stale(tbl, catalog):
                    stats = compute_table_stats(catalog, tbl)
                else:
                    stats = get_cached_stats(tbl)

                if not stats:
                    return [TextContent(type="text", text=f"No stats for `{tbl}`. Use refresh_table_stats first.")]

                lines = [
                    f"**Stats for `{tbl}`:**\n",
                    f"- **Rows:** {stats['row_count']}",
                    f"- **Columns:** {stats['column_count']}",
                    f"- **Data files:** {stats['data_files']}",
                    f"- **Size:** {stats['size_bytes']} bytes",
                    f"- **Snapshots:** {stats['snapshot_count']}",
                    f"- **Last modified:** {stats.get('last_modified', 'N/A')}",
                ]
                if stats.get("columns"):
                    lines.append("\n**Column stats:**")
                    for col, info in stats["columns"].items():
                        parts = [f"type={info['type']}"]
                        if "min" in info:
                            parts.append(f"min={info['min']}")
                        if "max" in info:
                            parts.append(f"max={info['max']}")
                        if "unique" in info:
                            parts.append(f"unique={info['unique']}")
                        if "nulls" in info:
                            parts.append(f"nulls={info['nulls']}")
                        lines.append(f"- **{col}**: {', '.join(parts)}")

                return [TextContent(type="text", text="\n".join(lines))]
            except ValueError as e:
                return [TextContent(type="text", text=f"Stats error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Stats failed: {str(e)}")]

        elif name == "refresh_table_stats":
            try:
                catalog = get_catalog()
                tbl = arguments.get("table_name")
                result = refresh_stats(catalog, table_name=tbl)
                return [TextContent(type="text", text=f"✓ {result['message']}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Refresh stats failed: {str(e)}")]

        elif name == "get_all_stats":
            try:
                all_stats = get_all_cached_stats()
                if not all_stats:
                    return [TextContent(type="text", text="No cached stats. Use refresh_table_stats to compute.")]

                lines = [f"**Cached stats for {len(all_stats)} table(s):**\n"]
                for tbl, s in all_stats.items():
                    lines.append(
                        f"- **{tbl}**: {s['row_count']} rows, {s['column_count']} cols, "
                        f"{s['data_files']} files, {s['size_bytes']} bytes, "
                        f"{s['snapshot_count']} snapshots"
                    )
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Get all stats failed: {str(e)}")]

        elif name == "set_maintenance_policy":
            try:
                tbl = arguments.get("table_name")
                policy = arguments.get("policy", {})
                if not tbl:
                    return [TextContent(type="text", text="Error: 'table_name' is required")]
                result = set_maintenance_policy(tbl, policy)
                p = result["policy"]
                lines = [
                    f"**{result['message']}**\n",
                    f"- Compact threshold: {p['auto_compact_threshold']} files",
                    f"- Retain last: {p['auto_expire_retain_last']} snapshots",
                    f"- Expire older than: {p.get('auto_expire_older_than') or 'not set'}",
                    f"- Auto cleanup orphans: {p['auto_cleanup_orphans']}",
                ]
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Set policy failed: {str(e)}")]

        elif name == "run_maintenance":
            try:
                tbl = arguments.get("table_name")
                dry_run = arguments.get("dry_run", False)
                catalog = get_catalog()
                actions = run_maintenance(catalog, table_name=tbl, dry_run=dry_run)

                if not actions:
                    return [TextContent(type="text", text="No maintenance actions needed.")]

                mode = "(dry run) " if dry_run else ""
                lines = [f"**{mode}Maintenance actions ({len(actions)}):**\n"]
                for a in actions:
                    lines.append(f"- **{a['table']}** → {a['action']}: {a['detail']} [{a['status']}]")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Run maintenance failed: {str(e)}")]

        elif name == "check_maintenance":
            try:
                tbl = arguments.get("table_name")
                if not tbl:
                    return [TextContent(type="text", text="Error: 'table_name' is required")]
                catalog = get_catalog()
                check = check_maintenance_needed(catalog, tbl)

                if not check["has_policy"]:
                    return [TextContent(type="text", text=f"No maintenance policy for {check['table']}")]

                lines = [f"**{check['table']}**: {check['message']}\n"]
                lines.append(f"- Data files: {check['data_files']}")
                lines.append(f"- Snapshots: {check['snapshots']}")
                lines.append(f"- Orphan files: {check['orphan_files']}")
                if check["actions_needed"]:
                    lines.append("\n**Actions needed:**")
                    for action in check["actions_needed"]:
                        lines.append(f"- {action}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Check maintenance failed: {str(e)}")]

        elif name == "create_view":
            try:
                view_name = arguments.get("name")
                view_sql = arguments.get("sql")
                view_desc = arguments.get("description", "")
                if not view_name or not view_sql:
                    return [TextContent(type="text", text="Error: 'name' and 'sql' are required")]
                result = create_view(view_name, view_sql, description=view_desc)
                return [TextContent(type="text", text=f"**{result['message']}**\n\nSQL: `{result['sql']}`")]
            except ValueError as e:
                return [TextContent(type="text", text=f"Error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Create view failed: {str(e)}")]

        elif name == "list_views":
            try:
                views = list_views()
                if not views:
                    return [TextContent(type="text", text="No views defined.")]
                lines = [f"**Views ({len(views)}):**\n"]
                for v in views:
                    desc = f" — {v['description']}" if v.get("description") else ""
                    lines.append(f"- **{v['name']}**: `{v['sql']}`{desc}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"List views failed: {str(e)}")]

        elif name == "query_view":
            try:
                view_name = arguments.get("name")
                max_rows = arguments.get("max_rows", 1000)
                if not view_name:
                    return [TextContent(type="text", text="Error: 'name' is required")]
                engine = get_engine()
                df = query_view(view_name, engine, max_rows=max_rows)
                if df.empty:
                    return [TextContent(type="text", text="View returned no results.")]
                return [TextContent(type="text", text=df.to_markdown(index=False))]
            except ValueError as e:
                return [TextContent(type="text", text=f"Error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Query view failed: {str(e)}")]

        elif name == "drop_view":
            try:
                view_name = arguments.get("name")
                if not view_name:
                    return [TextContent(type="text", text="Error: 'name' is required")]
                result = drop_view(view_name)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except ValueError as e:
                return [TextContent(type="text", text=f"Error: {str(e)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Drop view failed: {str(e)}")]

        elif name == "tag_table":
            try:
                tbl = arguments.get("table_name")
                tags = arguments.get("tags", [])
                remove = arguments.get("remove", False)
                if not tbl or not tags:
                    return [TextContent(type="text", text="Error: 'table_name' and 'tags' are required")]
                if remove:
                    result = untag_table(tbl, tags)
                else:
                    result = tag_table(tbl, tags)
                return [TextContent(type="text", text=f"**{result['message']}**\n\nTags: {', '.join(result['tags'])}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Tag operation failed: {str(e)}")]

        elif name == "search_by_tag":
            try:
                tag = arguments.get("tag")
                if not tag:
                    return [TextContent(type="text", text="Error: 'tag' is required")]
                tables = search_by_tag(tag)
                if not tables:
                    return [TextContent(type="text", text=f"No tables tagged '{tag}'")]
                lines = [f"**Tables tagged '{tag}' ({len(tables)}):**\n"]
                for t in tables:
                    lines.append(f"- {t}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Search by tag failed: {str(e)}")]

        elif name == "set_table_description":
            try:
                tbl = arguments.get("table_name")
                desc = arguments.get("description", "")
                if not tbl:
                    return [TextContent(type="text", text="Error: 'table_name' is required")]
                result = set_table_description(tbl, desc)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Set description failed: {str(e)}")]

        elif name == "bookmark_table":
            try:
                tbl = arguments.get("table_name")
                remove = arguments.get("remove", False)
                if not tbl:
                    return [TextContent(type="text", text="Error: 'table_name' is required")]
                if remove:
                    result = unbookmark_table(tbl)
                else:
                    result = bookmark_table(tbl)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Bookmark operation failed: {str(e)}")]

        elif name == "search_tables":
            try:
                q = arguments.get("query")
                if not q:
                    return [TextContent(type="text", text="Error: 'query' is required")]
                catalog = get_catalog()
                results = search_tables(q, catalog=catalog)
                if not results:
                    return [TextContent(type="text", text=f"No tables matching '{q}'")]
                lines = [f"**Search results for '{q}' ({len(results)}):**\n"]
                for r in results:
                    tags = f" [{', '.join(r['tags'])}]" if r['tags'] else ""
                    bmark = " ★" if r['bookmarked'] else ""
                    desc = f" — {r['description']}" if r['description'] else ""
                    lines.append(f"- **{r['table']}**{tags}{bmark}{desc}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Search failed: {str(e)}")]

        elif name == "create_materialized_view":
            try:
                mv_name = arguments.get("name")
                sql = arguments.get("sql")
                desc = arguments.get("description", "")
                if not mv_name or not sql:
                    return [TextContent(type="text", text="Error: 'name' and 'sql' are required")]
                catalog = get_catalog()
                engine = get_engine()
                result = create_materialized_view(mv_name, sql, engine, catalog, description=desc)
                return [TextContent(type="text", text=f"**{result['message']}**\n\nBacking table: {result['backing_table']}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Create materialized view failed: {str(e)}")]

        elif name == "list_materialized_views":
            try:
                views = list_materialized_views()
                if not views:
                    return [TextContent(type="text", text="No materialized views.")]
                lines = [f"**Materialized Views ({len(views)}):**\n"]
                for v in views:
                    lines.append(f"- **{v['name']}** ({v['row_count']} rows, refreshed {v['last_refreshed'][:19]})")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"List materialized views failed: {str(e)}")]

        elif name == "query_materialized_view":
            try:
                mv_name = arguments.get("name")
                max_rows = arguments.get("max_rows", 1000)
                if not mv_name:
                    return [TextContent(type="text", text="Error: 'name' is required")]
                engine = get_engine()
                df = query_materialized_view(mv_name, engine, max_rows=max_rows)
                if df.empty:
                    return [TextContent(type="text", text="View returned no results.")]
                markdown = df.to_markdown(index=False)
                return [TextContent(type="text", text=f"**{mv_name} ({len(df)} rows):**\n\n{markdown}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Query materialized view failed: {str(e)}")]

        elif name == "refresh_materialized_view":
            try:
                mv_name = arguments.get("name")
                if not mv_name:
                    return [TextContent(type="text", text="Error: 'name' is required")]
                catalog = get_catalog()
                engine = get_engine()
                result = refresh_materialized_view(mv_name, engine, catalog)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Refresh materialized view failed: {str(e)}")]

        elif name == "drop_materialized_view":
            try:
                mv_name = arguments.get("name")
                if not mv_name:
                    return [TextContent(type="text", text="Error: 'name' is required")]
                catalog = get_catalog()
                result = drop_materialized_view(mv_name, catalog)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Drop materialized view failed: {str(e)}")]

        elif name == "execute_join":
            try:
                sql = arguments.get("sql")
                max_rows = arguments.get("max_rows", 1000)
                if not sql:
                    return [TextContent(type="text", text="Error: 'sql' is required")]
                catalog = get_catalog()
                result = execute_join(catalog, sql, max_rows=max_rows)
                df = result["dataframe"]
                if df.empty:
                    return [TextContent(type="text", text="Join returned no results.")]
                markdown = df.to_markdown(index=False)
                return [TextContent(type="text", text=f"**Join results ({result['row_count']} rows):**\n\n{markdown}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Join failed: {str(e)}")]

        elif name == "join_to_table":
            try:
                sql = arguments.get("sql")
                target = arguments.get("target_table")
                mode = arguments.get("mode", "overwrite")
                if not sql or not target:
                    return [TextContent(type="text", text="Error: 'sql' and 'target_table' are required")]
                catalog = get_catalog()
                result = join_to_table(catalog, sql, target, mode=mode)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Join to table failed: {str(e)}")]

        elif name == "suggest_joins":
            try:
                tbl = arguments.get("table_name")
                if not tbl:
                    return [TextContent(type="text", text="Error: 'table_name' is required")]
                catalog = get_catalog()
                suggestions = suggest_joins(catalog, tbl)
                if not suggestions:
                    return [TextContent(type="text", text=f"No join suggestions for {tbl}")]
                lines = [f"**Join suggestions for {tbl} ({len(suggestions)}):**\n"]
                for s in suggestions:
                    lines.append(f"- **{s['table']}** on `{s['column']}` ({s['source_type']} = {s['target_type']})")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Suggest joins failed: {str(e)}")]

        elif name == "clone_table":
            try:
                src = arguments.get("source_table")
                tgt = arguments.get("target_table")
                as_of = arguments.get("as_of")
                if not src or not tgt:
                    return [TextContent(type="text", text="Error: 'source_table' and 'target_table' are required")]
                catalog = get_catalog()
                result = clone_table(catalog, src, tgt, as_of=as_of)
                return [TextContent(type="text", text=f"**{result['message']}**\n\nSource snapshot: {result['source_snapshot_id']}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Clone failed: {str(e)}")]

        elif name == "list_clones":
            try:
                clones = list_clones()
                if not clones:
                    return [TextContent(type="text", text="No active clones.")]
                lines = [f"**Active Clones ({len(clones)}):**\n"]
                for c in clones:
                    as_of_info = f" (as of {c['as_of']})" if c.get("as_of") else ""
                    lines.append(f"- **{c['clone']}** ← {c['source_table']} ({c['row_count']} rows){as_of_info}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"List clones failed: {str(e)}")]

        elif name == "promote_clone":
            try:
                clone_name = arguments.get("clone_table")
                original = arguments.get("original_table")
                if not clone_name or not original:
                    return [TextContent(type="text", text="Error: 'clone_table' and 'original_table' are required")]
                catalog = get_catalog()
                result = promote_clone(catalog, clone_name, original)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Promote clone failed: {str(e)}")]

        elif name == "discard_clone":
            try:
                clone_name = arguments.get("clone_table")
                if not clone_name:
                    return [TextContent(type="text", text="Error: 'clone_table' is required")]
                catalog = get_catalog()
                result = discard_clone(catalog, clone_name)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Discard clone failed: {str(e)}")]

        elif name == "record_lineage":
            try:
                sources = arguments.get("source_tables", [])
                target = arguments.get("target_table")
                operation = arguments.get("operation", "manual")
                sql = arguments.get("sql")
                if not sources or not target:
                    return [TextContent(type="text", text="Error: 'source_tables' and 'target_table' are required")]
                result = record_lineage(sources, target, operation=operation, sql=sql)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Record lineage failed: {str(e)}")]

        elif name == "get_lineage":
            try:
                tbl = arguments.get("table_name")
                direction = arguments.get("direction", "upstream")
                if not tbl:
                    return [TextContent(type="text", text="Error: 'table_name' is required")]

                if direction == "impact":
                    result = get_impact_analysis(tbl)
                    lines = [f"**{result['message']}**\n"]
                    for d in result["details"]:
                        lines.append(f"- {d['table']} (depth {d['depth']}, via {d['operation']})")
                    return [TextContent(type="text", text="\n".join(lines))]

                if direction == "downstream":
                    deps = get_downstream(tbl)
                else:
                    deps = get_upstream(tbl)

                if not deps:
                    return [TextContent(type="text", text=f"No {direction} dependencies for {tbl}")]
                lines = [f"**{direction.title()} dependencies for {tbl} ({len(deps)}):**\n"]
                for d in deps:
                    lines.append(f"- {d['table']} (depth {d['depth']}, via {d['operation']})")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Get lineage failed: {str(e)}")]

        elif name == "lineage_graph":
            try:
                graph = get_lineage_graph()
                if not graph["nodes"]:
                    return [TextContent(type="text", text="No lineage data recorded.")]
                lines = [f"**Lineage Graph ({graph['node_count']} tables, {graph['edge_count']} edges):**\n"]
                for edge in graph["edges"]:
                    sources = ", ".join(edge["sources"])
                    lines.append(f"- {sources} →({edge['operation']})→ {edge['target']}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Lineage graph failed: {str(e)}")]

        elif name == "create_pipeline":
            try:
                from .pipelines import create_pipeline
                pipe_name = arguments.get("name")
                steps = arguments.get("steps", [])
                desc = arguments.get("description", "")
                if not pipe_name or not steps:
                    return [TextContent(type="text", text="Error: 'name' and 'steps' are required")]
                result = create_pipeline(pipe_name, steps, description=desc)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Create pipeline failed: {str(e)}")]

        elif name == "list_pipelines":
            try:
                from .pipelines import list_pipelines
                pipelines = list_pipelines()
                if not pipelines:
                    return [TextContent(type="text", text="No pipelines defined.")]
                lines = [f"**Pipelines ({len(pipelines)}):**\n"]
                for p in pipelines:
                    last_run = p["last_run"][:19] if p.get("last_run") else "never"
                    status = p.get("last_run_status") or "-"
                    lines.append(f"- **{p['name']}** ({p['step_count']} steps, last run: {last_run}, status: {status})")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"List pipelines failed: {str(e)}")]

        elif name == "run_pipeline":
            try:
                from .pipelines import run_pipeline
                pipe_name = arguments.get("name")
                dry_run = arguments.get("dry_run", False)
                if not pipe_name:
                    return [TextContent(type="text", text="Error: 'name' is required")]
                catalog = get_catalog()
                engine = get_engine()
                result = run_pipeline(pipe_name, catalog, engine, dry_run=dry_run)
                lines = [f"**{result['message']}**\n"]
                for r in result["step_results"]:
                    rows = r.get("rows_affected", "-")
                    if r["status"] == "error":
                        lines.append(f"- Step {r['step']}: **error** — {r['error']}")
                    else:
                        lines.append(f"- Step {r['step']}: {r['status']} ({rows} rows, {r['duration_ms']}ms)")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Run pipeline failed: {str(e)}")]

        elif name == "drop_pipeline":
            try:
                from .pipelines import drop_pipeline
                pipe_name = arguments.get("name")
                if not pipe_name:
                    return [TextContent(type="text", text="Error: 'name' is required")]
                result = drop_pipeline(pipe_name)
                return [TextContent(type="text", text=f"**{result['message']}**")]
            except Exception as e:
                return [TextContent(type="text", text=f"Drop pipeline failed: {str(e)}")]

        elif name == "quality_score":
            try:
                from .quality import compute_quality_score
                tbl = arguments.get("table_name")
                if not tbl:
                    return [TextContent(type="text", text="Error: 'table_name' is required")]
                catalog = get_catalog()
                result = compute_quality_score(catalog, tbl)
                lines = [
                    f"**{result['message']}**\n",
                    f"| Component | Score |",
                    f"|-----------|-------|",
                    f"| Completeness (30%) | {result['completeness']} |",
                    f"| Uniqueness (25%) | {result['uniqueness']} |",
                    f"| Freshness (20%) | {result['freshness']} |",
                    f"| Rule Compliance (25%) | {result['rule_compliance']} |",
                    f"| **Overall** | **{result['overall_score']}** |",
                ]
                if result["recommendations"]:
                    lines.append("\n**Recommendations:**")
                    for r in result["recommendations"]:
                        lines.append(f"- {r}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Quality score failed: {str(e)}")]

        elif name == "detect_anomalies":
            try:
                from .quality import detect_anomalies
                tbl = arguments.get("table_name")
                if not tbl:
                    return [TextContent(type="text", text="Error: 'table_name' is required")]
                catalog = get_catalog()
                anomalies = detect_anomalies(catalog, tbl)
                if not anomalies:
                    return [TextContent(type="text", text=f"No anomalies detected for {tbl}.")]
                lines = [f"**Anomalies for {tbl} ({len(anomalies)}):**\n"]
                for a in anomalies:
                    col = f" in `{a['column']}`" if a.get("column") else ""
                    lines.append(f"- **{a['severity']}** {a['type']}{col}: {a['description']}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Detect anomalies failed: {str(e)}")]

        elif name == "quality_report":
            try:
                from .quality import get_quality_report
                catalog = get_catalog()
                report = get_quality_report(catalog)
                lines = [f"**Quality Report ({report['total_tables']} tables, avg score: {report['average_score']}):**\n"]
                lines.append("| Table | Score | Complete | Unique | Fresh | Rules | Anomalies |")
                lines.append("|-------|-------|----------|--------|-------|-------|-----------|")
                for t in report["tables"]:
                    if t.get("overall_score") is not None:
                        lines.append(
                            f"| {t['table']} | {t['overall_score']} | {t['completeness']} | "
                            f"{t['uniqueness']} | {t['freshness']} | {t['rule_compliance']} | {t['anomalies']} |"
                        )
                    else:
                        lines.append(f"| {t['table']} | error | - | - | - | - | - |")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Quality report failed: {str(e)}")]

        elif name == "schema_history":
            from .schema_evolution import get_schema_history
            try:
                catalog = get_catalog()
                history = get_schema_history(catalog, arguments["table_name"])
                lines = [f"## Schema History: {arguments['table_name']}\n"]
                for entry in history:
                    fields = ", ".join(f["name"] for f in entry["fields"])
                    change = f" — {entry['change_summary']}" if entry["change_summary"] else ""
                    ts = entry["timestamp"][:19] if entry["timestamp"] else "no snapshot"
                    lines.append(f"- **Schema {entry['schema_id']}** ({ts}): {fields}{change}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Schema history failed: {str(e)}")]

        elif name == "schema_diff":
            from .schema_evolution import schema_diff as _schema_diff
            try:
                catalog = get_catalog()
                diff = _schema_diff(
                    catalog, arguments["table_name"],
                    from_snapshot=arguments.get("from_snapshot"),
                    to_snapshot=arguments.get("to_snapshot"),
                )
                lines = [
                    f"## Schema Diff: {diff['table']}\n",
                    f"From schema {diff['from_schema_id']} → {diff['to_schema_id']}",
                    f"Summary: {diff['summary']}\n",
                ]
                for c in diff["added_columns"]:
                    lines.append(f"+ Added: {c['name']} ({c['type']})")
                for c in diff["dropped_columns"]:
                    lines.append(f"- Dropped: {c['name']} ({c['type']})")
                for c in diff["renamed_columns"]:
                    lines.append(f"~ Renamed: {c['old_name']} → {c['new_name']}")
                for c in diff["type_changes"]:
                    lines.append(f"~ Type changed: {c['name']}: {c['old_type']} → {c['new_type']}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Schema diff failed: {str(e)}")]

        elif name == "schema_migration":
            from .schema_evolution import generate_migration
            try:
                catalog = get_catalog()
                result = generate_migration(
                    catalog, arguments["table_name"],
                    from_snapshot=arguments.get("from_snapshot"),
                    to_snapshot=arguments.get("to_snapshot"),
                )
                lines = [f"## {result['message']}\n"]
                for i, step in enumerate(result["steps"], 1):
                    details = ""
                    if step["operation"] == "add_column":
                        details = f" (type: {step['column_type']})"
                    elif step["operation"] == "rename_column":
                        details = f" → {step['new_name']}"
                    lines.append(f"{i}. {step['operation']} `{step['column_name']}`{details}")
                if not result["steps"]:
                    lines.append("No migration steps needed.")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Schema migration failed: {str(e)}")]

        elif name == "schema_compatibility":
            from .schema_evolution import check_schema_compatibility
            try:
                catalog = get_catalog()
                result = check_schema_compatibility(
                    catalog, arguments["table_name"], arguments["changes"],
                )
                status = "Compatible" if result["compatible"] else "NOT Compatible"
                lines = [f"## Schema Compatibility: {status}\n", result["message"], ""]
                if result["breaking_changes"]:
                    lines.append("**Breaking changes:**")
                    for bc in result["breaking_changes"]:
                        lines.append(f"- {bc}")
                if result["warnings"]:
                    lines.append("**Warnings:**")
                    for w in result["warnings"]:
                        lines.append(f"- {w}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Schema compatibility check failed: {str(e)}")]

        elif name == "set_retention_policy":
            from .retention import set_retention_policy as _set_retention
            try:
                policy = {}
                if "max_snapshot_age_hours" in arguments:
                    policy["max_snapshot_age_hours"] = arguments["max_snapshot_age_hours"]
                if "max_snapshot_count" in arguments:
                    policy["max_snapshot_count"] = arguments["max_snapshot_count"]
                if "min_snapshots_to_keep" in arguments:
                    policy["min_snapshots_to_keep"] = arguments["min_snapshots_to_keep"]
                result = _set_retention(arguments["table_name"], policy)
                return [TextContent(type="text", text=result["message"])]
            except Exception as e:
                return [TextContent(type="text", text=f"Set retention policy failed: {str(e)}")]

        elif name == "list_retention_policies":
            from .retention import list_retention_policies as _list_retention
            try:
                policies = _list_retention()
                if not policies:
                    return [TextContent(type="text", text="No retention policies configured.")]
                lines = ["## Retention Policies\n"]
                lines.append("| Table | Max Age (hrs) | Max Count | Min Keep | Last Evaluated |")
                lines.append("|-------|---------------|-----------|----------|----------------|")
                for p in policies:
                    lines.append(
                        f"| {p['table']} | {p.get('max_snapshot_age_hours') or '-'} | "
                        f"{p.get('max_snapshot_count') or '-'} | {p.get('min_snapshots_to_keep', 1)} | "
                        f"{p.get('last_evaluated') or 'never'} |"
                    )
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"List retention policies failed: {str(e)}")]

        elif name == "evaluate_retention":
            from .retention import evaluate_retention as _eval_retention
            try:
                catalog = get_catalog()
                results = _eval_retention(
                    catalog,
                    table_name=arguments.get("table_name"),
                    dry_run=arguments.get("dry_run", False),
                )
                if not results:
                    return [TextContent(type="text", text="No tables with retention policies to evaluate.")]
                lines = ["## Retention Evaluation\n"]
                for r in results:
                    lines.append(f"- **{r['table']}**: {r['message']}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Evaluate retention failed: {str(e)}")]

        elif name == "set_column_description":
            from .catalog_metadata import set_column_description as _set_desc
            try:
                result = _set_desc(arguments["table_name"], arguments["column_name"], arguments["description"])
                return [TextContent(type="text", text=result["message"])]
            except Exception as e:
                return [TextContent(type="text", text=f"Set column description failed: {str(e)}")]

        elif name == "classify_column":
            from .catalog_metadata import classify_column as _classify
            try:
                result = _classify(arguments["table_name"], arguments["column_name"], arguments["classification"])
                return [TextContent(type="text", text=result["message"])]
            except Exception as e:
                return [TextContent(type="text", text=f"Classify column failed: {str(e)}")]

        elif name == "get_enriched_schema":
            from .catalog_metadata import get_enriched_schema as _enriched
            try:
                catalog = get_catalog()
                result = _enriched(catalog, arguments["table_name"])
                lines = [f"## Enriched Schema: {result['table']}\n"]
                lines.append(f"**Fields:** {result['total_fields']} ({result['described_fields']} described, {result['classified_fields']} classified)\n")
                lines.append("| Field | Type | Description | Classification | Glossary |")
                lines.append("|-------|------|-------------|----------------|----------|")
                for f in result["fields"]:
                    lines.append(
                        f"| {f['name']} | {f['type']} | {f['description'] or '-'} | "
                        f"{f['classification'] or '-'} | {', '.join(f['glossary_matches']) or '-'} |"
                    )
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Get enriched schema failed: {str(e)}")]

        elif name == "search_glossary":
            from .catalog_metadata import search_glossary as _search_gl
            try:
                results = _search_gl(arguments["query"])
                if not results:
                    return [TextContent(type="text", text="No matching glossary terms found.")]
                lines = ["## Glossary Search Results\n"]
                for r in results:
                    aliases = f" (aliases: {', '.join(r['aliases'])})" if r["aliases"] else ""
                    lines.append(f"- **{r['term']}**: {r['definition']}{aliases}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Search glossary failed: {str(e)}")]

        elif name == "add_masking_policy":
            from .masking import add_masking_policy as _add_mask
            try:
                result = _add_mask(
                    arguments["table_name"], arguments["column_name"],
                    arguments["strategy"], options=arguments.get("options"),
                )
                return [TextContent(type="text", text=result["message"])]
            except Exception as e:
                return [TextContent(type="text", text=f"Add masking policy failed: {str(e)}")]

        elif name == "list_masking_policies":
            from .masking import list_masking_policies as _list_mask
            try:
                policies = _list_mask(table_name=arguments.get("table_name"))
                if not policies:
                    return [TextContent(type="text", text="No masking policies configured.")]
                lines = ["## Masking Policies\n"]
                lines.append("| Table | Column | Strategy | Options |")
                lines.append("|-------|--------|----------|---------|")
                for p in policies:
                    opts = ", ".join(f"{k}={v}" for k, v in p["options"].items()) if p["options"] else "-"
                    lines.append(f"| {p['table']} | {p['column']} | {p['strategy']} | {opts} |")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"List masking policies failed: {str(e)}")]

        elif name == "remove_masking_policy":
            from .masking import remove_masking_policy as _remove_mask
            try:
                result = _remove_mask(arguments["table_name"], arguments["column_name"])
                return [TextContent(type="text", text=result["message"])]
            except Exception as e:
                return [TextContent(type="text", text=f"Remove masking policy failed: {str(e)}")]

        elif name == "query_with_masking":
            from .masking import query_with_masking as _mask_query
            from .query import QueryEngine
            try:
                engine = QueryEngine(catalog=get_catalog())
                df = _mask_query(engine, arguments["sql"])
                if df.empty:
                    return [TextContent(type="text", text="Query returned no results.")]
                return [TextContent(type="text", text=df.to_markdown(index=False))]
            except Exception as e:
                return [TextContent(type="text", text=f"Masked query failed: {str(e)}")]

        elif name == "get_watermark":
            from .incremental import get_watermark as _get_wm
            try:
                result = _get_wm(arguments["pipeline_name"], arguments["table_name"])
                return [TextContent(type="text", text=result["message"])]
            except Exception as e:
                return [TextContent(type="text", text=f"Get watermark failed: {str(e)}")]

        elif name == "list_watermarks":
            from .incremental import list_watermarks as _list_wm
            try:
                results = _list_wm(pipeline_name=arguments.get("pipeline_name"))
                if not results:
                    return [TextContent(type="text", text="No watermarks configured.")]
                lines = ["## Watermarks\n"]
                lines.append("| Pipeline | Table | Snapshot | Processed At | Rows |")
                lines.append("|----------|-------|----------|--------------|------|")
                for r in results:
                    lines.append(
                        f"| {r['pipeline']} | {r['table']} | {r['snapshot_id']} | "
                        f"{str(r.get('processed_at', ''))[:19]} | {r.get('rows_processed', 0)} |"
                    )
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"List watermarks failed: {str(e)}")]

        elif name == "reset_watermark":
            from .incremental import reset_watermark as _reset_wm
            try:
                result = _reset_wm(arguments["pipeline_name"], table_name=arguments.get("table_name"))
                return [TextContent(type="text", text=result["message"])]
            except Exception as e:
                return [TextContent(type="text", text=f"Reset watermark failed: {str(e)}")]

        elif name == "run_pipeline_incremental":
            from .incremental import run_pipeline_incremental as _run_inc
            from .query import QueryEngine
            try:
                catalog = get_catalog()
                engine = QueryEngine(catalog=catalog)
                result = _run_inc(arguments["pipeline_name"], catalog, engine)
                lines = [f"## {result['message']}\n"]
                for s in result["steps"]:
                    status = s["status"].upper()
                    lines.append(f"- Step {s['step']}: {status} — {s['message']}")
                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Incremental pipeline failed: {str(e)}")]

        elif name == "dashboard":
            try:
                catalog = get_catalog()
                data = get_dashboard(catalog)

                lines = [
                    "# Lakehouse Dashboard\n",
                    f"**Storage:** {data['storage_path']}",
                    f"**Namespaces:** {len(data['namespaces'])} ({', '.join(data['namespaces'])})",
                    f"**Total tables:** {data['total_tables']}",
                    f"**Total size:** {data['total_size_display']}\n",
                ]

                if data["tables"]:
                    lines.append("## Tables\n")
                    lines.append("| Table | Rows | Size | Files | Health |")
                    lines.append("|-------|------|------|-------|--------|")
                    for t in data["tables"]:
                        lines.append(
                            f"| {t['name']} | {t['rows']} | {t['size_display']} | "
                            f"{t['data_files']} | {t['health']} |"
                        )

                if data["recent_activity"]:
                    lines.append(f"\n## Recent Activity (last {len(data['recent_activity'])})\n")
                    for entry in data["recent_activity"]:
                        ts = entry.get("timestamp", "")[:16]
                        op = entry.get("operation", "").upper()
                        tbl = entry.get("table", "")
                        rows_affected = entry.get("rows_affected", 0)
                        source = entry.get("source", "")
                        row_info = f" ({rows_affected} rows)" if rows_affected else ""
                        lines.append(f"- {ts} {op} {tbl}{row_info} via {source}")

                lines.append(f"\n**Saved Queries:** {data['saved_queries_count']}")
                lines.append(f"**Query History:** {data['history_entries_count']} entries")

                return [TextContent(type="text", text="\n".join(lines))]
            except Exception as e:
                return [TextContent(type="text", text=f"Dashboard failed: {str(e)}")]

        else:
            return [TextContent(
                type="text",
                text=f"Unknown tool: {name}",
            )]

    except Exception as e:
        return [TextContent(
            type="text",
            text=f"Internal error: {str(e)}",
        )]


async def run_server():
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main():
    """Entry point for MCP server."""
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
