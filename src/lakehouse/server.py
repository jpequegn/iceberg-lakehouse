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

from .catalog import get_catalog, list_tables, get_table_schema
from .query import QueryEngine


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
                "Use standard SQL syntax. Results limited to 1000 rows by default."
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
                },
                "required": ["sql"],
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
            engine = get_engine()

            try:
                result = engine.execute(sql, max_rows=max_rows)

                # Format as markdown table for readability
                if result.empty:
                    return [TextContent(type="text", text="Query returned no results.")]

                # Convert to markdown
                markdown = result.to_markdown(index=False)
                row_count = len(result)

                return [TextContent(
                    type="text",
                    text=f"**Results ({row_count} rows):**\n\n{markdown}",
                )]

            except Exception as e:
                return [TextContent(
                    type="text",
                    text=f"Query error: {str(e)}",
                )]

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
