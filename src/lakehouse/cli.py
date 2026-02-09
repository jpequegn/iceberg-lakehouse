"""CLI for Iceberg Lakehouse."""

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


@click.group()
@click.version_option(version="0.1.0")
def main():
    """Iceberg Lakehouse - Local-first data lakehouse with LLM access."""
    pass


@main.command()
@click.option("--with-sample-data", is_flag=True, help="Insert sample data")
def init(with_sample_data: bool):
    """Initialize the lakehouse (create catalog and tables)."""
    from .catalog import (
        get_catalog,
        init_catalog,
        create_sample_tables,
        insert_sample_data,
    )

    console.print("[bold blue]Initializing Iceberg Lakehouse...[/bold blue]")

    # Get/create catalog
    catalog = get_catalog()
    console.print("  ✓ Catalog created")

    # Initialize namespace
    init_catalog(catalog)
    console.print("  ✓ Namespace 'default' created")

    # Create sample tables
    create_sample_tables(catalog)
    console.print("  ✓ Sample tables created (expenses, health, notes)")

    if with_sample_data:
        insert_sample_data(catalog)
        console.print("  ✓ Sample data inserted")

    console.print("\n[bold green]✓ Lakehouse initialized successfully![/bold green]")
    console.print("\nNext steps:")
    console.print("  • Query data: [cyan]lakehouse query 'SELECT * FROM expenses'[/cyan]")
    console.print("  • Start MCP server: [cyan]lakehouse serve[/cyan]")


@main.command()
@click.argument("sql")
@click.option("--max-rows", default=100, help="Maximum rows to return")
@click.option("--format", "output_format", type=click.Choice(["table", "csv", "json"]), default="table")
@click.option("--as-of", default=None, help="Time travel: ISO timestamp or snapshot ID")
@click.option("--table-name", default=None, help="Table name for time travel queries (required with --as-of)")
def query(sql: str, max_rows: int, output_format: str, as_of: str, table_name: str):
    """Execute a SQL query against the lakehouse."""
    from .query import QueryEngine

    engine = QueryEngine()

    try:
        if as_of:
            if not table_name:
                console.print("[bold red]Error:[/bold red] --table-name is required with --as-of")
                raise click.Abort()
            result = engine.execute_as_of(sql, table_name, as_of, max_rows=max_rows)
        else:
            result = engine.execute(sql, max_rows=max_rows)

        if result.empty:
            console.print("[yellow]Query returned no results.[/yellow]")
            return

        if output_format == "table":
            table = Table(show_header=True, header_style="bold magenta")
            for col in result.columns:
                table.add_column(str(col))
            for _, row in result.iterrows():
                table.add_row(*[str(v) for v in row])
            console.print(table)

        elif output_format == "csv":
            print(result.to_csv(index=False))

        elif output_format == "json":
            print(result.to_json(orient="records", indent=2))

        label = f"({len(result)} rows)"
        if as_of:
            label = f"({len(result)} rows, as of {as_of})"
        console.print(f"\n[dim]{label}[/dim]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name")
def snapshots(table_name: str):
    """List all snapshots for a table (for time travel queries)."""
    from .catalog import get_catalog, get_snapshots

    catalog = get_catalog()

    try:
        snaps = get_snapshots(catalog, table_name)

        if not snaps:
            console.print(f"[yellow]No snapshots found for {table_name}.[/yellow]")
            return

        console.print(f"[bold]Snapshots for {table_name} ({len(snaps)}):[/bold]\n")
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Snapshot ID")
        table.add_column("Timestamp")
        table.add_column("Operation")
        table.add_column("Parent ID")

        for s in snaps:
            table.add_row(
                str(s["snapshot_id"]),
                s["timestamp"],
                s["operation"] or "",
                str(s["parent_id"]) if s["parent_id"] else "",
            )

        console.print(table)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
def tables():
    """List all tables in the lakehouse."""
    from .catalog import get_catalog, list_tables

    catalog = get_catalog()
    table_list = list_tables(catalog)

    if not table_list:
        console.print("[yellow]No tables found. Run 'lakehouse init' first.[/yellow]")
        return

    console.print("[bold]Available Tables:[/bold]\n")
    for t in table_list:
        console.print(f"  • {t}")


@main.command()
@click.argument("table_name")
def describe(table_name: str):
    """Describe a table's schema."""
    from .catalog import get_catalog, get_table_schema

    catalog = get_catalog()

    try:
        schema = get_table_schema(catalog, table_name)

        console.print(Panel(f"[bold]{schema['name']}[/bold]"))

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Field")
        table.add_column("Type")
        table.add_column("Required")

        for field in schema["fields"]:
            table.add_row(
                field["name"],
                field["type"],
                "✓" if field["required"] else "",
            )

        console.print(table)
        console.print(f"\nPartition: {schema['partition_spec']}")
        console.print(f"Snapshots: {schema['snapshots']}")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.option("--host", default="localhost", help="Host to bind to")
@click.option("--port", default=8765, help="Port for SSE transport (not used for stdio)")
def serve(host: str, port: int):
    """Start the MCP server for LLM access."""
    from .server import main as server_main

    console.print("[bold blue]Starting MCP Server...[/bold blue]")
    console.print("Transport: stdio (for Claude Desktop)")
    console.print("\nAdd to Claude Desktop config:")
    console.print(Panel('''{
  "mcpServers": {
    "lakehouse": {
      "command": "uv",
      "args": ["--directory", "/path/to/iceberg-lakehouse", "run", "lakehouse", "serve"]
    }
  }
}''', title="claude_desktop_config.json"))

    # Run the server
    server_main()


@main.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.argument("table_name")
@click.option("--format", "file_format", type=click.Choice(["csv", "json", "parquet"]), default="csv")
def ingest(file_path: str, table_name: str, file_format: str):
    """Ingest data from a file into an Iceberg table."""
    import pandas as pd
    import pyarrow as pa
    from .catalog import get_catalog

    console.print(f"[bold blue]Ingesting {file_path} into {table_name}...[/bold blue]")

    # Read file
    if file_format == "csv":
        df = pd.read_csv(file_path)
    elif file_format == "json":
        df = pd.read_json(file_path)
    elif file_format == "parquet":
        df = pd.read_parquet(file_path)

    # Convert to Arrow
    arrow_table = pa.Table.from_pandas(df)

    # Get catalog and table
    catalog = get_catalog()

    if "." not in table_name:
        table_name = f"default.{table_name}"

    try:
        table = catalog.load_table(table_name)
        table.append(arrow_table)
        console.print(f"[bold green]✓ Ingested {len(df)} rows into {table_name}[/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name")
@click.option("--snapshot-id", type=int, default=None, help="Snapshot ID to rollback to")
@click.option("--timestamp", default=None, help="ISO timestamp to rollback to")
def rollback(table_name: str, snapshot_id: int, timestamp: str):
    """Rollback a table to a previous snapshot.

    Use 'lakehouse snapshots TABLE' to list available snapshots first.
    """
    from .catalog import get_catalog, rollback_table

    if not snapshot_id and not timestamp:
        console.print("[bold red]Error:[/bold red] Either --snapshot-id or --timestamp is required")
        raise click.Abort()

    catalog = get_catalog()

    try:
        result = rollback_table(catalog, table_name, snapshot_id=snapshot_id, timestamp=timestamp)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name")
@click.option("--older-than", default=None, help="Expire snapshots older than this (ISO timestamp or duration like '30d')")
@click.option("--retain-last", type=int, default=None, help="Minimum snapshots to keep")
def expire(table_name: str, older_than: str, retain_last: int):
    """Expire old snapshots to free storage.

    Examples:
        lakehouse expire expenses --older-than 30d
        lakehouse expire expenses --retain-last 5
        lakehouse expire expenses --older-than 7d --retain-last 3
    """
    from .catalog import get_catalog, expire_snapshots

    if not older_than and not retain_last:
        console.print("[bold red]Error:[/bold red] Either --older-than or --retain-last is required")
        raise click.Abort()

    catalog = get_catalog()

    try:
        result = expire_snapshots(catalog, table_name, older_than=older_than, retain_last=retain_last)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("json_operations")
def batch(json_operations: str):
    """Execute multiple operations as a batch.

    JSON_OPERATIONS is a JSON array of operation objects. Each needs:
    action (insert/update/delete), table_name, and action-specific fields.

    Example:
        lakehouse batch '[{"action":"insert","table_name":"expenses","rows":[{"id":10,"amount":50}]},{"action":"delete","table_name":"expenses","filter":"id = 3"}]'
    """
    import json
    from .catalog import get_catalog, execute_batch

    try:
        operations = json.loads(json_operations)
    except json.JSONDecodeError as e:
        console.print(f"[bold red]Error:[/bold red] Invalid JSON: {e}")
        raise click.Abort()

    if not isinstance(operations, list) or not operations:
        console.print("[bold red]Error:[/bold red] JSON must be a non-empty array of operations")
        raise click.Abort()

    catalog = get_catalog()

    try:
        results = execute_batch(catalog, operations)

        ok_count = sum(1 for r in results if r["status"] == "ok")
        err_count = sum(1 for r in results if r["status"] == "error")
        skip_count = sum(1 for r in results if r["status"] == "skipped")

        console.print(f"\n[bold]Batch: {ok_count} succeeded, {err_count} failed, {skip_count} skipped[/bold]\n")

        for r in results:
            if r["status"] == "ok":
                console.print(f"  [green]✓[/green] [{r['index']}] {r['action']} on {r['table']}: {r['rows_affected']} rows")
            elif r["status"] == "error":
                console.print(f"  [red]✗[/red] [{r['index']}] {r.get('action', '?')}: {r['message']}")
            else:
                console.print(f"  [dim]⊘[/dim] [{r['index']}] {r['message']}")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.group()
@click.argument("table_name")
@click.pass_context
def alter(ctx, table_name: str):
    """Alter a table's schema (add/drop/rename columns).

    TABLE_NAME is the target table (e.g., 'expenses').
    """
    ctx.ensure_object(dict)
    ctx.obj["table_name"] = table_name


@alter.command("add-column")
@click.argument("column_name")
@click.argument("column_type")
@click.pass_context
def alter_add_column(ctx, column_name: str, column_type: str):
    """Add a column to the table.

    COLUMN_NAME is the new column name.
    COLUMN_TYPE is the data type (string, long, double, date, timestamp).
    """
    from .catalog import get_catalog, alter_table

    table_name = ctx.obj["table_name"]
    catalog = get_catalog()

    try:
        msg = alter_table(catalog, table_name, "add_column", column_name, column_type=column_type)
        console.print(f"[bold green]✓ {msg}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@alter.command("drop-column")
@click.argument("column_name")
@click.pass_context
def alter_drop_column(ctx, column_name: str):
    """Drop a column from the table.

    COLUMN_NAME is the column to remove.
    """
    from .catalog import get_catalog, alter_table

    table_name = ctx.obj["table_name"]
    catalog = get_catalog()

    try:
        msg = alter_table(catalog, table_name, "drop_column", column_name)
        console.print(f"[bold green]✓ {msg}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@alter.command("rename-column")
@click.argument("column_name")
@click.argument("new_name")
@click.pass_context
def alter_rename_column(ctx, column_name: str, new_name: str):
    """Rename a column in the table.

    COLUMN_NAME is the current column name.
    NEW_NAME is the new column name.
    """
    from .catalog import get_catalog, alter_table

    table_name = ctx.obj["table_name"]
    catalog = get_catalog()

    try:
        msg = alter_table(catalog, table_name, "rename_column", column_name, new_name=new_name)
        console.print(f"[bold green]✓ {msg}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name")
@click.argument("key_columns")
@click.argument("json_data")
def upsert(table_name: str, key_columns: str, json_data: str):
    """Upsert rows into a table (insert or update on key match).

    TABLE_NAME is the target table (e.g., 'expenses').
    KEY_COLUMNS is a comma-separated list of key columns (e.g., 'id' or 'id,date').
    JSON_DATA is a JSON array of row objects.
    """
    import json
    from .catalog import get_catalog, upsert_rows

    # Parse key columns
    keys = [k.strip() for k in key_columns.split(",") if k.strip()]
    if not keys:
        console.print("[bold red]Error:[/bold red] key_columns must not be empty")
        raise click.Abort()

    # Parse JSON data
    try:
        rows = json.loads(json_data)
    except json.JSONDecodeError as e:
        console.print(f"[bold red]Error:[/bold red] Invalid JSON: {e}")
        raise click.Abort()

    if not isinstance(rows, list) or not rows:
        console.print("[bold red]Error:[/bold red] JSON_DATA must be a non-empty array of objects")
        raise click.Abort()

    catalog = get_catalog()

    try:
        result = upsert_rows(catalog, table_name, keys, rows)
        console.print(
            f"[bold green]✓ Upsert into {table_name}: "
            f"{result['inserted']} inserted, {result['updated']} updated.[/bold green]"
        )
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name")
@click.argument("filter_expr")
@click.option("--force", is_flag=True, help="Skip confirmation prompt")
def delete(table_name: str, filter_expr: str, force: bool):
    """Delete rows from a table matching a filter.

    TABLE_NAME is the table to delete from (e.g., 'expenses').
    FILTER_EXPR is a SQL WHERE clause (e.g., "id = 5").
    """
    from .catalog import get_catalog, delete_rows

    catalog = get_catalog()

    if not force:
        # Show matching rows first
        from .query import execute_query

        if "." not in table_name:
            qualified = f"default.{table_name}"
        else:
            qualified = table_name

        short_name = table_name.split(".")[-1] if "." in table_name else table_name

        try:
            preview = execute_query(f"SELECT * FROM {short_name} WHERE {filter_expr}", max_rows=20)
            if preview.empty:
                console.print("[yellow]No rows match the filter. Nothing to delete.[/yellow]")
                return

            console.print(f"[bold]Rows to delete from {qualified}:[/bold]\n")
            table = Table(show_header=True, header_style="bold red")
            for col in preview.columns:
                table.add_column(str(col))
            for _, row in preview.iterrows():
                table.add_row(*[str(v) for v in row])
            console.print(table)
            console.print(f"\n[bold red]{len(preview)} row(s) will be deleted.[/bold red]")

        except Exception as e:
            console.print(f"[yellow]Could not preview rows: {e}[/yellow]")

        if not click.confirm("Proceed with deletion?"):
            console.print("[dim]Aborted.[/dim]")
            return

    try:
        row_count = delete_rows(catalog, table_name, filter_expr)

        if row_count == 0:
            console.print("[yellow]No rows matched the filter.[/yellow]")
        else:
            console.print(f"[bold green]✓ Deleted {row_count} row(s) from {table_name}.[/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.option("--to", "target_format", type=click.Choice(["vortex", "parquet"]), required=True, help="Target format")
@click.option("--output", "-o", default=None, help="Output file path (default: same name with new extension)")
@click.option("--compact", is_flag=True, help="Optimize for smaller file size (Vortex only)")
def convert(input_path: str, target_format: str, output: str, compact: bool):
    """Convert a file between Parquet and Vortex formats.

    Examples:
        lakehouse convert data.parquet --to vortex
        lakehouse convert data.vortex --to parquet
        lakehouse convert data.parquet --to vortex --compact -o out.vortex
    """
    from .vortex_io import convert_parquet_to_vortex, convert_vortex_to_parquet

    try:
        if target_format == "vortex":
            result = convert_parquet_to_vortex(input_path, output, compact=compact)
        else:
            result = convert_vortex_to_parquet(input_path, output)

        console.print(f"[bold green]✓ Converted {result['input']}[/bold green]")
        console.print(f"  Output: {result['output']}")
        console.print(f"  Rows: {result['rows']:,}")
        console.print(f"  Input size:  {result['input_size']:,} bytes")
        console.print(f"  Output size: {result['output_size']:,} bytes")

        ratio = result['output_size'] / result['input_size'] if result['input_size'] > 0 else 0
        console.print(f"  Size ratio:  {ratio:.2f}x")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("query-vortex")
@click.argument("vortex_path", type=click.Path(exists=True))
@click.argument("sql")
@click.option("--max-rows", default=100, help="Maximum rows to return")
@click.option("--table-name", default="data", help="Table name to use in SQL (default: data)")
@click.option("--format", "output_format", type=click.Choice(["table", "csv", "json"]), default="table")
def query_vortex(vortex_path: str, sql: str, max_rows: int, table_name: str, output_format: str):
    """Execute a SQL query against a Vortex file.

    Examples:
        lakehouse query-vortex data.vortex "SELECT * FROM data"
        lakehouse query-vortex data.vortex "SELECT count(*) FROM data" --table-name data
    """
    from .query import QueryEngine

    engine = QueryEngine()

    try:
        result = engine.query_vortex(sql, vortex_path, table_name=table_name, max_rows=max_rows)

        if result.empty:
            console.print("[yellow]Query returned no results.[/yellow]")
            return

        if output_format == "table":
            table = Table(show_header=True, header_style="bold magenta")
            for col in result.columns:
                table.add_column(str(col))
            for _, row in result.iterrows():
                table.add_row(*[str(v) for v in row])
            console.print(table)

        elif output_format == "csv":
            print(result.to_csv(index=False))

        elif output_format == "json":
            print(result.to_json(orient="records", indent=2))

        console.print(f"\n[dim]({len(result)} rows)[/dim]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("convert-table")
@click.argument("table_name")
@click.option("--output-dir", "-o", default=".", help="Output directory for the Vortex file")
@click.option("--compact", is_flag=True, help="Optimize for smaller file size")
def convert_table(table_name: str, output_dir: str, compact: bool):
    """Export an Iceberg table to Vortex format.

    Examples:
        lakehouse convert-table expenses
        lakehouse convert-table expenses -o ./exports --compact
    """
    from .catalog import get_catalog
    from .vortex_io import convert_table_to_vortex

    catalog = get_catalog()

    try:
        result = convert_table_to_vortex(catalog, table_name, output_dir, compact=compact)
        console.print(f"[bold green]✓ Exported {result['table']} to Vortex[/bold green]")
        console.print(f"  Output: {result['output']}")
        console.print(f"  Rows: {result['rows']:,}")
        console.print(f"  Size: {result['size_bytes']:,} bytes")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


if __name__ == "__main__":
    main()
