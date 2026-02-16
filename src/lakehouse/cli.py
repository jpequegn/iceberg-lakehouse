"""CLI for Iceberg Lakehouse."""

import json
from pathlib import Path

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
@click.argument("table_name")
@click.option("--from", "from_snapshot", required=True, help="Snapshot ID or ISO timestamp (older)")
@click.option("--to", "to_snapshot", default=None, help="Snapshot ID or ISO timestamp (newer, default: current)")
@click.option("--summary", is_flag=True, help="Show summary counts only (no row details)")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table", help="Output format")
@click.option("--max-rows", default=50, help="Maximum rows to display per section")
def diff(table_name: str, from_snapshot: str, to_snapshot: str, summary: bool, output_format: str, max_rows: int):
    """Compare two snapshots to see added and deleted rows.

    Examples:
        lakehouse diff expenses --from 12345 --to 67890
        lakehouse diff expenses --from 12345
        lakehouse diff expenses --from 2026-01-01T00:00:00 --summary
        lakehouse diff expenses --from 12345 --format json
    """
    import json as json_mod
    from .catalog import get_catalog, snapshot_diff

    catalog = get_catalog()

    try:
        result = snapshot_diff(catalog, table_name, from_snapshot, to_snapshot)

        if output_format == "json":
            console.print(json_mod.dumps(result, indent=2, default=str))
            return

        from_id = result["from_snapshot_id"]
        to_id = result["to_snapshot_id"]
        console.print(f"\n[bold]Diff for {table_name} (snapshot {from_id} → {to_id}):[/bold]\n")

        s = result["summary"]
        console.print(f"  [green]Added:[/green]    {s['added']} rows")
        console.print(f"  [red]Deleted:[/red]  {s['deleted']} rows")
        console.print(f"  [yellow]Modified:[/yellow] {s['modified']} rows")

        if summary:
            return

        if result["added"]:
            console.print(f"\n[bold green]Added rows{f' (showing first {max_rows})' if len(result['added']) > max_rows else ''}:[/bold green]")
            tbl = Table(show_header=True, header_style="bold green")
            cols = list(result["added"][0].keys())
            for col in cols:
                tbl.add_column(col)
            for row in result["added"][:max_rows]:
                tbl.add_row(*[str(v) for v in row.values()])
            console.print(tbl)

        if result["deleted"]:
            console.print(f"\n[bold red]Deleted rows{f' (showing first {max_rows})' if len(result['deleted']) > max_rows else ''}:[/bold red]")
            tbl = Table(show_header=True, header_style="bold red")
            cols = list(result["deleted"][0].keys())
            for col in cols:
                tbl.add_column(col)
            for row in result["deleted"][:max_rows]:
                tbl.add_row(*[str(v) for v in row.values()])
            console.print(tbl)

        if not result["added"] and not result["deleted"] and not result["modified"]:
            console.print("\n[dim]No changes between these snapshots.[/dim]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.option("--namespace", "-n", default="default", help="Namespace to list tables from (use '*' for all)")
def tables(namespace: str):
    """List all tables in the lakehouse."""
    from .catalog import get_catalog, list_tables

    catalog = get_catalog()
    table_list = list_tables(catalog, namespace=namespace)

    if not table_list:
        if namespace == "*":
            console.print("[yellow]No tables found. Run 'lakehouse init' first.[/yellow]")
        else:
            console.print(f"[yellow]No tables found in namespace '{namespace}'.[/yellow]")
        return

    if namespace == "*":
        console.print("[bold]All Tables:[/bold]\n")
    else:
        console.print(f"[bold]Tables in '{namespace}':[/bold]\n")
    for t in table_list:
        console.print(f"  • {t}")


@main.command("namespaces")
def namespaces_cmd():
    """List all namespaces in the lakehouse."""
    from .catalog import get_catalog, list_namespaces

    catalog = get_catalog()
    ns_list = list_namespaces(catalog)

    if not ns_list:
        console.print("[yellow]No namespaces found.[/yellow]")
        return

    console.print("[bold]Namespaces:[/bold]\n")
    for ns in ns_list:
        console.print(f"  • {ns}")


@main.command("create-namespace")
@click.argument("namespace")
@click.option("--property", "-p", "properties", multiple=True,
              help="Property as 'key=value' (e.g., 'owner=data-team')")
def create_namespace_cmd(namespace: str, properties: tuple):
    """Create a new namespace.

    Examples:
        lakehouse create-namespace staging
        lakehouse create-namespace analytics -p "owner=data-team" -p "env=prod"
    """
    from .catalog import get_catalog, create_namespace

    props = {}
    for prop_str in properties:
        if "=" not in prop_str:
            console.print(f"[bold red]Error:[/bold red] Invalid property '{prop_str}'. Expected 'key=value'.")
            raise click.Abort()
        key, value = prop_str.split("=", 1)
        props[key.strip()] = value.strip()

    catalog = get_catalog()

    try:
        result = create_namespace(catalog, namespace, properties=props if props else None)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("drop-namespace")
@click.argument("namespace")
def drop_namespace_cmd(namespace: str):
    """Drop an empty namespace.

    The namespace must not contain any tables.

    Examples:
        lakehouse drop-namespace staging
    """
    from .catalog import get_catalog, drop_namespace

    if namespace == "default":
        console.print("[bold red]Error:[/bold red] Cannot drop the 'default' namespace.")
        raise click.Abort()

    catalog = get_catalog()

    try:
        result = drop_namespace(catalog, namespace)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


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


@main.group()
def config():
    """Manage lakehouse configuration."""
    pass


@config.command("show")
def config_show():
    """Show current configuration."""
    from .config import get_config_summary

    summary = get_config_summary()

    console.print("[bold]Lakehouse Configuration:[/bold]\n")
    console.print(f"  Default format: [cyan]{summary['default_format']}[/cyan]")

    if summary["table_overrides"]:
        console.print("\n  [bold]Table overrides:[/bold]")
        for table_name, fmt in summary["table_overrides"].items():
            console.print(f"    {table_name}: [cyan]{fmt}[/cyan]")
    else:
        console.print("\n  [dim]No per-table format overrides.[/dim]")


@config.command("set-format")
@click.argument("format_name", type=click.Choice(["parquet", "vortex"]))
@click.option("--table", default=None, help="Set format for a specific table only")
def config_set_format(format_name: str, table: str):
    """Set the default file format (globally or per-table).

    Examples:
        lakehouse config set-format vortex
        lakehouse config set-format parquet --table expenses
    """
    from .config import set_default_format, set_table_format

    try:
        if table:
            set_table_format(table, format_name)
            console.print(f"[bold green]✓ Set format for '{table}' to {format_name}[/bold green]")
        else:
            set_default_format(format_name)
            console.print(f"[bold green]✓ Set default format to {format_name}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@config.command("get-format")
@click.option("--table", default=None, help="Get format for a specific table")
def config_get_format(table: str):
    """Get the effective file format (globally or per-table).

    Examples:
        lakehouse config get-format
        lakehouse config get-format --table expenses
    """
    from .config import get_default_format, get_table_format

    if table:
        fmt = get_table_format(table)
        console.print(f"Effective format for '{table}': [cyan]{fmt}[/cyan]")
    else:
        fmt = get_default_format()
        console.print(f"Default format: [cyan]{fmt}[/cyan]")


@alter.command("set-property")
@click.argument("key")
@click.argument("value")
@click.pass_context
def alter_set_property(ctx, key: str, value: str):
    """Set a property on the table.

    KEY is the property name (e.g., 'write.format.default').
    VALUE is the property value (e.g., 'vortex').
    """
    from .catalog import get_catalog, set_table_property

    table_name = ctx.obj["table_name"]
    catalog = get_catalog()

    try:
        msg = set_table_property(catalog, table_name, key, value)
        console.print(f"[bold green]✓ {msg}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@alter.command("get-property")
@click.argument("key")
@click.pass_context
def alter_get_property(ctx, key: str):
    """Get a property from the table.

    KEY is the property name (e.g., 'write.format.default').
    """
    from .catalog import get_catalog, get_table_property

    table_name = ctx.obj["table_name"]
    catalog = get_catalog()

    try:
        value = get_table_property(catalog, table_name, key)
        if value is None:
            console.print(f"[yellow]Property '{key}' not set on {table_name}[/yellow]")
        else:
            console.print(f"{key} = [cyan]{value}[/cyan]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@alter.command("remove-property")
@click.argument("key")
@click.pass_context
def alter_remove_property(ctx, key: str):
    """Remove a property from the table.

    KEY is the property name to remove.
    """
    from .catalog import get_catalog, remove_table_property

    table_name = ctx.obj["table_name"]
    catalog = get_catalog()

    try:
        msg = remove_table_property(catalog, table_name, key)
        console.print(f"[bold green]✓ {msg}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("import")
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--table", "table_name", required=True, help="Target table name")
@click.option("--if-exists", "if_exists", type=click.Choice(["fail", "append", "replace"]), default="fail",
              help="What to do if table exists (default: fail)")
@click.option("--format", "file_format", type=click.Choice(["csv", "json", "ndjson"]), default=None,
              help="File format (auto-detected from extension)")
@click.option("--delimiter", default=",", help="CSV delimiter (default: ',')")
@click.option("--header/--no-header", default=True, help="Whether CSV has headers (default: --header)")
def import_file(file_path: str, table_name: str, if_exists: str, file_format: str, delimiter: str, header: bool):
    """Import data from a CSV or JSON file into a table.

    Examples:
        lakehouse import data.csv --table expenses
        lakehouse import data.csv --table expenses --if-exists append
        lakehouse import data.json --table events
        lakehouse import data.ndjson --table events --if-exists replace
    """
    from .catalog import get_catalog, import_file as catalog_import_file

    catalog = get_catalog()

    try:
        result = catalog_import_file(
            catalog, file_path, table_name,
            file_format=file_format,
            if_exists=if_exists,
            delimiter=delimiter,
            has_header=header,
        )
        console.print(
            f"[bold green]✓ Imported {result['rows_imported']:,} rows "
            f"from {file_path} into {result['table']}[/bold green]"
        )
        console.print(f"  Format: {result['format']}")
    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name")
@click.option("--format", "file_format", type=click.Choice(["csv", "json", "ndjson", "parquet"]), default=None,
              help="Output format (auto-detected from -o extension)")
@click.option("--output", "-o", "output_path", default=None, help="Output file path (default: <table>.<format>)")
@click.option("--where", default=None, help="SQL WHERE clause for filtering")
@click.option("--columns", default=None, help="Comma-separated column names to include")
@click.option("--limit", type=int, default=None, help="Maximum rows to export")
def export(table_name: str, file_format: str, output_path: str, where: str, columns: str, limit: int):
    """Export a table to CSV, JSON, NDJSON, or Parquet.

    Examples:
        lakehouse export expenses --format csv -o expenses.csv
        lakehouse export expenses -o data.json
        lakehouse export expenses --format parquet --where "amount > 100"
        lakehouse export expenses -o report.csv --columns id,category,amount --limit 50
    """
    from .catalog import get_catalog, export_table

    catalog = get_catalog()

    col_list = [c.strip() for c in columns.split(",") if c.strip()] if columns else None

    try:
        result = export_table(
            catalog, table_name, output_path,
            file_format=file_format,
            where=where,
            columns=col_list,
            limit=limit,
        )
        console.print(
            f"[bold green]✓ Exported {result['rows_exported']:,} rows "
            f"from {result['table']} to {result['output']}[/bold green]"
        )
        console.print(f"  Format: {result['format']}")
        console.print(f"  Size: {result['size_bytes']:,} bytes")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name")
@click.option("--columns", default=None, help="Comma-separated column names to profile")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def profile(table_name: str, columns: str, as_json: bool):
    """Profile a table's data (statistics, distributions, top values).

    Examples:
        lakehouse profile expenses
        lakehouse profile expenses --columns amount,category
        lakehouse profile expenses --json
    """
    import json as json_mod
    from .catalog import get_catalog, profile_table

    catalog = get_catalog()
    col_list = [c.strip() for c in columns.split(",") if c.strip()] if columns else None

    try:
        stats = profile_table(catalog, table_name, columns=col_list)

        if as_json:
            print(json_mod.dumps(stats, indent=2, default=str))
            return

        console.print(f"\n[bold]Table: {stats['table']}[/bold] ({stats['row_count']:,} rows, {stats['column_count']} columns)\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Column")
        table.add_column("Type")
        table.add_column("Nulls", justify="right")
        table.add_column("Unique", justify="right")
        table.add_column("Min")
        table.add_column("Max")
        table.add_column("Mean")
        table.add_column("Std Dev")

        for col_name, col_stats in stats["columns"].items():
            table.add_row(
                col_name,
                col_stats.get("type", ""),
                str(col_stats.get("nulls", "")),
                str(col_stats.get("unique", "")),
                str(col_stats.get("min", "-")),
                str(col_stats.get("max", "-")),
                str(col_stats.get("mean", "-")) if "mean" in col_stats else "-",
                str(col_stats.get("std", "-")) if "std" in col_stats else "-",
            )

        console.print(table)

        # Print top values for string columns
        for col_name, col_stats in stats["columns"].items():
            if "top_values" in col_stats and col_stats["top_values"]:
                vals = ", ".join(f"{v} ({c})" for v, c in col_stats["top_values"].items())
                console.print(f"\n[dim]Top values for '{col_name}':[/dim] {vals}")

    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name", required=False)
@click.option("--all", "compact_all", is_flag=True, help="Compact all tables")
@click.option("--target-size-mb", type=int, default=128, help="Target file size in MB (default: 128)")
def compact(table_name: str, compact_all: bool, target_size_mb: int):
    """Compact a table by rewriting small files into fewer large files.

    Examples:
        lakehouse compact expenses
        lakehouse compact expenses --target-size-mb 128
        lakehouse compact --all
    """
    from .catalog import get_catalog, compact_table, list_tables

    if not table_name and not compact_all:
        console.print("[bold red]Error:[/bold red] Provide a table name or use --all")
        raise click.Abort()

    catalog = get_catalog()

    if compact_all:
        tables = list_tables(catalog)
        if not tables:
            console.print("[yellow]No tables found.[/yellow]")
            return

        for tbl in tables:
            try:
                result = compact_table(catalog, tbl, target_size_mb=target_size_mb)
                console.print(
                    f"[bold green]✓[/bold green] {tbl}: {result['message']} "
                    f"({result['rows']:,} rows)"
                )
            except Exception as e:
                console.print(f"[bold red]✗[/bold red] {tbl}: {e}")
    else:
        try:
            result = compact_table(catalog, table_name, target_size_mb=target_size_mb)
            console.print(f"[bold green]✓ {result['message']}[/bold green]")
            console.print(f"  Rows: {result['rows']:,}")
            console.print(f"  Size: {result['size_before']:,} → {result['size_after']:,} bytes")
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            raise click.Abort()


@main.command("maintenance-status")
@click.argument("table_name")
def maintenance_status_cmd(table_name: str):
    """Show maintenance status for a table (file count, sizes, orphans).

    Examples:
        lakehouse maintenance-status expenses
    """
    from .catalog import get_catalog, maintenance_status

    catalog = get_catalog()

    try:
        status = maintenance_status(catalog, table_name)

        console.print(f"\n[bold]Maintenance Status: {status['table']}[/bold]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Metric")
        table.add_column("Value", justify="right")

        table.add_row("Data files", str(status["data_files"]))
        table.add_row("Total size", f"{status['total_size_bytes']:,} bytes")
        table.add_row("Avg file size", f"{status['avg_file_size']:,} bytes")
        table.add_row("Snapshots", str(status["snapshots"]))
        table.add_row("Orphan files", str(status["orphan_files"]))
        table.add_row("Orphan size", f"{status['orphan_bytes']:,} bytes")

        console.print(table)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("table_name")
@click.option("--dry-run", is_flag=True, help="Report orphans without deleting")
def cleanup(table_name: str, dry_run: bool):
    """Clean up orphan files not referenced by any snapshot.

    Examples:
        lakehouse cleanup expenses --dry-run
        lakehouse cleanup expenses
    """
    from .catalog import get_catalog, cleanup_orphans

    catalog = get_catalog()

    try:
        result = cleanup_orphans(catalog, table_name, dry_run=dry_run)
        if result["orphan_files_found"] == 0:
            console.print("[green]No orphan files found.[/green]")
        else:
            console.print(f"[bold green]✓ {result['message']}[/bold green]")
            if dry_run:
                for f in result.get("files", []):
                    console.print(f"  [dim]{f}[/dim]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("create-table")
@click.argument("table_name")
@click.option("--column", "-c", "columns", multiple=True, required=True,
              help="Column definition as 'name:type' (e.g., 'id:long')")
@click.option("--partition", "-p", "partitions", multiple=True,
              help="Partition spec (e.g., 'month(date)', 'identity(category)', 'bucket(16, id)')")
def create_table_cmd(table_name: str, columns: tuple, partitions: tuple):
    """Create a new Iceberg table with optional partitioning.

    Examples:
        lakehouse create-table events -c id:long -c name:string
        lakehouse create-table events -c event_date:date -c category:string -c value:double -p "month(event_date)" -p "identity(category)"
        lakehouse create-table users -c id:long -c name:string -p "bucket(16, id)"
    """
    from .catalog import get_catalog, create_table

    # Parse column definitions
    col_dict = {}
    for col_def in columns:
        if ":" not in col_def:
            console.print(f"[bold red]Error:[/bold red] Invalid column format '{col_def}'. Expected 'name:type'.")
            raise click.Abort()
        name, col_type = col_def.split(":", 1)
        col_dict[name.strip()] = col_type.strip()

    catalog = get_catalog()

    try:
        result = create_table(catalog, table_name, col_dict, partitions=list(partitions) if partitions else None)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
        if result["partitions"]:
            console.print(f"  Partitions: {', '.join(result['partitions'])}")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("partitions")
@click.argument("table_name")
def partitions_cmd(table_name: str):
    """Show partition spec for a table.

    Examples:
        lakehouse partitions expenses
    """
    from .catalog import get_catalog, get_partitions

    catalog = get_catalog()

    try:
        info = get_partitions(catalog, table_name)

        if not info["is_partitioned"]:
            console.print(f"[yellow]Table {info['table']} is not partitioned.[/yellow]")
            return

        console.print(f"\n[bold]Partitions for {info['table']}:[/bold]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Name")
        table.add_column("Source Column")
        table.add_column("Transform")

        for field in info["fields"]:
            table.add_row(
                field["name"],
                field["source_column"],
                field["transform"],
            )

        console.print(table)

    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("partition-stats")
@click.argument("table_name")
def partition_stats_cmd(table_name: str):
    """Show partition data distribution.

    Examples:
        lakehouse partition-stats expenses
    """
    from .catalog import get_catalog, get_partition_stats

    catalog = get_catalog()

    try:
        stats = get_partition_stats(catalog, table_name)

        if not stats["is_partitioned"]:
            console.print(f"[yellow]{stats['message']}[/yellow]")
            return

        console.print(f"\n[bold]Partition stats for {stats['table']} ({stats['total_partitions']} partition(s)):[/bold]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Partition")
        table.add_column("Files", justify="right")
        table.add_column("Size", justify="right")

        for p in stats["partitions"]:
            table.add_row(
                p["partition"],
                str(p["files"]),
                f"{p['size_bytes']:,} bytes",
            )

        console.print(table)

    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.group()
@click.argument("table_name")
@click.pass_context
def validate(ctx, table_name: str):
    """Manage validation rules for a table."""
    ctx.ensure_object(dict)
    ctx.obj["table_name"] = table_name


@validate.command("add")
@click.argument("rule_type", type=click.Choice(["not_null", "unique", "range", "regex", "expression"]))
@click.option("--column", "-c", help="Column name (for not_null, range, regex)")
@click.option("--columns", help="Comma-separated column names (for unique)")
@click.option("--min", "min_val", type=float, help="Minimum value (for range)")
@click.option("--max", "max_val", type=float, help="Maximum value (for range)")
@click.option("--pattern", "-p", help="Regex pattern (for regex)")
@click.option("--sql", "sql_expr", help="SQL expression (for expression)")
@click.pass_context
def validate_add(ctx, rule_type: str, column: str, columns: str, min_val: float, max_val: float, pattern: str, sql_expr: str):
    """Add a validation rule.

    Examples:
        lakehouse validate expenses add not_null --column id
        lakehouse validate expenses add range --column amount --min 0 --max 100000
        lakehouse validate expenses add regex --column category --pattern "^[a-z_]+$"
        lakehouse validate expenses add expression --sql "amount > 0"
        lakehouse validate expenses add unique --columns id
    """
    from .validation import add_validation_rule

    table_name = ctx.obj["table_name"]

    rule = {"type": rule_type}
    if rule_type == "not_null":
        if not column:
            console.print("[bold red]Error:[/bold red] --column is required for not_null")
            raise click.Abort()
        rule["column"] = column
    elif rule_type == "unique":
        if not columns:
            console.print("[bold red]Error:[/bold red] --columns is required for unique")
            raise click.Abort()
        rule["columns"] = [c.strip() for c in columns.split(",")]
    elif rule_type == "range":
        if not column:
            console.print("[bold red]Error:[/bold red] --column is required for range")
            raise click.Abort()
        rule["column"] = column
        if min_val is not None:
            rule["min"] = min_val
        if max_val is not None:
            rule["max"] = max_val
    elif rule_type == "regex":
        if not column or not pattern:
            console.print("[bold red]Error:[/bold red] --column and --pattern are required for regex")
            raise click.Abort()
        rule["column"] = column
        rule["pattern"] = pattern
    elif rule_type == "expression":
        if not sql_expr:
            console.print("[bold red]Error:[/bold red] --sql is required for expression")
            raise click.Abort()
        rule["sql"] = sql_expr

    try:
        result = add_validation_rule(table_name, rule)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@validate.command("list")
@click.pass_context
def validate_list(ctx):
    """List all validation rules for a table."""
    from .validation import list_validation_rules

    table_name = ctx.obj["table_name"]
    rules = list_validation_rules(table_name)

    if not rules:
        console.print(f"[yellow]No validation rules for {table_name}.[/yellow]")
        return

    console.print(f"[bold]Validation rules for {table_name} ({len(rules)}):[/bold]\n")
    tbl = Table(show_header=True, header_style="bold cyan")
    tbl.add_column("ID")
    tbl.add_column("Type")
    tbl.add_column("Details")
    tbl.add_column("Created")

    for rule in rules:
        details = _format_rule_details(rule)
        tbl.add_row(
            rule["id"],
            rule["type"],
            details,
            rule.get("created_at", "")[:19],
        )

    console.print(tbl)


@validate.command("remove")
@click.argument("rule_id")
@click.pass_context
def validate_remove(ctx, rule_id: str):
    """Remove a validation rule by ID."""
    from .validation import remove_validation_rule

    table_name = ctx.obj["table_name"]

    try:
        result = remove_validation_rule(table_name, rule_id)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@validate.command("check")
@click.argument("json_data")
@click.pass_context
def validate_check(ctx, json_data: str):
    """Validate data against rules (dry run).

    Examples:
        lakehouse validate expenses check '[{"id": 1, "amount": -5}]'
    """
    import json as json_mod
    from .validation import list_validation_rules, validate_rows

    table_name = ctx.obj["table_name"]

    try:
        rows = json_mod.loads(json_data)
    except json_mod.JSONDecodeError as e:
        console.print(f"[bold red]Error:[/bold red] Invalid JSON: {e}")
        raise click.Abort()

    if not isinstance(rows, list):
        console.print("[bold red]Error:[/bold red] JSON data must be an array of objects")
        raise click.Abort()

    rules = list_validation_rules(table_name)
    if not rules:
        console.print(f"[yellow]No validation rules for {table_name}. All data passes.[/yellow]")
        return

    result = validate_rows(rows, rules)

    if result["valid"]:
        console.print(f"[bold green]✓ All {result['checked']} rows pass validation[/bold green]")
    else:
        console.print(f"[bold red]✗ Validation failed ({len(result['failures'])} issues):[/bold red]\n")
        for f in result["failures"]:
            console.print(f"  [red]•[/red] {f['message']}")


def _format_rule_details(rule: dict) -> str:
    """Format rule details for display."""
    rule_type = rule["type"]
    if rule_type == "not_null":
        return f"column: {rule['column']}"
    elif rule_type == "unique":
        return f"columns: {', '.join(rule['columns'])}"
    elif rule_type == "range":
        parts = []
        if "min" in rule:
            parts.append(f"min={rule['min']}")
        if "max" in rule:
            parts.append(f"max={rule['max']}")
        return f"column: {rule['column']}, {', '.join(parts)}"
    elif rule_type == "regex":
        return f"column: {rule['column']}, pattern: {rule['pattern']}"
    elif rule_type == "expression":
        return f"sql: {rule['sql']}"
    return ""


@main.command("audit")
@click.option("--table", "table_name", default=None, help="Filter by table name")
@click.option("--operation", default=None, help="Filter by operation type")
@click.option("--limit", default=50, help="Maximum entries to show")
@click.option("--since", default=None, help="Show entries after this ISO timestamp")
@click.option("--clear", "clear_flag", is_flag=True, help="Clear audit log")
@click.option("--older-than", default=None, help="Clear entries older than this (e.g., '30d', '24h')")
def audit_cmd(table_name: str, operation: str, limit: int, since: str, clear_flag: bool, older_than: str):
    """Show or manage the audit log of write operations.

    Examples:
        lakehouse audit
        lakehouse audit --table expenses --limit 20
        lakehouse audit --operation insert
        lakehouse audit --since 2026-02-01T00:00:00
        lakehouse audit --clear
        lakehouse audit --clear --older-than 30d
    """
    from .audit import get_audit_log, clear_audit_log

    if clear_flag:
        try:
            result = clear_audit_log(older_than=older_than)
            console.print(f"[bold green]✓ {result['message']}[/bold green]")
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            raise click.Abort()
        return

    entries = get_audit_log(table_name=table_name, operation=operation, limit=limit, since=since)

    if not entries:
        console.print("[yellow]No audit log entries found.[/yellow]")
        return

    console.print(f"[bold]Audit log ({len(entries)} entries):[/bold]\n")
    tbl = Table(show_header=True, header_style="bold cyan")
    tbl.add_column("Timestamp")
    tbl.add_column("Table")
    tbl.add_column("Operation")
    tbl.add_column("Rows")
    tbl.add_column("Source")
    tbl.add_column("Details")

    for entry in entries:
        details = entry.get("details", {})
        details_str = ", ".join(f"{k}={v}" for k, v in details.items()) if details else ""
        tbl.add_row(
            entry.get("timestamp", "")[:19],
            entry.get("table", ""),
            entry.get("operation", ""),
            str(entry.get("rows_affected", 0)),
            entry.get("source", ""),
            details_str[:60],
        )

    console.print(tbl)


@main.command("stats")
@click.argument("table_name", required=False)
@click.option("--all", "show_all", is_flag=True, help="Show stats for all tables")
@click.option("--refresh", is_flag=True, help="Refresh stats before showing")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def stats_cmd(table_name: str, show_all: bool, refresh: bool, as_json: bool):
    """Show or refresh cached table statistics.

    Examples:
        lakehouse stats expenses
        lakehouse stats --all
        lakehouse stats expenses --refresh
        lakehouse stats --all --refresh
        lakehouse stats expenses --json
    """
    import json as json_mod
    from .catalog import get_catalog
    from .stats import compute_table_stats, get_cached_stats, get_all_cached_stats, refresh_stats, is_stats_stale

    if not table_name and not show_all:
        console.print("[bold red]Error:[/bold red] Provide a table name or use --all")
        raise click.Abort()

    catalog = get_catalog()

    try:
        if show_all:
            if refresh:
                result = refresh_stats(catalog)
                console.print(f"[bold green]✓ {result['message']}[/bold green]\n")

            all_stats = get_all_cached_stats()
            if not all_stats:
                console.print("[yellow]No cached stats. Use --refresh to compute.[/yellow]")
                return

            if as_json:
                console.print(json_mod.dumps(all_stats, indent=2, default=str))
                return

            for tbl_name, s in all_stats.items():
                stale = is_stats_stale(tbl_name, catalog)
                stale_mark = " [yellow](stale)[/yellow]" if stale else ""
                console.print(f"\n[bold]{tbl_name}[/bold]{stale_mark}")
                console.print(f"  Rows: {s['row_count']}  |  Columns: {s['column_count']}  |  "
                             f"Files: {s['data_files']}  |  Size: {s['size_bytes']} bytes  |  "
                             f"Snapshots: {s['snapshot_count']}")
                console.print(f"  Last modified: {s.get('last_modified', 'N/A')}")
                console.print(f"  Cached at: {s.get('cached_at', 'N/A')}")
            return

        # Single table
        if refresh:
            stats = compute_table_stats(catalog, table_name)
            console.print(f"[bold green]✓ Refreshed stats for {table_name}[/bold green]\n")
        else:
            stats = get_cached_stats(table_name)
            if stats is None:
                console.print(f"[yellow]No cached stats for {table_name}. Use --refresh to compute.[/yellow]")
                return

        if as_json:
            console.print(json_mod.dumps(stats, indent=2, default=str))
            return

        full_name = table_name if "." in table_name else f"default.{table_name}"
        stale = is_stats_stale(full_name, catalog)
        stale_mark = " [yellow](stale)[/yellow]" if stale else ""

        console.print(f"[bold]Stats for {full_name}[/bold]{stale_mark}\n")
        console.print(f"  Rows: {stats['row_count']}")
        console.print(f"  Columns: {stats['column_count']}")
        console.print(f"  Data files: {stats['data_files']}")
        console.print(f"  Size: {stats['size_bytes']} bytes")
        console.print(f"  Snapshots: {stats['snapshot_count']}")
        console.print(f"  Last modified: {stats.get('last_modified', 'N/A')}")
        console.print(f"  Cached at: {stats.get('cached_at', 'N/A')}")

        if stats.get("columns"):
            console.print(f"\n[bold]Column stats:[/bold]")
            tbl = Table(show_header=True, header_style="bold cyan")
            tbl.add_column("Column")
            tbl.add_column("Type")
            tbl.add_column("Nulls")
            tbl.add_column("Unique")
            tbl.add_column("Min")
            tbl.add_column("Max")

            for col_name, col_info in stats["columns"].items():
                tbl.add_row(
                    col_name,
                    col_info.get("type", ""),
                    str(col_info.get("nulls", "")),
                    str(col_info.get("unique", "")),
                    str(col_info.get("min", "")),
                    str(col_info.get("max", "")),
                )
            console.print(tbl)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("query-save")
@click.argument("name")
@click.argument("sql")
@click.option("--description", "-d", default="", help="Description for the saved query")
def query_save(name: str, sql: str, description: str):
    """Save a named query for later use.

    Examples:
        lakehouse query-save monthly_totals "SELECT category, sum(amount) FROM expenses GROUP BY category"
        lakehouse query-save top_expenses "SELECT * FROM expenses ORDER BY amount DESC LIMIT 10" -d "Top expenses"
    """
    from .queries import save_query

    try:
        result = save_query(name, sql, description=description)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("query-list")
def query_list():
    """List all saved queries."""
    from .queries import list_saved_queries

    queries = list_saved_queries()

    if not queries:
        console.print("[yellow]No saved queries.[/yellow]")
        return

    console.print(f"[bold]Saved Queries ({len(queries)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Name")
    table.add_column("SQL")
    table.add_column("Description")
    table.add_column("Created")

    for q in queries:
        sql_preview = q["sql"]
        if len(sql_preview) > 60:
            sql_preview = sql_preview[:57] + "..."
        table.add_row(
            q["name"],
            sql_preview,
            q.get("description", ""),
            q.get("created_at", "")[:19],
        )

    console.print(table)


@main.command("query-run")
@click.argument("name")
@click.option("--max-rows", default=100, help="Maximum rows to return")
@click.option("--format", "output_format", type=click.Choice(["table", "csv", "json"]), default="table")
def query_run(name: str, max_rows: int, output_format: str):
    """Run a saved query by name.

    Examples:
        lakehouse query-run monthly_totals
        lakehouse query-run monthly_totals --format json
    """
    from .queries import get_saved_query
    from .query import QueryEngine

    try:
        saved = get_saved_query(name)
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()

    console.print(f"[dim]Running: {saved['sql']}[/dim]\n")

    engine = QueryEngine()

    try:
        result = engine.execute(saved["sql"], max_rows=max_rows)

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


@main.command("query-delete")
@click.argument("name")
def query_delete(name: str):
    """Delete a saved query.

    Examples:
        lakehouse query-delete monthly_totals
    """
    from .queries import delete_saved_query

    try:
        result = delete_saved_query(name)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("query-history")
@click.option("--limit", default=20, help="Number of history entries to show")
@click.option("--clear", "clear_flag", is_flag=True, help="Clear all history")
def query_history(limit: int, clear_flag: bool):
    """Show recent query history or clear it.

    Examples:
        lakehouse query-history
        lakehouse query-history --limit 50
        lakehouse query-history --clear
    """
    from .queries import get_history, clear_history

    if clear_flag:
        result = clear_history()
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
        return

    history = get_history(limit=limit)

    if not history:
        console.print("[yellow]No query history.[/yellow]")
        return

    console.print(f"[bold]Query History (last {len(history)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("SQL")
    table.add_column("Rows", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Executed At")

    for entry in history:
        sql_preview = entry["sql"]
        if len(sql_preview) > 60:
            sql_preview = sql_preview[:57] + "..."
        table.add_row(
            sql_preview,
            str(entry.get("rows_returned", "")),
            f"{entry.get('duration_ms', '')}ms" if entry.get("duration_ms") else "",
            entry.get("executed_at", "")[:19],
        )

    console.print(table)


@main.command()
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def dashboard(as_json: bool):
    """Show comprehensive lakehouse status overview.

    Examples:
        lakehouse dashboard
        lakehouse dashboard --json
    """
    import json as json_mod
    from .catalog import get_catalog
    from .dashboard import get_dashboard

    catalog = get_catalog()

    try:
        data = get_dashboard(catalog)

        if as_json:
            console.print(json_mod.dumps(data, indent=2, default=str))
            return

        # Header
        console.print(Panel("[bold]Lakehouse Dashboard[/bold]", expand=False))

        # Summary
        console.print(f"\n  Storage: {data['storage_path']}")
        console.print(f"  Namespaces: {len(data['namespaces'])} ({', '.join(data['namespaces'])})")
        console.print(f"  Total tables: {data['total_tables']}")
        console.print(f"  Total size: {data['total_size_display']}")

        # Tables
        if data["tables"]:
            console.print()
            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("Table")
            table.add_column("Rows", justify="right")
            table.add_column("Size", justify="right")
            table.add_column("Files", justify="right")
            table.add_column("Health")

            health_icons = {
                "Good": "[green]● Good[/green]",
                "Compact": "[yellow]▲ Compact[/yellow]",
                "Orphans": "[yellow]▲ Orphans[/yellow]",
                "Stale": "[red]✗ Stale[/red]",
            }

            for t in data["tables"]:
                table.add_row(
                    t["name"],
                    str(t["rows"]),
                    t["size_display"],
                    f"{t['data_files']} files",
                    health_icons.get(t["health"], t["health"]),
                )
            console.print(table)

        # Health legend
        console.print("\n  [dim]Health indicators:[/dim]")
        console.print("    [green]● Good[/green] - No maintenance needed")
        console.print("    [yellow]▲ Compact[/yellow] - >10 data files, compaction recommended")
        console.print("    [yellow]▲ Orphans[/yellow] - Orphan files detected")
        console.print("    [red]✗ Stale[/red] - Stats cache is outdated")

        # Recent activity
        if data["recent_activity"]:
            console.print(f"\n[bold]Recent Activity (last {len(data['recent_activity'])}):[/bold]")
            for entry in data["recent_activity"]:
                ts = entry.get("timestamp", "")[:16]
                op = entry.get("operation", "").upper()
                tbl = entry.get("table", "")
                rows_affected = entry.get("rows_affected", 0)
                source = entry.get("source", "")
                row_info = f" ({rows_affected} rows)" if rows_affected else ""
                console.print(f"  • {ts} {op} {tbl}{row_info} via {source}")
        else:
            console.print("\n  [dim]No recent activity[/dim]")

        # Queries info
        console.print(f"\n  Saved Queries: {data['saved_queries_count']}")
        console.print(f"  Query History: {data['history_entries_count']} entries")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.group("maintain")
def maintain_group():
    """Manage maintenance policies for tables.

    Examples:
        lakehouse maintain set expenses --compact-threshold 10
        lakehouse maintain show expenses
        lakehouse maintain remove expenses
        lakehouse maintain run expenses
        lakehouse maintain run --all --dry-run
        lakehouse maintain check expenses
    """
    pass


@maintain_group.command("set")
@click.argument("table_name")
@click.option("--compact-threshold", type=int, default=None, help="Compact when data files >= this (default: 10)")
@click.option("--retain-last", type=int, default=None, help="Keep at least N snapshots (default: 5)")
@click.option("--expire-older-than", default=None, help="Expire snapshots older than this (e.g. '30d')")
@click.option("--no-cleanup", is_flag=True, help="Disable auto orphan cleanup")
def maintain_set(table_name: str, compact_threshold: int, retain_last: int, expire_older_than: str, no_cleanup: bool):
    """Set maintenance policy for a table."""
    from .maintenance import set_maintenance_policy

    policy = {}
    if compact_threshold is not None:
        policy["auto_compact_threshold"] = compact_threshold
    if retain_last is not None:
        policy["auto_expire_retain_last"] = retain_last
    if expire_older_than is not None:
        policy["auto_expire_older_than"] = expire_older_than
    if no_cleanup:
        policy["auto_cleanup_orphans"] = False

    result = set_maintenance_policy(table_name, policy)
    console.print(f"[bold green]✓ {result['message']}[/bold green]")

    p = result["policy"]
    console.print(f"  Compact threshold: {p['auto_compact_threshold']} files")
    console.print(f"  Retain last: {p['auto_expire_retain_last']} snapshots")
    console.print(f"  Expire older than: {p.get('auto_expire_older_than') or 'not set'}")
    console.print(f"  Auto cleanup orphans: {p['auto_cleanup_orphans']}")


@maintain_group.command("show")
@click.argument("table_name")
def maintain_show(table_name: str):
    """Show maintenance policy for a table."""
    from .maintenance import get_maintenance_policy

    policy = get_maintenance_policy(table_name)
    if policy is None:
        console.print(f"[yellow]No maintenance policy for {table_name}[/yellow]")
        return

    full_name = table_name if "." in table_name else f"default.{table_name}"
    console.print(f"[bold]Maintenance policy for {full_name}:[/bold]\n")
    console.print(f"  Compact threshold: {policy['auto_compact_threshold']} files")
    console.print(f"  Retain last: {policy['auto_expire_retain_last']} snapshots")
    console.print(f"  Expire older than: {policy.get('auto_expire_older_than') or 'not set'}")
    console.print(f"  Auto cleanup orphans: {policy['auto_cleanup_orphans']}")
    console.print(f"  Created: {policy.get('created_at', 'N/A')}")
    console.print(f"  Last run: {policy.get('last_run') or 'never'}")


@maintain_group.command("remove")
@click.argument("table_name")
def maintain_remove(table_name: str):
    """Remove maintenance policy for a table."""
    from .maintenance import remove_maintenance_policy

    result = remove_maintenance_policy(table_name)
    console.print(f"[bold green]✓ {result['message']}[/bold green]")


@maintain_group.command("run")
@click.argument("table_name", required=False)
@click.option("--all", "run_all", is_flag=True, help="Run maintenance for all tables with policies")
@click.option("--dry-run", is_flag=True, help="Show what would happen without making changes")
def maintain_run(table_name: str, run_all: bool, dry_run: bool):
    """Run maintenance for a table or all tables with policies."""
    from .catalog import get_catalog
    from .maintenance import run_maintenance

    if not table_name and not run_all:
        console.print("[bold red]Error:[/bold red] Provide a table name or use --all")
        raise click.Abort()

    catalog = get_catalog()

    try:
        actions = run_maintenance(
            catalog,
            table_name=table_name if not run_all else None,
            dry_run=dry_run,
        )

        if not actions:
            console.print("[green]No maintenance actions needed.[/green]")
            return

        mode = "[yellow](dry run)[/yellow] " if dry_run else ""
        console.print(f"{mode}[bold]Maintenance actions ({len(actions)}):[/bold]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Table")
        table.add_column("Action")
        table.add_column("Status")
        table.add_column("Detail")

        status_colors = {"completed": "green", "dry_run": "yellow", "failed": "red"}

        for a in actions:
            color = status_colors.get(a["status"], "white")
            table.add_row(
                a["table"],
                a["action"],
                f"[{color}]{a['status']}[/{color}]",
                a["detail"],
            )
        console.print(table)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@maintain_group.command("check")
@click.argument("table_name", required=False)
@click.option("--all", "check_all", is_flag=True, help="Check all tables with policies")
def maintain_check(table_name: str, check_all: bool):
    """Check if a table needs maintenance."""
    from .catalog import get_catalog
    from .maintenance import check_maintenance_needed, _load_store

    if not table_name and not check_all:
        console.print("[bold red]Error:[/bold red] Provide a table name or use --all")
        raise click.Abort()

    catalog = get_catalog()

    try:
        if check_all:
            store = _load_store()
            tables = list(store.keys())
        else:
            tables = [table_name]

        for tbl in tables:
            check = check_maintenance_needed(catalog, tbl)
            name = check["table"]

            if not check["has_policy"]:
                console.print(f"[yellow]{name}: no policy[/yellow]")
                continue

            if check["actions_needed"]:
                console.print(f"[bold]{name}:[/bold] {check['message']}")
                for action in check["actions_needed"]:
                    console.print(f"  • {action}")
            else:
                console.print(f"[green]{name}: {check['message']}[/green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.group("view")
def view_group():
    """Manage SQL views (named virtual tables).

    Examples:
        lakehouse view create recent_expenses "SELECT * FROM expenses WHERE date >= '2026-01-01'"
        lakehouse view list
        lakehouse view query recent_expenses
        lakehouse view drop recent_expenses
    """
    pass


@view_group.command("create")
@click.argument("name")
@click.argument("sql")
@click.option("--description", "-d", default="", help="Description for the view")
def view_create(name: str, sql: str, description: str):
    """Create a named SQL view."""
    from .views import create_view

    try:
        result = create_view(name, sql, description=description)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
        console.print(f"  SQL: {result['sql']}")
        if result["description"]:
            console.print(f"  Description: {result['description']}")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@view_group.command("list")
def view_list():
    """List all SQL views."""
    from .views import list_views

    views = list_views()
    if not views:
        console.print("[yellow]No views defined.[/yellow]")
        return

    console.print(f"[bold]Views ({len(views)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Name")
    table.add_column("SQL")
    table.add_column("Description")
    table.add_column("Created")

    for v in views:
        sql_preview = v["sql"]
        if len(sql_preview) > 60:
            sql_preview = sql_preview[:57] + "..."
        table.add_row(
            v["name"],
            sql_preview,
            v.get("description", ""),
            v.get("created_at", "")[:19],
        )
    console.print(table)


@view_group.command("show")
@click.argument("name")
def view_show(name: str):
    """Show a view definition."""
    from .views import get_view

    try:
        v = get_view(name)
        console.print(f"[bold]View: {v['name']}[/bold]\n")
        console.print(f"  SQL: {v['sql']}")
        if v["description"]:
            console.print(f"  Description: {v['description']}")
        console.print(f"  Created: {v.get('created_at', 'N/A')}")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@view_group.command("query")
@click.argument("name")
@click.option("--max-rows", default=100, help="Maximum rows to return")
@click.option("--format", "output_format", type=click.Choice(["table", "csv", "json"]), default="table")
def view_query(name: str, max_rows: int, output_format: str):
    """Query a SQL view."""
    from .views import query_view
    from .query import QueryEngine

    engine = QueryEngine()

    try:
        result = query_view(name, engine, max_rows=max_rows)

        if result.empty:
            console.print("[yellow]View returned no results.[/yellow]")
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

    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Query error:[/bold red] {e}")
        raise click.Abort()


@view_group.command("drop")
@click.argument("name")
def view_drop(name: str):
    """Drop a SQL view."""
    from .views import drop_view

    try:
        result = drop_view(name)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.group("tag")
def tag_group():
    """Manage table tags for organization and discovery.

    Examples:
        lakehouse tag add expenses finance pii
        lakehouse tag show expenses
        lakehouse tag search finance
        lakehouse tag remove expenses deprecated
    """
    pass


@tag_group.command("add")
@click.argument("table_name")
@click.argument("tags", nargs=-1, required=True)
def tag_add(table_name: str, tags: tuple):
    """Add tags to a table."""
    from .tagging import tag_table

    result = tag_table(table_name, list(tags))
    console.print(f"[bold green]✓ {result['message']}[/bold green]")
    console.print(f"  Tags: {', '.join(result['tags'])}")


@tag_group.command("remove")
@click.argument("table_name")
@click.argument("tags", nargs=-1, required=True)
def tag_remove(table_name: str, tags: tuple):
    """Remove tags from a table."""
    from .tagging import untag_table

    result = untag_table(table_name, list(tags))
    console.print(f"[bold green]✓ {result['message']}[/bold green]")
    if result["tags"]:
        console.print(f"  Remaining tags: {', '.join(result['tags'])}")


@tag_group.command("show")
@click.argument("table_name")
def tag_show(table_name: str):
    """Show tags for a table."""
    from .tagging import get_tags

    tags = get_tags(table_name)
    full_name = table_name if "." in table_name else f"default.{table_name}"
    if tags:
        console.print(f"[bold]{full_name}:[/bold] {', '.join(tags)}")
    else:
        console.print(f"[yellow]{full_name} has no tags[/yellow]")


@tag_group.command("search")
@click.argument("tag")
def tag_search(tag: str):
    """Find all tables with a given tag."""
    from .tagging import search_by_tag

    tables = search_by_tag(tag)
    if tables:
        console.print(f"[bold]Tables tagged '{tag}' ({len(tables)}):[/bold]")
        for t in tables:
            console.print(f"  • {t}")
    else:
        console.print(f"[yellow]No tables tagged '{tag}'[/yellow]")


@main.command("bookmark")
@click.argument("table_name", required=False)
@click.option("--list", "list_flag", is_flag=True, help="List all bookmarks")
@click.option("--remove", "remove_flag", is_flag=True, help="Remove bookmark")
def bookmark_cmd(table_name: str, list_flag: bool, remove_flag: bool):
    """Bookmark tables for quick access.

    Examples:
        lakehouse bookmark expenses
        lakehouse bookmark --list
        lakehouse bookmark expenses --remove
    """
    from .tagging import bookmark_table, unbookmark_table, list_bookmarks

    if list_flag:
        bookmarks = list_bookmarks()
        if bookmarks:
            console.print(f"[bold]Bookmarked tables ({len(bookmarks)}):[/bold]")
            for b in bookmarks:
                console.print(f"  ★ {b}")
        else:
            console.print("[yellow]No bookmarks[/yellow]")
        return

    if not table_name:
        console.print("[bold red]Error:[/bold red] Provide a table name or use --list")
        raise click.Abort()

    if remove_flag:
        result = unbookmark_table(table_name)
    else:
        result = bookmark_table(table_name)

    console.print(f"[bold green]✓ {result['message']}[/bold green]")


@main.command("search")
@click.argument("query")
def search_cmd(query: str):
    """Search tables by name, tag, or description.

    Examples:
        lakehouse search finance
        lakehouse search expenses
    """
    from .catalog import get_catalog
    from .tagging import search_tables

    catalog = get_catalog()
    results = search_tables(query, catalog=catalog)

    if not results:
        console.print(f"[yellow]No tables matching '{query}'[/yellow]")
        return

    console.print(f"[bold]Search results for '{query}' ({len(results)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Table")
    table.add_column("Tags")
    table.add_column("Description")
    table.add_column("Match")
    table.add_column("★", justify="center")

    for r in results:
        table.add_row(
            r["table"],
            ", ".join(r["tags"]) if r["tags"] else "",
            r["description"][:50] if r["description"] else "",
            ", ".join(r["match_type"]),
            "★" if r["bookmarked"] else "",
        )
    console.print(table)


@main.group("clone")
def clone_group():
    """Clone tables for safe experimentation.

    Examples:
        lakehouse clone create expenses expenses_experiment
        lakehouse clone create expenses expenses_backup --as-of 2026-01-15
        lakehouse clone list
        lakehouse clone promote expenses_experiment expenses
        lakehouse clone discard expenses_experiment
    """
    pass


@clone_group.command("create")
@click.argument("source_table")
@click.argument("target_table")
@click.option("--as-of", default=None, help="Snapshot ID or ISO timestamp for point-in-time clone")
def clone_create(source_table: str, target_table: str, as_of: str):
    """Clone a table (zero-copy)."""
    from .catalog import get_catalog
    from .cloning import clone_table

    catalog = get_catalog()

    try:
        result = clone_table(catalog, source_table, target_table, as_of=as_of)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
        if result["as_of"]:
            console.print(f"  Point-in-time: {result['as_of']}")
        console.print(f"  Source snapshot: {result['source_snapshot_id']}")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@clone_group.command("list")
def clone_list():
    """List all active clones."""
    from .cloning import list_clones

    clones = list_clones()
    if not clones:
        console.print("[yellow]No active clones.[/yellow]")
        return

    console.print(f"[bold]Active Clones ({len(clones)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Clone")
    table.add_column("Source")
    table.add_column("Rows", justify="right")
    table.add_column("Cloned At")
    table.add_column("As Of")

    for c in clones:
        table.add_row(
            c["clone"],
            c["source_table"],
            str(c["row_count"]),
            c["cloned_at"][:19],
            c.get("as_of") or "",
        )
    console.print(table)


@clone_group.command("promote")
@click.argument("clone_table")
@click.argument("original_table")
def clone_promote(clone_table: str, original_table: str):
    """Promote a clone to replace the original table."""
    from .catalog import get_catalog
    from .cloning import promote_clone

    catalog = get_catalog()

    try:
        result = promote_clone(catalog, clone_table, original_table)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@clone_group.command("discard")
@click.argument("clone_table")
def clone_discard(clone_table: str):
    """Discard (drop) a cloned table."""
    from .catalog import get_catalog
    from .cloning import discard_clone

    catalog = get_catalog()

    try:
        result = discard_clone(catalog, clone_table)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.group("matview")
def matview_group():
    """Manage materialized views (cached query results).

    Examples:
        lakehouse matview create monthly_totals "SELECT category, SUM(amount) FROM expenses GROUP BY category"
        lakehouse matview list
        lakehouse matview query monthly_totals
        lakehouse matview refresh monthly_totals
        lakehouse matview check monthly_totals
        lakehouse matview drop monthly_totals
    """
    pass


@matview_group.command("create")
@click.argument("name")
@click.argument("sql")
@click.option("--description", "-d", default="", help="Description for the view")
def matview_create(name: str, sql: str, description: str):
    """Create a materialized view (execute SQL and cache results)."""
    from .catalog import get_catalog
    from .query import QueryEngine
    from .matviews import create_materialized_view

    catalog = get_catalog()
    engine = QueryEngine(catalog=catalog)

    try:
        result = create_materialized_view(name, sql, engine, catalog, description=description)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
        console.print(f"  Backing table: {result['backing_table']}")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@matview_group.command("list")
def matview_list():
    """List all materialized views."""
    from .matviews import list_materialized_views

    views = list_materialized_views()
    if not views:
        console.print("[yellow]No materialized views.[/yellow]")
        return

    console.print(f"[bold]Materialized Views ({len(views)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Name")
    table.add_column("SQL")
    table.add_column("Rows", justify="right")
    table.add_column("Refreshed")
    table.add_column("Description")

    for v in views:
        sql_preview = v["sql"][:50] + "..." if len(v["sql"]) > 50 else v["sql"]
        table.add_row(
            v["name"],
            sql_preview,
            str(v["row_count"]),
            v["last_refreshed"][:19],
            v.get("description", ""),
        )
    console.print(table)


@matview_group.command("query")
@click.argument("name")
@click.option("--max-rows", default=100, help="Maximum rows to return")
@click.option("--format", "output_format", type=click.Choice(["table", "csv", "json"]), default="table")
def matview_query(name: str, max_rows: int, output_format: str):
    """Query cached results of a materialized view (fast)."""
    from .catalog import get_catalog
    from .query import QueryEngine
    from .matviews import query_materialized_view

    catalog = get_catalog()
    engine = QueryEngine(catalog=catalog)

    try:
        df = query_materialized_view(name, engine, max_rows=max_rows)

        if df.empty:
            console.print("[yellow]View returned no results.[/yellow]")
            return

        if output_format == "table":
            table = Table(show_header=True, header_style="bold magenta")
            for col in df.columns:
                table.add_column(str(col))
            for _, row in df.iterrows():
                table.add_row(*[str(v) for v in row])
            console.print(table)
        elif output_format == "csv":
            print(df.to_csv(index=False))
        elif output_format == "json":
            print(df.to_json(orient="records", indent=2))

        console.print(f"\n[dim]({len(df)} rows)[/dim]")

    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@matview_group.command("refresh")
@click.argument("name")
def matview_refresh(name: str):
    """Refresh a materialized view (re-execute SQL)."""
    from .catalog import get_catalog
    from .query import QueryEngine
    from .matviews import refresh_materialized_view

    catalog = get_catalog()
    engine = QueryEngine(catalog=catalog)

    try:
        result = refresh_materialized_view(name, engine, catalog)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@matview_group.command("check")
@click.argument("name")
def matview_check(name: str):
    """Check if a materialized view is stale."""
    from .catalog import get_catalog
    from .matviews import check_materialized_view_freshness

    catalog = get_catalog()

    try:
        result = check_materialized_view_freshness(name, catalog)
        if result["stale"]:
            console.print(f"[yellow]{result['message']}[/yellow]")
            for t in result["changed_tables"]:
                console.print(f"  • {t}")
        else:
            console.print(f"[green]{result['message']}[/green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@matview_group.command("drop")
@click.argument("name")
def matview_drop(name: str):
    """Drop a materialized view and its backing table."""
    from .catalog import get_catalog
    from .matviews import drop_materialized_view

    catalog = get_catalog()

    try:
        result = drop_materialized_view(name, catalog)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("join")
@click.argument("sql")
@click.option("--into", "target_table", default=None, help="Save results to a table")
@click.option("--mode", type=click.Choice(["overwrite", "append"]), default="overwrite", help="Write mode for --into")
@click.option("--max-rows", default=100, help="Maximum rows to return")
@click.option("--format", "output_format", type=click.Choice(["table", "csv", "json"]), default="table")
def join_cmd(sql: str, target_table: str, mode: str, max_rows: int, output_format: str):
    """Execute a cross-table join query.

    Examples:
        lakehouse join "SELECT e.*, n.text FROM expenses e JOIN notes n ON e.id = n.id"
        lakehouse join "SELECT ..." --into analytics.report
        lakehouse join "SELECT ..." --format json --max-rows 500
    """
    from .catalog import get_catalog
    from .joins import execute_join, join_to_table

    catalog = get_catalog()

    try:
        if target_table:
            result = join_to_table(catalog, sql, target_table, mode=mode)
            console.print(f"[bold green]✓ {result['message']}[/bold green]")
            return

        result = execute_join(catalog, sql, max_rows=max_rows)
        df = result["dataframe"]

        if df.empty:
            console.print("[yellow]Join returned no results.[/yellow]")
            return

        if output_format == "table":
            table = Table(show_header=True, header_style="bold magenta")
            for col in df.columns:
                table.add_column(str(col))
            for _, row in df.iterrows():
                table.add_row(*[str(v) for v in row])
            console.print(table)
        elif output_format == "csv":
            print(df.to_csv(index=False))
        elif output_format == "json":
            print(df.to_json(orient="records", indent=2))

        console.print(f"\n[dim]({result['row_count']} rows)[/dim]")

    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command("join-suggest")
@click.argument("table_name")
def join_suggest_cmd(table_name: str):
    """Suggest possible joins for a table based on matching column names.

    Examples:
        lakehouse join-suggest expenses
    """
    from .catalog import get_catalog
    from .joins import suggest_joins

    catalog = get_catalog()

    try:
        suggestions = suggest_joins(catalog, table_name)
        if not suggestions:
            console.print(f"[yellow]No join suggestions found for {table_name}[/yellow]")
            return

        console.print(f"[bold]Join suggestions for {table_name} ({len(suggestions)}):[/bold]\n")
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Join Table")
        table.add_column("Column")
        table.add_column("Source Type")
        table.add_column("Target Type")
        table.add_column("Example SQL")

        for s in suggestions:
            table.add_row(
                s["table"],
                s["column"],
                s["source_type"],
                s["target_type"],
                s["join_sql"][:60],
            )
        console.print(table)

    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.group("lineage")
def lineage_group():
    """Track data lineage (table-level dependency graph).

    Examples:
        lakehouse lineage add --source expenses --source categories --target spending_report
        lakehouse lineage upstream spending_report
        lakehouse lineage downstream expenses
        lakehouse lineage graph
        lakehouse lineage remove --source expenses --target spending_report
        lakehouse lineage impact expenses
    """
    pass


@lineage_group.command("add")
@click.option("--source", "-s", "sources", multiple=True, required=True, help="Source table name (repeat for multiple)")
@click.option("--target", "-t", "target", required=True, help="Target table name")
@click.option("--operation", "-o", default="manual", help="Operation type (manual, insert_from, view, pipeline)")
@click.option("--sql", default=None, help="SQL that produced this relationship")
def lineage_add(sources: tuple, target: str, operation: str, sql: str):
    """Record a lineage edge: sources → target."""
    from .lineage import record_lineage

    try:
        result = record_lineage(list(sources), target, operation=operation, sql=sql)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@lineage_group.command("upstream")
@click.argument("table_name")
@click.option("--direct", is_flag=True, help="Show only direct dependencies")
def lineage_upstream(table_name: str, direct: bool):
    """Show tables that feed into this table."""
    from .lineage import get_upstream

    deps = get_upstream(table_name, transitive=not direct)
    if not deps:
        console.print(f"[yellow]No upstream dependencies for {table_name}[/yellow]")
        return

    mode = "direct" if direct else "all"
    console.print(f"[bold]Upstream dependencies for {table_name} ({mode}, {len(deps)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Table")
    table.add_column("Operation")
    table.add_column("Depth", justify="right")
    for d in deps:
        table.add_row(d["table"], d["operation"], str(d["depth"]))
    console.print(table)


@lineage_group.command("downstream")
@click.argument("table_name")
@click.option("--direct", is_flag=True, help="Show only direct dependents")
def lineage_downstream(table_name: str, direct: bool):
    """Show tables that depend on this table."""
    from .lineage import get_downstream

    deps = get_downstream(table_name, transitive=not direct)
    if not deps:
        console.print(f"[yellow]No downstream dependents for {table_name}[/yellow]")
        return

    mode = "direct" if direct else "all"
    console.print(f"[bold]Downstream dependents of {table_name} ({mode}, {len(deps)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Table")
    table.add_column("Operation")
    table.add_column("Depth", justify="right")
    for d in deps:
        table.add_row(d["table"], d["operation"], str(d["depth"]))
    console.print(table)


@lineage_group.command("graph")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def lineage_graph(as_json: bool):
    """Show the full lineage graph."""
    import json as json_mod
    from .lineage import get_lineage_graph

    graph = get_lineage_graph()

    if as_json:
        console.print(json_mod.dumps(graph, indent=2))
        return

    if not graph["nodes"]:
        console.print("[yellow]No lineage data recorded.[/yellow]")
        return

    console.print(f"[bold]Lineage Graph ({graph['node_count']} tables, {graph['edge_count']} edges):[/bold]\n")
    for edge in graph["edges"]:
        sources = ", ".join(edge["sources"])
        console.print(f"  {sources} [dim]──({edge['operation']})──▶[/dim] {edge['target']}")


@lineage_group.command("remove")
@click.option("--source", "-s", "source", required=True, help="Source table name")
@click.option("--target", "-t", "target", required=True, help="Target table name")
def lineage_remove(source: str, target: str):
    """Remove a lineage edge."""
    from .lineage import remove_lineage

    result = remove_lineage(source, target)
    console.print(f"[bold green]✓ {result['message']}[/bold green]")


@lineage_group.command("impact")
@click.argument("table_name")
def lineage_impact(table_name: str):
    """Analyze impact of dropping or modifying a table."""
    from .lineage import get_impact_analysis

    result = get_impact_analysis(table_name)
    console.print(f"[bold]{result['message']}[/bold]")

    if result["details"]:
        console.print()
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Affected Table")
        table.add_column("Via Operation")
        table.add_column("Depth", justify="right")
        for d in result["details"]:
            table.add_row(d["table"], d["operation"], str(d["depth"]))
        console.print(table)


# --- Pipeline commands ---


@main.group("pipeline")
def pipeline_group():
    """Manage data pipelines (multi-step SQL transformations)."""
    pass


@pipeline_group.command("create")
@click.argument("name")
@click.option("--steps", required=True, help="JSON array of steps: [{\"sql\":\"...\",\"target_table\":\"...\",\"mode\":\"overwrite\"}]")
@click.option("--description", "-d", default="", help="Pipeline description")
def pipeline_create(name: str, steps: str, description: str):
    """Create a named pipeline."""
    import json as _json
    try:
        from lakehouse.pipelines import create_pipeline
        parsed_steps = _json.loads(steps)
        result = create_pipeline(name, parsed_steps, description=description)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except (ValueError, _json.JSONDecodeError) as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@pipeline_group.command("list")
def pipeline_list():
    """List all pipelines."""
    from lakehouse.pipelines import list_pipelines
    pipelines = list_pipelines()
    if not pipelines:
        console.print("[dim]No pipelines defined.[/dim]")
        return

    table = Table(title="Pipelines")
    table.add_column("Name", style="cyan")
    table.add_column("Steps", justify="right")
    table.add_column("Description")
    table.add_column("Last Run")
    table.add_column("Status")
    for p in pipelines:
        last_run = p["last_run"][:19] if p.get("last_run") else "never"
        status = p.get("last_run_status") or "-"
        table.add_row(p["name"], str(p["step_count"]), p["description"], last_run, status)
    console.print(table)


@pipeline_group.command("show")
@click.argument("name")
def pipeline_show(name: str):
    """Show pipeline definition."""
    import json as _json
    try:
        from lakehouse.pipelines import get_pipeline
        p = get_pipeline(name)
        console.print(f"[bold cyan]Pipeline: {p['name']}[/bold cyan]")
        if p["description"]:
            console.print(f"  Description: {p['description']}")
        console.print(f"  Created: {p['created_at'][:19]}")
        last_run = p["last_run"][:19] if p.get("last_run") else "never"
        console.print(f"  Last run: {last_run} ({p.get('last_run_status') or '-'})")
        console.print(f"\n[bold]Steps ({len(p['steps'])}):[/bold]")
        for i, step in enumerate(p["steps"]):
            target = step.get("target_table") or "(no target)"
            mode = step.get("mode", "overwrite")
            console.print(f"  [{i}] {step['sql']}")
            console.print(f"      → {target} ({mode})")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@pipeline_group.command("run")
@click.argument("name")
@click.option("--dry-run", is_flag=True, help="Validate SQL without executing")
def pipeline_run(name: str, dry_run: bool):
    """Run a pipeline."""
    try:
        from lakehouse.pipelines import run_pipeline
        from lakehouse.catalog import get_catalog
        from lakehouse.query import QueryEngine
        catalog = get_catalog()
        engine = QueryEngine(catalog=catalog)
        result = run_pipeline(name, catalog, engine, dry_run=dry_run)

        if result["steps_failed"] > 0:
            console.print(f"[bold red]✗ {result['message']}[/bold red]")
        else:
            console.print(f"[bold green]✓ {result['message']}[/bold green]")

        table = Table(title="Step Results")
        table.add_column("Step", justify="right")
        table.add_column("SQL")
        table.add_column("Target")
        table.add_column("Rows", justify="right")
        table.add_column("Status")
        table.add_column("Time", justify="right")
        for r in result["step_results"]:
            rows = str(r.get("rows_affected", "-"))
            status_style = "green" if r["status"] in ("completed", "validated") else "red"
            table.add_row(
                str(r["step"]),
                r["sql"][:60],
                r.get("target_table") or "-",
                rows,
                f"[{status_style}]{r['status']}[/{status_style}]",
                f"{r['duration_ms']}ms",
            )
        console.print(table)

        if result["steps_failed"] > 0:
            for r in result["step_results"]:
                if r["status"] == "error":
                    console.print(f"\n[red]Error in step {r['step']}:[/red] {r['error']}")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@pipeline_group.command("drop")
@click.argument("name")
def pipeline_drop(name: str):
    """Drop a pipeline."""
    try:
        from lakehouse.pipelines import drop_pipeline
        result = drop_pipeline(name)
        console.print(f"[bold green]✓ {result['message']}[/bold green]")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


# --- Quality commands ---


@main.group("quality")
def quality_group():
    """Data quality scoring and anomaly detection."""
    pass


@quality_group.command("score")
@click.argument("table_name")
def quality_score(table_name: str):
    """Compute quality score for a table."""
    try:
        from lakehouse.quality import compute_quality_score
        from lakehouse.catalog import get_catalog
        catalog = get_catalog()
        result = compute_quality_score(catalog, table_name)

        console.print(f"\n[bold cyan]Quality Score: {result['overall_score']}/100[/bold cyan]")
        table = Table(title=f"Score Breakdown — {result['table']}")
        table.add_column("Component", style="cyan")
        table.add_column("Score", justify="right")
        table.add_column("Weight", justify="right")
        table.add_row("Completeness", f"{result['completeness']}", "30%")
        table.add_row("Uniqueness", f"{result['uniqueness']}", "25%")
        table.add_row("Freshness", f"{result['freshness']}", "20%")
        table.add_row("Rule Compliance", f"{result['rule_compliance']}", "25%")
        table.add_row("[bold]Overall[/bold]", f"[bold]{result['overall_score']}[/bold]", "[bold]100%[/bold]")
        console.print(table)

        if result["recommendations"]:
            console.print("\n[bold yellow]Recommendations:[/bold yellow]")
            for r in result["recommendations"]:
                console.print(f"  • {r}")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@quality_group.command("anomalies")
@click.argument("table_name")
def quality_anomalies(table_name: str):
    """Detect data anomalies for a table."""
    try:
        from lakehouse.quality import detect_anomalies
        from lakehouse.catalog import get_catalog
        catalog = get_catalog()
        anomalies = detect_anomalies(catalog, table_name)

        if not anomalies:
            console.print("[bold green]✓ No anomalies detected.[/bold green]")
            return

        table = Table(title="Anomalies Detected")
        table.add_column("Type", style="cyan")
        table.add_column("Column")
        table.add_column("Severity")
        table.add_column("Description")
        for a in anomalies:
            sev_style = "red" if a["severity"] == "critical" else "yellow" if a["severity"] == "warning" else "dim"
            table.add_row(
                a["type"],
                a.get("column") or "-",
                f"[{sev_style}]{a['severity']}[/{sev_style}]",
                a["description"],
            )
        console.print(table)
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@quality_group.command("report")
@click.option("--json-output", "json_out", is_flag=True, help="Output as JSON")
def quality_report(json_out: bool):
    """Generate quality report for all tables."""
    import json as _json
    try:
        from lakehouse.quality import get_quality_report
        from lakehouse.catalog import get_catalog
        catalog = get_catalog()
        report = get_quality_report(catalog)

        if json_out:
            console.print(_json.dumps(report, indent=2, default=str))
            return

        console.print(f"\n[bold cyan]Quality Report — {report['total_tables']} table(s), avg score: {report['average_score']}/100[/bold cyan]\n")
        table = Table(title="Table Quality Scores")
        table.add_column("Table", style="cyan")
        table.add_column("Score", justify="right")
        table.add_column("Complete", justify="right")
        table.add_column("Unique", justify="right")
        table.add_column("Fresh", justify="right")
        table.add_column("Rules", justify="right")
        table.add_column("Anomalies", justify="right")
        for t in report["tables"]:
            if t.get("overall_score") is not None:
                table.add_row(
                    t["table"], str(t["overall_score"]),
                    str(t["completeness"]), str(t["uniqueness"]),
                    str(t["freshness"]), str(t["rule_compliance"]),
                    str(t["anomalies"]),
                )
            else:
                table.add_row(t["table"], "[red]error[/red]", "-", "-", "-", "-", "-")
        console.print(table)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@quality_group.command("history")
@click.argument("table_name")
def quality_history(table_name: str):
    """Show quality score history for a table."""
    from lakehouse.quality import get_quality_history
    history = get_quality_history(table_name)
    if not history:
        console.print("[dim]No quality history for this table.[/dim]")
        return

    table = Table(title=f"Quality History — {table_name}")
    table.add_column("Date", style="cyan")
    table.add_column("Score", justify="right")
    table.add_column("Complete", justify="right")
    table.add_column("Unique", justify="right")
    table.add_column("Fresh", justify="right")
    table.add_column("Rules", justify="right")
    for h in history:
        table.add_row(
            h["computed_at"][:19],
            str(h["overall_score"]),
            str(h["completeness"]),
            str(h["uniqueness"]),
            str(h["freshness"]),
            str(h["rule_compliance"]),
        )
    console.print(table)


@main.group()
def schema():
    """Schema evolution tracking commands."""
    pass


@schema.command("history")
@click.argument("table_name")
def schema_history(table_name: str):
    """Show schema evolution history for a table.

    Examples:
        lakehouse schema history my_table
    """
    from .schema_evolution import get_schema_history

    from .catalog import get_catalog
    catalog = get_catalog()
    history = get_schema_history(catalog, table_name)

    table = Table(title=f"Schema History: {table_name}")
    table.add_column("Schema ID", style="cyan")
    table.add_column("Snapshot", style="dim")
    table.add_column("Timestamp", style="dim")
    table.add_column("Fields")
    table.add_column("Change", style="yellow")

    for entry in history:
        fields = ", ".join(f["name"] for f in entry["fields"])
        snap = str(entry["snapshot_id"]) if entry["snapshot_id"] else "-"
        ts = entry["timestamp"][:19] if entry["timestamp"] else "-"
        change = entry["change_summary"] or "-"
        table.add_row(str(entry["schema_id"]), snap, ts, fields, change)

    console.print(table)


@schema.command("diff")
@click.argument("table_name")
@click.option("--from-snapshot", type=int, default=None, help="Starting snapshot ID")
@click.option("--to-snapshot", type=int, default=None, help="Ending snapshot ID")
def schema_diff_cmd(table_name: str, from_snapshot: int, to_snapshot: int):
    """Show schema diff between snapshots.

    Examples:
        lakehouse schema diff my_table
        lakehouse schema diff my_table --from-snapshot 123 --to-snapshot 456
    """
    from .schema_evolution import schema_diff

    from .catalog import get_catalog
    catalog = get_catalog()
    diff = schema_diff(catalog, table_name, from_snapshot=from_snapshot, to_snapshot=to_snapshot)

    console.print(f"\n[bold]Schema Diff:[/bold] {diff['table']}")
    console.print(f"  From schema {diff['from_schema_id']} → {diff['to_schema_id']}")
    console.print(f"  Summary: {diff['summary']}\n")

    if diff["added_columns"]:
        for c in diff["added_columns"]:
            console.print(f"  [green]+ {c['name']} ({c['type']})[/green]")
    if diff["dropped_columns"]:
        for c in diff["dropped_columns"]:
            console.print(f"  [red]- {c['name']} ({c['type']})[/red]")
    if diff["renamed_columns"]:
        for c in diff["renamed_columns"]:
            console.print(f"  [yellow]~ {c['old_name']} → {c['new_name']}[/yellow]")
    if diff["type_changes"]:
        for c in diff["type_changes"]:
            console.print(f"  [yellow]~ {c['name']}: {c['old_type']} → {c['new_type']}[/yellow]")


@schema.command("migrate")
@click.argument("table_name")
@click.option("--from-snapshot", type=int, default=None, help="Starting snapshot ID")
@click.option("--to-snapshot", type=int, default=None, help="Ending snapshot ID")
def schema_migrate(table_name: str, from_snapshot: int, to_snapshot: int):
    """Generate migration steps between schema versions.

    Examples:
        lakehouse schema migrate my_table
    """
    from .schema_evolution import generate_migration

    from .catalog import get_catalog
    catalog = get_catalog()
    result = generate_migration(catalog, table_name, from_snapshot=from_snapshot, to_snapshot=to_snapshot)

    console.print(f"\n[bold]{result['message']}[/bold]\n")

    if result["steps"]:
        table = Table(title="Migration Steps")
        table.add_column("#", style="cyan")
        table.add_column("Operation", style="yellow")
        table.add_column("Column")
        table.add_column("Details")

        for i, step in enumerate(result["steps"], 1):
            details = ""
            if step["operation"] == "add_column":
                details = f"type: {step['column_type']}"
            elif step["operation"] == "rename_column":
                details = f"→ {step['new_name']}"
            table.add_row(str(i), step["operation"], step["column_name"], details)

        console.print(table)
    else:
        console.print("  No migration steps needed.")


@schema.command("check")
@click.argument("table_name")
@click.option("--add", multiple=True, help="Add column: NAME:TYPE (e.g. email:string)")
@click.option("--drop", multiple=True, help="Drop column: NAME")
@click.option("--rename", multiple=True, help="Rename column: OLD:NEW")
def schema_check(table_name: str, add: tuple, drop: tuple, rename: tuple):
    """Check schema change compatibility.

    Examples:
        lakehouse schema check my_table --add email:string
        lakehouse schema check my_table --drop old_column
        lakehouse schema check my_table --rename name:full_name
    """
    from .schema_evolution import check_schema_compatibility

    proposed = []
    for a in add:
        parts = a.split(":", 1)
        proposed.append({"op": "add_column", "column": parts[0], "type": parts[1] if len(parts) > 1 else "string"})
    for d in drop:
        proposed.append({"op": "drop_column", "column": d})
    for r in rename:
        parts = r.split(":", 1)
        if len(parts) == 2:
            proposed.append({"op": "rename_column", "column": parts[0], "new_name": parts[1]})

    from .catalog import get_catalog
    catalog = get_catalog()
    result = check_schema_compatibility(catalog, table_name, proposed)

    if result["compatible"]:
        console.print(f"\n[green bold]Compatible[/green bold]: {result['message']}")
    else:
        console.print(f"\n[red bold]NOT Compatible[/red bold]: {result['message']}")

    if result["breaking_changes"]:
        console.print("\n[red]Breaking changes:[/red]")
        for bc in result["breaking_changes"]:
            console.print(f"  - {bc}")
    if result["warnings"]:
        console.print("\n[yellow]Warnings:[/yellow]")
        for w in result["warnings"]:
            console.print(f"  - {w}")


@main.group()
def retention():
    """Snapshot retention policy commands."""
    pass


@retention.command("set")
@click.argument("table_name")
@click.option("--max-age", type=float, default=None, help="Max snapshot age in hours")
@click.option("--max-count", type=int, default=None, help="Max number of snapshots to keep")
@click.option("--min-keep", type=int, default=1, help="Minimum snapshots to always keep (default: 1)")
def retention_set(table_name: str, max_age: float, max_count: int, min_keep: int):
    """Set a snapshot retention policy for a table.

    Examples:
        lakehouse retention set expenses --max-age 168 --max-count 10 --min-keep 2
    """
    from .retention import set_retention_policy

    policy = {"min_snapshots_to_keep": min_keep}
    if max_age is not None:
        policy["max_snapshot_age_hours"] = max_age
    if max_count is not None:
        policy["max_snapshot_count"] = max_count

    result = set_retention_policy(table_name, policy)
    console.print(f"[green]{result['message']}[/green]")
    p = result["policy"]
    if p.get("max_snapshot_age_hours"):
        console.print(f"  Max age: {p['max_snapshot_age_hours']} hours")
    if p.get("max_snapshot_count"):
        console.print(f"  Max count: {p['max_snapshot_count']}")
    console.print(f"  Min keep: {p['min_snapshots_to_keep']}")


@retention.command("show")
@click.argument("table_name")
def retention_show(table_name: str):
    """Show the retention policy for a table.

    Examples:
        lakehouse retention show expenses
    """
    from .retention import get_retention_policy

    result = get_retention_policy(table_name)
    if result["policy"] is None:
        console.print(f"No retention policy for '{table_name}'")
        return
    p = result["policy"]
    console.print(f"[bold]Retention Policy: {result['table']}[/bold]")
    if p.get("max_snapshot_age_hours"):
        console.print(f"  Max age: {p['max_snapshot_age_hours']} hours")
    if p.get("max_snapshot_count"):
        console.print(f"  Max count: {p['max_snapshot_count']}")
    console.print(f"  Min keep: {p['min_snapshots_to_keep']}")
    console.print(f"  Created: {p['created_at']}")
    console.print(f"  Last evaluated: {p.get('last_evaluated') or 'never'}")


@retention.command("list")
def retention_list():
    """List all retention policies.

    Examples:
        lakehouse retention list
    """
    from .retention import list_retention_policies

    policies = list_retention_policies()
    if not policies:
        console.print("No retention policies configured.")
        return

    table = Table(title="Retention Policies")
    table.add_column("Table", style="cyan")
    table.add_column("Max Age (hrs)")
    table.add_column("Max Count")
    table.add_column("Min Keep")
    table.add_column("Last Evaluated", style="dim")

    for p in policies:
        table.add_row(
            p["table"],
            str(p.get("max_snapshot_age_hours") or "-"),
            str(p.get("max_snapshot_count") or "-"),
            str(p.get("min_snapshots_to_keep", 1)),
            str(p.get("last_evaluated") or "never"),
        )
    console.print(table)


@retention.command("remove")
@click.argument("table_name")
def retention_remove(table_name: str):
    """Remove a retention policy.

    Examples:
        lakehouse retention remove expenses
    """
    from .retention import remove_retention_policy

    result = remove_retention_policy(table_name)
    console.print(result["message"])


@retention.command("run")
@click.argument("table_name", required=False)
@click.option("--dry-run", is_flag=True, help="Preview without acting")
def retention_run(table_name: str, dry_run: bool):
    """Enforce retention policies.

    Examples:
        lakehouse retention run
        lakehouse retention run expenses --dry-run
    """
    from .retention import evaluate_retention

    from .catalog import get_catalog
    catalog = get_catalog()
    results = evaluate_retention(catalog, table_name=table_name, dry_run=dry_run)

    if not results:
        console.print("No tables with retention policies to evaluate.")
        return

    for r in results:
        if r["action"] == "error":
            console.print(f"[red]{r['table']}: {r['message']}[/red]")
        elif r["action"] == "no_action":
            console.print(f"[dim]{r['table']}: {r['message']}[/dim]")
        elif r["action"] == "would_expire":
            console.print(f"[yellow]{r['table']}: {r['message']}[/yellow]")
        elif r["action"] == "expired":
            console.print(f"[green]{r['table']}: {r['message']}[/green]")


@main.group("catalog")
def catalog_group():
    """Data catalog enrichment commands."""
    pass


@catalog_group.command("describe-column")
@click.argument("table_name")
@click.argument("column_name")
@click.argument("description")
def catalog_describe_column(table_name: str, column_name: str, description: str):
    """Set a column description.

    Examples:
        lakehouse catalog describe-column expenses amount "Total expense amount in USD"
    """
    from .catalog_metadata import set_column_description

    result = set_column_description(table_name, column_name, description)
    console.print(f"[green]{result['message']}[/green]")


@catalog_group.command("column-descriptions")
@click.argument("table_name")
def catalog_column_descriptions(table_name: str):
    """Show column descriptions for a table.

    Examples:
        lakehouse catalog column-descriptions expenses
    """
    from .catalog_metadata import get_column_descriptions

    result = get_column_descriptions(table_name)
    if not result["descriptions"]:
        console.print(f"No descriptions for '{table_name}'")
        return
    table = Table(title=f"Column Descriptions: {result['table']}")
    table.add_column("Column", style="cyan")
    table.add_column("Description")
    for col, desc in result["descriptions"].items():
        table.add_row(col, desc)
    console.print(table)


@catalog_group.command("classify")
@click.argument("table_name")
@click.argument("column_name")
@click.argument("classification")
def catalog_classify(table_name: str, column_name: str, classification: str):
    """Classify a column (pii, financial, public, internal, confidential).

    Examples:
        lakehouse catalog classify users email pii
    """
    from .catalog_metadata import classify_column

    result = classify_column(table_name, column_name, classification)
    console.print(f"[green]{result['message']}[/green]")


@catalog_group.command("classifications")
@click.option("--table", default=None, help="Filter by table")
@click.option("--type", "cls_type", default=None, help="Filter by classification type")
def catalog_classifications(table: str, cls_type: str):
    """List data classifications.

    Examples:
        lakehouse catalog classifications --type pii
    """
    from .catalog_metadata import get_classifications

    results = get_classifications(table_name=table, classification=cls_type)
    if not results:
        console.print("No classifications found.")
        return
    tbl = Table(title="Data Classifications")
    tbl.add_column("Table", style="cyan")
    tbl.add_column("Column")
    tbl.add_column("Classification", style="yellow")
    for r in results:
        tbl.add_row(r["table"], r["column"], r["classification"])
    console.print(tbl)


@catalog_group.group("glossary")
def glossary_group():
    """Business glossary commands."""
    pass


@glossary_group.command("add")
@click.argument("term")
@click.argument("definition")
@click.option("--aliases", default=None, help="Comma-separated aliases")
def glossary_add(term: str, definition: str, aliases: str):
    """Add a glossary term.

    Examples:
        lakehouse catalog glossary add MRR "Monthly Recurring Revenue" --aliases "monthly revenue"
    """
    from .catalog_metadata import add_glossary_term

    alias_list = [a.strip() for a in aliases.split(",")] if aliases else None
    result = add_glossary_term(term, definition, aliases=alias_list)
    console.print(f"[green]{result['message']}[/green]")


@glossary_group.command("search")
@click.argument("query")
def glossary_search(query: str):
    """Search glossary terms.

    Examples:
        lakehouse catalog glossary search revenue
    """
    from .catalog_metadata import search_glossary

    results = search_glossary(query)
    if not results:
        console.print("No matching terms found.")
        return
    for r in results:
        aliases = f" (aliases: {', '.join(r['aliases'])})" if r["aliases"] else ""
        console.print(f"[bold]{r['term']}[/bold]: {r['definition']}{aliases}")


@glossary_group.command("list")
def glossary_list():
    """List all glossary terms.

    Examples:
        lakehouse catalog glossary list
    """
    from .catalog_metadata import list_glossary

    terms = list_glossary()
    if not terms:
        console.print("No glossary terms defined.")
        return
    table = Table(title="Business Glossary")
    table.add_column("Term", style="cyan")
    table.add_column("Definition")
    table.add_column("Aliases", style="dim")
    for t in terms:
        table.add_row(t["term"], t["definition"], ", ".join(t["aliases"]) or "-")
    console.print(table)


@glossary_group.command("remove")
@click.argument("term")
def glossary_remove(term: str):
    """Remove a glossary term.

    Examples:
        lakehouse catalog glossary remove MRR
    """
    from .catalog_metadata import remove_glossary_term

    result = remove_glossary_term(term)
    console.print(result["message"])


@catalog_group.command("enriched-schema")
@click.argument("table_name")
def catalog_enriched_schema(table_name: str):
    """Show enriched schema with descriptions, classifications, and glossary.

    Examples:
        lakehouse catalog enriched-schema expenses
    """
    from .catalog_metadata import get_enriched_schema

    from .catalog import get_catalog
    catalog = get_catalog()
    result = get_enriched_schema(catalog, table_name)

    table = Table(title=f"Enriched Schema: {result['table']}")
    table.add_column("Field", style="cyan")
    table.add_column("Type")
    table.add_column("Description")
    table.add_column("Classification", style="yellow")
    table.add_column("Glossary", style="dim")

    for f in result["fields"]:
        table.add_row(
            f["name"],
            f["type"],
            f["description"] or "-",
            f["classification"] or "-",
            ", ".join(f["glossary_matches"]) or "-",
        )
    console.print(table)
    console.print(f"\n{result['described_fields']}/{result['total_fields']} described, "
                  f"{result['classified_fields']}/{result['total_fields']} classified")


@main.group()
def mask():
    """Data masking policy commands."""
    pass


@mask.command("add")
@click.argument("table_name")
@click.argument("column_name")
@click.argument("strategy")
@click.option("--replacement", default=None, help="Replacement text for redact strategy")
@click.option("--length", type=int, default=None, help="Keep first N chars for truncate strategy")
@click.option("--sql", "sql_expr", default=None, help="SQL expression for expression strategy")
def mask_add(table_name: str, column_name: str, strategy: str, replacement: str, length: int, sql_expr: str):
    """Add a masking policy to a column.

    Examples:
        lakehouse mask add users email hash
        lakehouse mask add users name redact --replacement "***"
        lakehouse mask add users ssn truncate --length 3
    """
    from .masking import add_masking_policy

    options = {}
    if replacement:
        options["replacement"] = replacement
    if length is not None:
        options["length"] = length
    if sql_expr:
        options["sql"] = sql_expr

    result = add_masking_policy(table_name, column_name, strategy, options=options or None)
    console.print(f"[green]{result['message']}[/green]")


@mask.command("list")
@click.option("--table", default=None, help="Filter by table")
def mask_list(table: str):
    """List masking policies.

    Examples:
        lakehouse mask list
        lakehouse mask list --table users
    """
    from .masking import list_masking_policies

    policies = list_masking_policies(table_name=table)
    if not policies:
        console.print("No masking policies configured.")
        return

    tbl = Table(title="Masking Policies")
    tbl.add_column("Table", style="cyan")
    tbl.add_column("Column")
    tbl.add_column("Strategy", style="yellow")
    tbl.add_column("Options", style="dim")
    for p in policies:
        opts = ", ".join(f"{k}={v}" for k, v in p["options"].items()) if p["options"] else "-"
        tbl.add_row(p["table"], p["column"], p["strategy"], opts)
    console.print(tbl)


@mask.command("remove")
@click.argument("table_name")
@click.argument("column_name")
def mask_remove(table_name: str, column_name: str):
    """Remove a masking policy.

    Examples:
        lakehouse mask remove users email
    """
    from .masking import remove_masking_policy

    result = remove_masking_policy(table_name, column_name)
    console.print(result["message"])


@mask.command("preview")
@click.argument("table_name")
@click.option("--rows", type=int, default=5, help="Number of rows to preview")
def mask_preview(table_name: str, rows: int):
    """Preview a table with masking applied.

    Examples:
        lakehouse mask preview users --rows 5
    """
    from .masking import preview_masking

    from .catalog import get_catalog
    catalog = get_catalog()
    result = preview_masking(catalog, table_name, max_rows=rows)

    console.print(f"\n[bold]Masking Preview: {result['table']}[/bold] ({result['policies_applied']} policies)\n")

    if result["masked"]:
        cols = list(result["masked"][0].keys())
        tbl = Table(title="Masked Data")
        for col in cols:
            tbl.add_column(col)
        for row in result["masked"]:
            tbl.add_row(*[str(row.get(c, "")) for c in cols])
        console.print(tbl)


@mask.command("query")
@click.argument("sql")
def mask_query(sql: str):
    """Execute a query with masking applied.

    Examples:
        lakehouse mask query "SELECT * FROM users"
    """
    from .masking import query_with_masking
    from .query import QueryEngine

    engine = QueryEngine()
    df = query_with_masking(engine, sql)
    console.print(df.to_string())


@main.group()
def watermark():
    """Watermark tracking commands for incremental processing."""
    pass


@watermark.command("set")
@click.argument("pipeline_name")
@click.argument("table_name")
@click.argument("snapshot_id", type=int)
def watermark_set(pipeline_name: str, table_name: str, snapshot_id: int):
    """Set a watermark for a pipeline/table pair.

    Examples:
        lakehouse watermark set etl_daily raw_events 1234567890
    """
    from .incremental import set_watermark

    result = set_watermark(pipeline_name, table_name, snapshot_id)
    console.print(f"[green]{result['message']}[/green]")


@watermark.command("show")
@click.argument("pipeline_name")
@click.argument("table_name", required=False)
def watermark_show(pipeline_name: str, table_name: str):
    """Show watermarks for a pipeline.

    Examples:
        lakehouse watermark show etl_daily
    """
    from .incremental import list_watermarks, get_watermark

    if table_name:
        result = get_watermark(pipeline_name, table_name)
        if result["snapshot_id"] is None:
            console.print(f"No watermark for '{pipeline_name}/{table_name}'")
        else:
            console.print(f"[bold]{result['pipeline']}/{result['table']}[/bold]")
            console.print(f"  Snapshot: {result['snapshot_id']}")
            console.print(f"  Processed at: {result.get('processed_at', 'unknown')}")
            console.print(f"  Rows: {result.get('rows_processed', 0)}")
    else:
        results = list_watermarks(pipeline_name=pipeline_name)
        if not results:
            console.print(f"No watermarks for pipeline '{pipeline_name}'")
            return
        table = Table(title=f"Watermarks: {pipeline_name}")
        table.add_column("Table", style="cyan")
        table.add_column("Snapshot")
        table.add_column("Processed At", style="dim")
        table.add_column("Rows")
        for r in results:
            table.add_row(r["table"], str(r["snapshot_id"]), str(r.get("processed_at", "")[:19]), str(r.get("rows_processed", 0)))
        console.print(table)


@watermark.command("list")
def watermark_list():
    """List all watermarks.

    Examples:
        lakehouse watermark list
    """
    from .incremental import list_watermarks

    results = list_watermarks()
    if not results:
        console.print("No watermarks configured.")
        return
    table = Table(title="All Watermarks")
    table.add_column("Pipeline", style="cyan")
    table.add_column("Table")
    table.add_column("Snapshot")
    table.add_column("Processed At", style="dim")
    for r in results:
        table.add_row(r["pipeline"], r["table"], str(r["snapshot_id"]), str(r.get("processed_at", "")[:19]))
    console.print(table)


@watermark.command("reset")
@click.argument("pipeline_name")
@click.option("--table", default=None, help="Reset only for a specific table")
def watermark_reset(pipeline_name: str, table: str):
    """Reset watermarks to force full reprocessing.

    Examples:
        lakehouse watermark reset etl_daily
        lakehouse watermark reset etl_daily --table raw_events
    """
    from .incremental import reset_watermark

    result = reset_watermark(pipeline_name, table_name=table)
    console.print(result["message"])


@main.group()
def sla():
    """Table SLA monitoring commands."""
    pass


@sla.command("set")
@click.argument("table_name")
@click.option("--max-staleness", type=float, default=None, help="Max hours since last update")
@click.option("--min-quality", type=float, default=None, help="Minimum quality score 0-100")
@click.option("--min-rows", type=int, default=None, help="Minimum row count")
@click.option("--max-null-pct", type=float, default=None, help="Max null percentage per column")
def sla_set(table_name: str, max_staleness: float, min_quality: float, min_rows: int, max_null_pct: float):
    """Set SLA thresholds for a table.

    Examples:
        lakehouse sla set expenses --max-staleness 24 --min-quality 80 --min-rows 100
    """
    from .sla import set_sla

    sla_def = {}
    if max_staleness is not None:
        sla_def["max_staleness_hours"] = max_staleness
    if min_quality is not None:
        sla_def["min_quality_score"] = min_quality
    if min_rows is not None:
        sla_def["min_row_count"] = min_rows
    if max_null_pct is not None:
        sla_def["max_null_pct"] = max_null_pct

    result = set_sla(table_name, sla_def)
    console.print(f"[green]{result['message']}[/green]")


@sla.command("show")
@click.argument("table_name")
def sla_show(table_name: str):
    """Show SLA for a table.

    Examples:
        lakehouse sla show expenses
    """
    from .sla import get_sla

    result = get_sla(table_name)
    if result["sla"] is None:
        console.print(f"No SLA for '{table_name}'")
        return
    s = result["sla"]
    console.print(f"[bold]SLA: {result['table']}[/bold]")
    if s.get("max_staleness_hours"):
        console.print(f"  Max staleness: {s['max_staleness_hours']}h")
    if s.get("min_quality_score"):
        console.print(f"  Min quality: {s['min_quality_score']}")
    if s.get("min_row_count") is not None:
        console.print(f"  Min rows: {s['min_row_count']}")
    if s.get("max_null_pct") is not None:
        console.print(f"  Max null %: {s['max_null_pct']}%")


@sla.command("list")
def sla_list():
    """List all SLA definitions.

    Examples:
        lakehouse sla list
    """
    from .sla import list_slas

    slas = list_slas()
    if not slas:
        console.print("No SLAs configured.")
        return
    table = Table(title="SLA Definitions")
    table.add_column("Table", style="cyan")
    table.add_column("Max Staleness")
    table.add_column("Min Quality")
    table.add_column("Min Rows")
    table.add_column("Max Null %")
    for s in slas:
        table.add_row(
            s["table"],
            f"{s.get('max_staleness_hours', '-')}h" if s.get("max_staleness_hours") else "-",
            str(s.get("min_quality_score") or "-"),
            str(s.get("min_row_count") if s.get("min_row_count") is not None else "-"),
            f"{s.get('max_null_pct', '-')}%" if s.get("max_null_pct") is not None else "-",
        )
    console.print(table)


@sla.command("remove")
@click.argument("table_name")
def sla_remove(table_name: str):
    """Remove an SLA.

    Examples:
        lakehouse sla remove expenses
    """
    from .sla import remove_sla

    result = remove_sla(table_name)
    console.print(result["message"])


@sla.command("check")
@click.argument("table_name", required=False)
def sla_check(table_name: str):
    """Check SLA compliance.

    Examples:
        lakehouse sla check
        lakehouse sla check expenses
    """
    from .sla import check_sla

    from .catalog import get_catalog
    catalog = get_catalog()
    result = check_sla(catalog, table_name=table_name)

    console.print(f"\n[bold]SLA Check: {result['message']}[/bold]\n")
    for t in result["tables"]:
        if t["status"] == "passing":
            console.print(f"  [green]{t['table']}: PASSING[/green]")
        elif t["status"] == "warning":
            console.print(f"  [yellow]{t['table']}: WARNING[/yellow]")
            for w in t["warnings"]:
                console.print(f"    - {w}")
        elif t["status"] == "violation":
            console.print(f"  [red]{t['table']}: VIOLATION[/red]")
            for v in t["violations"]:
                console.print(f"    - {v}")
            if t.get("recommendations"):
                for r in t["recommendations"]:
                    console.print(f"    [dim]→ {r}[/dim]")


@sla.command("history")
@click.argument("table_name")
def sla_history(table_name: str):
    """Show SLA check history.

    Examples:
        lakehouse sla history expenses
    """
    from .sla import get_sla_history

    history = get_sla_history(table_name)
    if not history:
        console.print(f"No SLA history for '{table_name}'")
        return
    table = Table(title=f"SLA History: {table_name}")
    table.add_column("Checked At", style="dim")
    table.add_column("Status")
    table.add_column("Violations")
    for h in history:
        status_style = {"passing": "green", "warning": "yellow", "violation": "red"}.get(h["status"], "")
        table.add_row(
            h["checked_at"][:19],
            f"[{status_style}]{h['status'].upper()}[/{status_style}]",
            str(len(h.get("violations", []))),
        )
    console.print(table)


@main.group()
def optimize():
    """Query optimization advisor commands."""
    pass


@optimize.command("patterns")
@click.option("--limit", "-n", default=100, help="Max history entries to analyze")
def optimize_patterns(limit: int):
    """Analyze query patterns from history.

    Examples:
        lakehouse optimize patterns
        lakehouse optimize patterns --limit 50
    """
    from .optimizer import analyze_query_patterns

    result = analyze_query_patterns(limit=limit)
    console.print(f"[bold]{result['message']}[/bold]\n")

    if result["frequent_tables"]:
        table = Table(title="Frequent Tables")
        table.add_column("Table", style="cyan")
        table.add_column("Queries", style="green")
        for t in result["frequent_tables"]:
            table.add_row(t["table"], str(t["count"]))
        console.print(table)

    if result["frequent_filters"]:
        table = Table(title="Frequent Filter Columns")
        table.add_column("Column", style="cyan")
        table.add_column("Used", style="green")
        for f in result["frequent_filters"]:
            table.add_row(f["column"], str(f["count"]))
        console.print(table)

    if result["repeated_queries"]:
        table = Table(title="Repeated Queries")
        table.add_column("SQL Pattern", style="yellow", max_width=60)
        table.add_column("Count", style="green")
        for rq in result["repeated_queries"]:
            table.add_row(rq["sql_pattern"][:60], str(rq["count"]))
        console.print(table)

    if result["slow_queries"]:
        table = Table(title="Slow Queries (>p90)")
        table.add_column("SQL", style="red", max_width=60)
        table.add_column("Duration (ms)", style="yellow")
        for sq in result["slow_queries"]:
            table.add_row(sq["sql"][:60], str(sq["duration_ms"]))
        console.print(table)


@optimize.command("partitions")
@click.argument("table_name")
def optimize_partitions(table_name: str):
    """Suggest partitioning strategies for a table.

    Examples:
        lakehouse optimize partitions expenses
    """
    from .catalog import get_catalog
    from .optimizer import suggest_partitions

    catalog = get_catalog()
    suggestions = suggest_partitions(catalog, table_name)

    if not suggestions:
        console.print("[dim]No partition suggestions for this table.[/dim]")
        return

    table = Table(title=f"Partition Suggestions for '{table_name}'")
    table.add_column("Column", style="cyan")
    table.add_column("Unique Values", style="green")
    table.add_column("Filter Frequency", style="yellow")
    table.add_column("Benefit", style="bold")
    table.add_column("Rationale", max_width=50)
    for s in suggestions:
        benefit_style = {"high": "green", "medium": "yellow", "low": "red"}.get(s["benefit"], "white")
        table.add_row(
            s["column"],
            str(s["unique_values"]),
            str(s["filter_frequency"]),
            f"[{benefit_style}]{s['benefit']}[/{benefit_style}]",
            s["rationale"],
        )
    console.print(table)


@optimize.command("materializations")
@click.option("--limit", "-n", default=100, help="Max history entries to analyze")
def optimize_materializations(limit: int):
    """Suggest materialized views based on repeated expensive queries.

    Examples:
        lakehouse optimize materializations
    """
    from .catalog import get_catalog
    from .optimizer import suggest_materializations

    catalog = get_catalog()
    suggestions = suggest_materializations(catalog, limit=limit)

    if not suggestions:
        console.print("[dim]No materialization suggestions.[/dim]")
        return

    table = Table(title="Materialization Suggestions")
    table.add_column("SQL Pattern", style="yellow", max_width=60)
    table.add_column("Run Count", style="green")
    table.add_column("Has Agg", style="cyan")
    table.add_column("Has Join", style="cyan")
    for s in suggestions:
        table.add_row(
            s["sql"][:60],
            str(s["run_count"]),
            "Yes" if s["has_aggregation"] else "No",
            "Yes" if s["has_join"] else "No",
        )
    console.print(table)


@optimize.command("report")
def optimize_report():
    """Generate a comprehensive optimization report.

    Examples:
        lakehouse optimize report
    """
    from .catalog import get_catalog
    from .optimizer import get_optimization_report

    catalog = get_catalog()
    result = get_optimization_report(catalog)

    score = result["optimization_score"]
    score_style = "green" if score >= 80 else "yellow" if score >= 50 else "red"
    console.print(Panel(
        f"[{score_style} bold]{score}/100[/{score_style} bold]",
        title="Optimization Score",
    ))
    console.print(f"[bold]{result['message']}[/bold]\n")

    if result["partition_suggestions"]:
        console.print(f"[yellow]Partition suggestions: {len(result['partition_suggestions'])}[/yellow]")
        for s in result["partition_suggestions"]:
            console.print(f"  • {s['rationale']}")

    if result["materialization_suggestions"]:
        console.print(f"[yellow]Materialization suggestions: {len(result['materialization_suggestions'])}[/yellow]")
        for s in result["materialization_suggestions"]:
            console.print(f"  • {s['rationale']}")

    if result["slow_queries"]:
        console.print(f"[red]Slow queries: {len(result['slow_queries'])}[/red]")
        for sq in result["slow_queries"]:
            console.print(f"  • {sq['sql'][:80]} ({sq['duration_ms']}ms)")


@optimize.command("cost")
@click.argument("sql")
def optimize_cost(sql: str):
    """Estimate the cost of a SQL query.

    Examples:
        lakehouse optimize cost "SELECT * FROM expenses WHERE amount > 100"
    """
    from .catalog import get_catalog
    from .optimizer import estimate_query_cost

    catalog = get_catalog()
    result = estimate_query_cost(catalog, sql)

    console.print(f"[bold]{result['message']}[/bold]\n")

    table = Table(title="Query Cost Estimate")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Complexity", result["complexity"])
    table.add_row("Estimated Rows Scanned", f"{result['estimated_rows_scanned']:,}")
    table.add_row("Total Source Rows", f"{result['total_source_rows']:,}")
    table.add_row("Has Filter", "Yes" if result["has_filter"] else "No")
    table.add_row("Has Join", "Yes" if result["has_join"] else "No")
    table.add_row("Has Aggregation", "Yes" if result["has_aggregation"] else "No")
    console.print(table)

    if result["tables_involved"]:
        t2 = Table(title="Tables Involved")
        t2.add_column("Table", style="cyan")
        t2.add_column("Estimated Rows", style="green")
        t2.add_column("Size (bytes)", style="yellow")
        for td in result["tables_involved"]:
            t2.add_row(td["table"], f"{td['estimated_rows']:,}", f"{td['size_bytes']:,}")
        console.print(t2)


@main.group()
def backup():
    """Backup and restore commands."""
    pass


@backup.command("create")
@click.argument("table_name")
@click.option("--output", "-o", default=None, help="Output directory")
def backup_create(table_name: str, output: str):
    """Backup a table to a compressed archive.

    Examples:
        lakehouse backup create expenses
        lakehouse backup create expenses --output /tmp/backups
    """
    from pathlib import Path
    from .catalog import get_catalog
    from .backup import backup_table

    catalog = get_catalog()
    output_dir = Path(output) if output else None
    result = backup_table(catalog, table_name, output_dir=output_dir)
    console.print(f"[green]{result['message']}[/green]")


@backup.command("create-ns")
@click.argument("namespace")
@click.option("--output", "-o", default=None, help="Output directory")
def backup_create_ns(namespace: str, output: str):
    """Backup all tables in a namespace.

    Examples:
        lakehouse backup create-ns default
    """
    from pathlib import Path
    from .catalog import get_catalog
    from .backup import backup_namespace

    catalog = get_catalog()
    output_dir = Path(output) if output else None
    result = backup_namespace(catalog, namespace, output_dir=output_dir)
    console.print(f"[green]{result['message']}[/green]")
    for tbl in result.get("tables", []):
        console.print(f"  • {tbl}")


@backup.command("restore")
@click.argument("archive")
@click.option("--name", default=None, help="Rename table on restore")
@click.option("--overwrite", is_flag=True, help="Overwrite existing table")
def backup_restore(archive: str, name: str, overwrite: bool):
    """Restore a table from a backup archive.

    Examples:
        lakehouse backup restore ~/.lakehouse/backups/expenses_20260215.tar.gz
        lakehouse backup restore archive.tar.gz --name new_expenses --overwrite
    """
    from .catalog import get_catalog
    from .backup import restore_table

    catalog = get_catalog()
    result = restore_table(catalog, archive, table_name=name, overwrite=overwrite)
    console.print(f"[green]{result['message']}[/green]")


@backup.command("list")
@click.option("--dir", "backup_dir", default=None, help="Backup directory")
def backup_list(backup_dir: str):
    """List available backup archives.

    Examples:
        lakehouse backup list
    """
    from pathlib import Path
    from .backup import list_backups

    dir_path = Path(backup_dir) if backup_dir else None
    backups = list_backups(dir_path)

    if not backups:
        console.print("[dim]No backups found.[/dim]")
        return

    table = Table(title="Available Backups")
    table.add_column("File", style="cyan")
    table.add_column("Table", style="green")
    table.add_column("Rows", style="yellow")
    table.add_column("Size", style="bold")
    table.add_column("Date", style="dim")
    for b in backups:
        table.add_row(
            b["file"],
            b.get("table", "?"),
            str(b.get("row_count", "?")),
            f"{b['size_bytes']:,}",
            b.get("backed_up_at", "")[:19],
        )
    console.print(table)


@backup.command("verify")
@click.argument("archive")
def backup_verify(archive: str):
    """Verify backup archive integrity.

    Examples:
        lakehouse backup verify ~/.lakehouse/backups/expenses_20260215.tar.gz
    """
    from .backup import verify_backup

    result = verify_backup(archive)
    if result["valid"]:
        console.print(f"[green]{result['message']}[/green]")
        for t in result["tables_verified"]:
            console.print(f"  ✓ {t}")
    else:
        console.print(f"[red]{result['message']}[/red]")
        for issue in result["issues"]:
            console.print(f"  ✗ {issue}")


@main.group()
def cdc():
    """Change data capture commands."""
    pass


@cdc.command("changes")
@click.argument("table_name")
@click.option("--from", "from_snap", default=None, help="From snapshot ID")
@click.option("--to", "to_snap", default=None, help="To snapshot ID")
@click.option("--keys", default=None, help="Comma-separated key columns for update detection")
def cdc_changes(table_name: str, from_snap: str, to_snap: str, keys: str):
    """Show row-level changes between snapshots.

    Examples:
        lakehouse cdc changes expenses
        lakehouse cdc changes expenses --from 123 --to 456 --keys id
    """
    from .catalog import get_catalog
    from .cdc import get_changes

    catalog = get_catalog()
    key_columns = [k.strip() for k in keys.split(",")] if keys else None
    result = get_changes(catalog, table_name, from_snapshot=from_snap, to_snapshot=to_snap, key_columns=key_columns)

    console.print(f"[bold]{result['message']}[/bold]\n")

    if not result["changes"]:
        console.print("[dim]No changes detected.[/dim]")
        return

    table = Table(title=f"Changes ({result['from_snapshot']} → {result['to_snapshot']})")
    table.add_column("Type", style="bold")
    table.add_column("Details", max_width=80)
    for c in result["changes"][:50]:
        if c["type"] == "INSERT":
            style = "green"
            details = str(c["row"])
        elif c["type"] == "DELETE":
            style = "red"
            details = str(c["row"])
        else:
            style = "yellow"
            details = f"key={c.get('key', {})} changed={c.get('changed_columns', [])}"
        table.add_row(f"[{style}]{c['type']}[/{style}]", details[:80])
    console.print(table)

    s = result["summary"]
    console.print(f"\n[green]+{s['inserts']} inserts[/green]  [yellow]~{s['updates']} updates[/yellow]  [red]-{s['deletes']} deletes[/red]")


@cdc.command("log")
@click.argument("table_name")
@click.option("--limit", "-n", default=20, help="Max entries")
@click.option("--keys", default=None, help="Comma-separated key columns")
def cdc_log(table_name: str, limit: int, keys: str):
    """Show change log across snapshots.

    Examples:
        lakehouse cdc log expenses --limit 10
    """
    from .catalog import get_catalog
    from .cdc import get_change_log

    catalog = get_catalog()
    key_columns = [k.strip() for k in keys.split(",")] if keys else None
    log = get_change_log(catalog, table_name, limit=limit, key_columns=key_columns)

    if not log:
        console.print("[dim]No change history.[/dim]")
        return

    table = Table(title="Change Log")
    table.add_column("Timestamp", style="cyan")
    table.add_column("Operation", style="yellow")
    table.add_column("Inserts", style="green")
    table.add_column("Updates", style="yellow")
    table.add_column("Deletes", style="red")
    table.add_column("Total", style="bold")
    for entry in log:
        s = entry["summary"]
        table.add_row(
            entry["timestamp"][:19],
            entry.get("operation", ""),
            str(s["inserts"]),
            str(s["updates"]),
            str(s["deletes"]),
            str(entry["change_count"]),
        )
    console.print(table)


@cdc.command("summary")
@click.argument("table_name")
@click.option("--from", "from_snap", default=None, help="From snapshot ID")
@click.option("--to", "to_snap", default=None, help="To snapshot ID")
@click.option("--keys", default=None, help="Comma-separated key columns")
def cdc_summary(table_name: str, from_snap: str, to_snap: str, keys: str):
    """Show change summary between snapshots.

    Examples:
        lakehouse cdc summary expenses
    """
    from .catalog import get_catalog
    from .cdc import get_change_summary

    catalog = get_catalog()
    key_columns = [k.strip() for k in keys.split(",")] if keys else None
    result = get_change_summary(catalog, table_name, from_snapshot=from_snap, to_snapshot=to_snap, key_columns=key_columns)

    console.print(f"[bold]{result['message']}[/bold]\n")

    table = Table(title="Change Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Inserts", str(result["inserts"]))
    table.add_row("Updates", str(result["updates"]))
    table.add_row("Deletes", str(result["deletes"]))
    table.add_row("Total Changes", str(result["total_changes"]))
    table.add_row("Affected Columns", ", ".join(result["affected_columns"]))
    console.print(table)


@cdc.command("export")
@click.argument("table_name")
@click.option("--from", "from_snap", required=True, help="From snapshot ID")
@click.option("--to", "to_snap", required=True, help="To snapshot ID")
@click.option("--format", "fmt", default="json", type=click.Choice(["json", "csv"]), help="Export format")
@click.option("--output", "-o", default=None, help="Output file (default: stdout)")
@click.option("--keys", default=None, help="Comma-separated key columns")
def cdc_export(table_name: str, from_snap: str, to_snap: str, fmt: str, output: str, keys: str):
    """Export changes to JSON or CSV.

    Examples:
        lakehouse cdc export expenses --from 123 --to 456 --format json -o changes.json
    """
    from .catalog import get_catalog
    from .cdc import export_changes

    catalog = get_catalog()
    key_columns = [k.strip() for k in keys.split(",")] if keys else None
    data = export_changes(catalog, table_name, from_snap, to_snap, format=fmt, key_columns=key_columns)

    if output:
        from pathlib import Path
        Path(output).write_text(data)
        console.print(f"[green]Exported to {output}[/green]")
    else:
        console.print(data)


@main.group()
def dedup():
    """Data deduplication commands."""
    pass


@dedup.command("find")
@click.argument("table_name")
@click.option("--keys", default=None, help="Comma-separated key columns")
@click.option("--limit", "-n", default=100, help="Max duplicate groups")
def dedup_find(table_name: str, keys: str, limit: int):
    """Find duplicate rows in a table.

    Examples:
        lakehouse dedup find expenses --keys id
    """
    from .catalog import get_catalog
    from .dedup import find_duplicates

    catalog = get_catalog()
    key_columns = [k.strip() for k in keys.split(",")] if keys else None
    result = find_duplicates(catalog, table_name, key_columns=key_columns, limit=limit)

    console.print(f"[bold]{result['message']}[/bold]\n")

    if result["duplicates"]:
        table = Table(title="Duplicate Groups")
        for col in result["key_columns"]:
            table.add_column(col, style="cyan")
        table.add_column("Count", style="red")
        for d in result["duplicates"]:
            row = [str(d.get(c, "")) for c in result["key_columns"]] + [str(d["_dup_count"])]
            table.add_row(*row)
        console.print(table)


@dedup.command("summary")
@click.argument("table_name")
@click.option("--keys", default=None, help="Comma-separated key columns")
def dedup_summary_cmd(table_name: str, keys: str):
    """Show deduplication summary.

    Examples:
        lakehouse dedup summary expenses --keys id
    """
    from .catalog import get_catalog
    from .dedup import dedup_summary

    catalog = get_catalog()
    key_columns = [k.strip() for k in keys.split(",")] if keys else None
    result = dedup_summary(catalog, table_name, key_columns=key_columns)

    console.print(f"[bold]{result['message']}[/bold]\n")

    table = Table(title="Dedup Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Total Rows", str(result["total_rows"]))
    table.add_row("Unique Rows", str(result["unique_rows"]))
    table.add_row("Duplicate Rows", str(result["duplicate_rows"]))
    table.add_row("Duplicate %", f"{result['duplicate_pct']:.1f}%")
    console.print(table)


@dedup.command("remove")
@click.argument("table_name")
@click.option("--keys", default=None, help="Comma-separated key columns")
@click.option("--keep", default="first", type=click.Choice(["first", "last"]), help="Keep first or last occurrence")
@click.option("--dry-run/--no-dry-run", default=True, help="Preview changes without applying")
def dedup_remove(table_name: str, keys: str, keep: str, dry_run: bool):
    """Remove duplicate rows.

    Examples:
        lakehouse dedup remove expenses --keys id --dry-run
        lakehouse dedup remove expenses --keys id --no-dry-run --keep first
    """
    from .catalog import get_catalog
    from .dedup import remove_duplicates

    catalog = get_catalog()
    key_columns = [k.strip() for k in keys.split(",")] if keys else None
    result = remove_duplicates(catalog, table_name, key_columns=key_columns, keep=keep, dry_run=dry_run)

    style = "yellow" if dry_run else "green"
    console.print(f"[{style}]{result['message']}[/{style}]")


@dedup.command("report")
@click.argument("table_name")
@click.option("--keys", default=None, help="Comma-separated key columns")
def dedup_report_cmd(table_name: str, keys: str):
    """Generate comprehensive dedup report.

    Examples:
        lakehouse dedup report expenses --keys id
    """
    from .catalog import get_catalog
    from .dedup import dedup_report

    catalog = get_catalog()
    key_columns = [k.strip() for k in keys.split(",")] if keys else None
    result = dedup_report(catalog, table_name, key_columns=key_columns)

    console.print(f"[bold]{result['message']}[/bold]\n")

    table = Table(title="Column Uniqueness Analysis")
    table.add_column("Column", style="cyan")
    table.add_column("Unique Values", style="green")
    table.add_column("Uniqueness %", style="yellow")
    table.add_column("Good Key?", style="bold")
    for col in result["column_analysis"]:
        table.add_row(
            col["column"],
            str(col["unique_values"]),
            f"{col['uniqueness_pct']:.1f}%",
            "[green]Yes[/green]" if col["good_dedup_key"] else "[red]No[/red]",
        )
    console.print(table)

    if result["suggested_keys"]:
        console.print(f"\n[bold]Suggested dedup keys:[/bold] {', '.join(result['suggested_keys'])}")


@main.group()
def notify():
    """Manage event notifications and handlers."""
    pass


main.add_command(notify)


@notify.command("add")
@click.argument("table_name")
@click.option("--event", required=True, type=click.Choice(["write", "schema_change", "sla_violation", "maintenance", "all"]))
@click.option("--type", "handler_type", required=True, type=click.Choice(["webhook", "shell", "log"]))
@click.option("--url", default=None, help="Webhook URL (for webhook type)")
@click.option("--command", default=None, help="Shell command (for shell type)")
@click.option("--file", "log_file", default=None, help="Log file path (for log type)")
def notify_add(table_name: str, event: str, handler_type: str, url: str, command: str, log_file: str):
    """Register a notification handler for table events."""
    from .notifications import register_handler

    config = {}
    if handler_type == "webhook":
        if not url:
            raise click.UsageError("--url required for webhook handler")
        config["url"] = url
    elif handler_type == "shell":
        if not command:
            raise click.UsageError("--command required for shell handler")
        config["command"] = command
    elif handler_type == "log":
        if not log_file:
            raise click.UsageError("--file required for log handler")
        config["file"] = log_file

    result = register_handler(table_name, event, handler_type, config)
    console.print(f"[green]{result['message']}[/green]")
    console.print(f"Handler ID: [bold]{result['handler_id']}[/bold]")


@notify.command("list")
@click.option("--table", default=None, help="Filter by table name")
def notify_list(table: str):
    """List registered notification handlers."""
    from .notifications import list_handlers

    handlers = list_handlers(table_name=table)
    if not handlers:
        console.print("[yellow]No handlers registered[/yellow]")
        return

    table_obj = Table(title="Notification Handlers")
    table_obj.add_column("ID", style="cyan")
    table_obj.add_column("Table")
    table_obj.add_column("Event")
    table_obj.add_column("Type")
    table_obj.add_column("Config")
    table_obj.add_column("Created")

    for h in handlers:
        config_str = json.dumps(h["config"], default=str)[:50]
        table_obj.add_row(
            h["handler_id"],
            h["table"],
            h["event_type"],
            h["handler_type"],
            config_str,
            h.get("created_at", "")[:19],
        )
    console.print(table_obj)


@notify.command("remove")
@click.argument("handler_id")
def notify_remove(handler_id: str):
    """Remove a registered handler."""
    from .notifications import remove_handler

    result = remove_handler(handler_id)
    console.print(result["message"])


@notify.command("history")
@click.option("--table", default=None, help="Filter by table name")
@click.option("--event", default=None, help="Filter by event type")
@click.option("--limit", default=20, help="Max entries to show")
def notify_history(table: str, event: str, limit: int):
    """Show event notification history."""
    from .notifications import get_event_history

    history = get_event_history(table_name=table, event_type=event, limit=limit)
    if not history:
        console.print("[yellow]No event history[/yellow]")
        return

    table_obj = Table(title="Event History")
    table_obj.add_column("Time", style="cyan")
    table_obj.add_column("Table")
    table_obj.add_column("Event")
    table_obj.add_column("Handlers")

    for entry in history:
        table_obj.add_row(
            entry.get("fired_at", "")[:19],
            entry["table"],
            entry["event_type"],
            str(entry["handlers_triggered"]),
        )
    console.print(table_obj)


@notify.command("test")
@click.argument("handler_id")
def notify_test(handler_id: str):
    """Send a test event to a handler."""
    from .notifications import send_test_event

    result = send_test_event(handler_id)
    status = result.get("result", {}).get("status", "unknown")
    if status == "success":
        console.print(f"[green]{result['message']}[/green]")
    else:
        console.print(f"[red]{result['message']}[/red]")


@main.group()
def cache():
    """Manage query result cache."""
    pass


main.add_command(cache)


@cache.command("stats")
def cache_stats():
    """Show cache statistics."""
    from .query_cache import get_cache_stats

    stats = get_cache_stats()
    console.print(Panel(
        f"Entries: {stats['total_entries']}  |  "
        f"In-memory: {stats['in_memory_entries']}  |  "
        f"Hits: {stats['hits']}  |  "
        f"Misses: {stats['misses']}  |  "
        f"Hit rate: {stats['hit_rate']}%",
        title="Query Cache Stats",
    ))


@cache.command("list")
@click.option("--limit", default=20, help="Max entries to show")
def cache_list(limit: int):
    """List cached queries."""
    from .query_cache import list_cached_queries

    entries = list_cached_queries(limit=limit)
    if not entries:
        console.print("[yellow]No cached queries[/yellow]")
        return

    table = Table(title="Cached Queries")
    table.add_column("Key", style="cyan")
    table.add_column("SQL")
    table.add_column("Tables")
    table.add_column("Rows")
    table.add_column("Hits")
    table.add_column("TTL Left")

    for e in entries:
        table.add_row(
            e["cache_key"][:8],
            e["sql"][:60],
            ", ".join(e["tables"]),
            str(e["row_count"]),
            str(e["hit_count"]),
            f"{e['ttl_remaining_seconds']}s",
        )
    console.print(table)


@cache.command("clear")
@click.argument("table_name", required=False)
def cache_clear(table_name: str):
    """Clear cache for a table or all."""
    from .query_cache import invalidate

    result = invalidate(table_name=table_name)
    console.print(result["message"])


@cache.command("policy")
@click.argument("table_name")
@click.option("--ttl", default=None, type=int, help="TTL in seconds")
@click.option("--disable", is_flag=True, help="Disable caching for this table")
def cache_policy(table_name: str, ttl: int, disable: bool):
    """Set cache policy for a table."""
    from .query_cache import set_cache_policy

    result = set_cache_policy(table_name, ttl_seconds=ttl, enabled=not disable)
    console.print(result["message"])


@main.group()
def sample():
    """Data sampling tools."""
    pass


main.add_command(sample)


@sample.command("random")
@click.argument("table_name")
@click.option("--fraction", default=0.1, help="Fraction of rows (0.0-1.0)")
@click.option("--seed", default=None, type=int, help="Random seed")
@click.option("--limit", default=20, type=int, help="Max rows to display")
def sample_random(table_name: str, fraction: float, seed: int, limit: int):
    """Random sample from a table."""
    from .sampling import random_sample
    from .catalog import get_catalog
    catalog = get_catalog()
    result = random_sample(catalog, table_name, fraction=fraction, seed=seed, limit=limit)
    console.print(f"[bold]{result['message']}[/bold]")
    if result["rows"]:
        table = Table(title=f"Random Sample ({result['sample_size']} rows)")
        for col in result["rows"][0].keys():
            table.add_column(col)
        for row in result["rows"][:limit]:
            table.add_row(*[str(v) for v in row.values()])
        console.print(table)


@sample.command("stratified")
@click.argument("table_name")
@click.argument("column")
@click.option("--fraction", default=0.1, help="Fraction of rows per stratum")
@click.option("--seed", default=None, type=int, help="Random seed")
def sample_stratified(table_name: str, column: str, fraction: float, seed: int):
    """Stratified sample maintaining column distribution."""
    from .sampling import stratified_sample
    from .catalog import get_catalog
    catalog = get_catalog()
    result = stratified_sample(catalog, table_name, column, fraction=fraction, seed=seed)
    console.print(f"[bold]{result['message']}[/bold]")

    strata_table = Table(title="Strata Distribution")
    strata_table.add_column("Value")
    strata_table.add_column("Total")
    strata_table.add_column("Sampled")
    for val, info in result["strata"].items():
        strata_table.add_row(val, str(info["total"]), str(info["sampled"]))
    console.print(strata_table)


@sample.command("systematic")
@click.argument("table_name")
@click.option("--every", "every_nth", default=10, help="Take every Nth row")
def sample_systematic(table_name: str, every_nth: int):
    """Systematic sample (every Nth row)."""
    from .sampling import systematic_sample
    from .catalog import get_catalog
    catalog = get_catalog()
    result = systematic_sample(catalog, table_name, every_nth=every_nth)
    console.print(f"[bold]{result['message']}[/bold]")


@sample.command("create")
@click.argument("table_name")
@click.argument("sample_name")
@click.option("--method", type=click.Choice(["random", "stratified", "systematic"]), default="random")
@click.option("--fraction", default=0.1, help="Fraction of rows")
@click.option("--column", default=None, help="Column for stratified sampling")
@click.option("--every", "every_nth", default=10, help="Every Nth row for systematic")
@click.option("--seed", default=None, type=int, help="Random seed")
def sample_create(table_name: str, sample_name: str, method: str, fraction: float, column: str, every_nth: int, seed: int):
    """Materialize a sample as a new table."""
    from .sampling import sample_to_table
    from .catalog import get_catalog
    catalog = get_catalog()
    result = sample_to_table(catalog, table_name, sample_name, method=method, fraction=fraction, column=column, every_nth=every_nth, seed=seed)
    console.print(f"[green]{result['message']}[/green]")


@main.group("auto-refresh")
def auto_refresh():
    """Manage dependency auto-refresh."""
    pass


main.add_command(auto_refresh)


@auto_refresh.command("enable")
@click.argument("table_name")
@click.option("--depth", default=3, help="Max cascade depth")
@click.option("--matviews/--no-matviews", default=True, help="Refresh materialized views")
@click.option("--pipelines/--no-pipelines", default=True, help="Re-run pipelines")
@click.option("--caches/--no-caches", default=True, help="Invalidate caches")
def auto_refresh_enable(table_name: str, depth: int, matviews: bool, pipelines: bool, caches: bool):
    """Enable auto-refresh for a table."""
    from .auto_refresh import set_auto_refresh
    result = set_auto_refresh(table_name, enabled=True, config={
        "cascade_depth": depth,
        "refresh_matviews": matviews,
        "rerun_pipelines": pipelines,
        "invalidate_caches": caches,
    })
    console.print(f"[green]{result['message']}[/green]")


@auto_refresh.command("disable")
@click.argument("table_name")
def auto_refresh_disable(table_name: str):
    """Disable auto-refresh for a table."""
    from .auto_refresh import remove_auto_refresh
    result = remove_auto_refresh(table_name)
    console.print(result["message"])


@auto_refresh.command("list")
def auto_refresh_list():
    """List auto-refresh configurations."""
    from .auto_refresh import list_auto_refresh
    configs = list_auto_refresh()
    if not configs:
        console.print("[yellow]No auto-refresh configurations[/yellow]")
        return

    table = Table(title="Auto-Refresh Configurations")
    table.add_column("Table", style="cyan")
    table.add_column("Enabled")
    table.add_column("Depth")
    table.add_column("Matviews")
    table.add_column("Pipelines")
    table.add_column("Caches")

    for c in configs:
        table.add_row(
            c["table"],
            "[green]Yes[/green]" if c["enabled"] else "[red]No[/red]",
            str(c.get("cascade_depth", 3)),
            "Yes" if c.get("refresh_matviews", True) else "No",
            "Yes" if c.get("rerun_pipelines", True) else "No",
            "Yes" if c.get("invalidate_caches", True) else "No",
        )
    console.print(table)


@auto_refresh.command("plan")
@click.argument("table_name")
def auto_refresh_plan(table_name: str):
    """Show what would be refreshed (dry-run)."""
    from .catalog import get_catalog
    from .auto_refresh import get_refresh_plan
    catalog = get_catalog()
    plan = get_refresh_plan(catalog, table_name)
    console.print(f"[bold]{plan['message']}[/bold]")

    if plan["actions"]:
        table = Table(title="Refresh Plan")
        table.add_column("Table", style="cyan")
        table.add_column("Action")
        table.add_column("Depth")
        for a in plan["actions"]:
            table.add_row(a["table"], a["action"], str(a["depth"]))
        console.print(table)


@auto_refresh.command("trigger")
@click.argument("table_name")
def auto_refresh_trigger(table_name: str):
    """Manually trigger cascade refresh."""
    from .catalog import get_catalog
    from .auto_refresh import trigger_refresh
    catalog = get_catalog()
    result = trigger_refresh(catalog, table_name)
    console.print(f"[bold]{result['message']}[/bold]")


@auto_refresh.command("history")
@click.option("--table", default=None, help="Filter by table")
@click.option("--limit", default=20, help="Max entries")
def auto_refresh_history(table: str, limit: int):
    """Show refresh history."""
    from .auto_refresh import get_refresh_history
    history = get_refresh_history(table_name=table, limit=limit)
    if not history:
        console.print("[yellow]No refresh history[/yellow]")
        return

    table_obj = Table(title="Refresh History")
    table_obj.add_column("Time", style="cyan")
    table_obj.add_column("Table")
    table_obj.add_column("Actions")
    table_obj.add_column("Success")
    table_obj.add_column("Errors")

    for h in history:
        table_obj.add_row(
            h.get("triggered_at", "")[:19],
            h["table"],
            str(h["actions_executed"]),
            str(h["successes"]),
            str(h["errors"]),
        )
    console.print(table_obj)


@main.group()
def contract():
    """Manage data contracts."""
    pass


main.add_command(contract)


@contract.command("create")
@click.argument("table_name")
@click.option("--file", "schema_file", required=True, help="JSON file with contract definition")
def contract_create(table_name: str, schema_file: str):
    """Create a contract from a JSON file."""
    from .contracts import create_contract

    contract_data = json.loads(Path(schema_file).read_text())
    result = create_contract(table_name, contract_data)
    console.print(f"[green]{result['message']}[/green]")


@contract.command("show")
@click.argument("table_name")
def contract_show(table_name: str):
    """Show contract details."""
    from .contracts import get_contract

    result = get_contract(table_name)
    if result is None:
        console.print(f"[yellow]No contract for '{table_name}'[/yellow]")
        return

    console.print(Panel(json.dumps(result, indent=2, default=str), title=f"Contract: {result['table']}"))


@contract.command("list")
@click.option("--namespace", default=None, help="Filter by namespace")
def contract_list(namespace: str):
    """List contracts."""
    from .contracts import list_contracts

    contracts = list_contracts(namespace=namespace)
    if not contracts:
        console.print("[yellow]No contracts defined[/yellow]")
        return

    table = Table(title="Data Contracts")
    table.add_column("Table", style="cyan")
    table.add_column("Owner")
    table.add_column("Status")
    table.add_column("Version")
    table.add_column("Description")
    table.add_column("Updated")

    for c in contracts:
        table.add_row(
            c["table"],
            c.get("owner", ""),
            c.get("status", "active"),
            str(c.get("version", 1)),
            c.get("description", "")[:40],
            c.get("updated_at", "")[:19],
        )
    console.print(table)


@contract.command("update")
@click.argument("table_name")
@click.option("--file", "update_file", required=True, help="JSON file with fields to update")
def contract_update(table_name: str, update_file: str):
    """Update contract fields from a JSON file."""
    from .contracts import update_contract

    updates = json.loads(Path(update_file).read_text())
    result = update_contract(table_name, updates)
    console.print(f"[green]{result['message']}[/green]")


@contract.command("remove")
@click.argument("table_name")
def contract_remove(table_name: str):
    """Remove a contract."""
    from .contracts import remove_contract

    result = remove_contract(table_name)
    console.print(result["message"])


@contract.command("summary")
@click.argument("table_name")
def contract_summary(table_name: str):
    """Show contract terms vs current table state."""
    from .catalog import get_catalog
    from .contracts import get_contract_summary

    catalog = get_catalog()
    result = get_contract_summary(catalog, table_name)

    if not result.get("has_contract"):
        console.print(f"[yellow]{result['message']}[/yellow]")
        return

    console.print(f"[bold]{result['message']}[/bold]")
    console.print(f"  Version: {result['version']}  |  Status: {result['status']}  |  Owner: {result.get('owner', 'N/A')}")

    if result["schema_issues"]:
        console.print("\n[red]Schema Issues:[/red]")
        for issue in result["schema_issues"]:
            console.print(f"  - {issue}")
    else:
        console.print("\n[green]Schema: OK[/green]")

    if result.get("quality_check"):
        qc = result["quality_check"]
        if "error" not in qc:
            status = "[green]PASS[/green]" if qc["passing"] else "[red]FAIL[/red]"
            console.print(f"  Quality: {qc['current_score']:.0f}/{qc['min_score']} {status}")

    if result.get("freshness_check"):
        fc = result["freshness_check"]
        if "error" not in fc:
            status = "[green]PASS[/green]" if fc["passing"] else "[red]FAIL[/red]"
            console.print(f"  Freshness: {fc['current_age_hours']:.1f}h / {fc['max_age_hours']}h max {status}")


@contract.command("validate")
@click.argument("table_name")
def contract_validate(table_name: str):
    """Validate table state against its contract."""
    from .catalog import get_catalog
    from .contracts import validate_contract

    catalog = get_catalog()
    result = validate_contract(catalog, table_name)

    if result["valid"]:
        console.print(f"[green]{result['message']}[/green]")
    else:
        console.print(f"[red]{result['message']}[/red]")
        table = Table(title="Violations")
        table.add_column("Type")
        table.add_column("Field")
        table.add_column("Rule")
        table.add_column("Message")
        for v in result["violations"]:
            table.add_row(v.get("type", ""), v.get("field", ""), v.get("rule", ""), v.get("message", ""))
        console.print(table)


@contract.command("check-data")
@click.argument("table_name")
@click.argument("rows_json")
def contract_check_data(table_name: str, rows_json: str):
    """Validate rows (JSON array) against a contract before writing."""
    from .contracts import validate_data_against_contract

    rows = json.loads(rows_json)
    result = validate_data_against_contract(table_name, rows)

    if result["valid"]:
        console.print(f"[green]{result['message']}[/green]")
    else:
        console.print(f"[red]{result['message']}[/red]")
        for v in result["violations"]:
            console.print(f"  - {v['message']}")


@contract.command("violations")
@click.argument("table_name")
def contract_violations(table_name: str):
    """Show all current violations for a table."""
    from .catalog import get_catalog
    from .contracts import get_contract_violations

    catalog = get_catalog()
    result = get_contract_violations(catalog, table_name)

    console.print(f"[bold]{result['message']}[/bold]")
    if result["violations"]:
        table = Table(title="Violations")
        table.add_column("Type")
        table.add_column("Field")
        table.add_column("Rule")
        table.add_column("Message")
        for v in result["violations"]:
            table.add_row(v.get("type", ""), str(v.get("field", "")), v.get("rule", ""), v.get("message", ""))
        console.print(table)


@contract.command("history")
@click.argument("table_name")
@click.option("--limit", "-n", default=20, help="Max versions to show")
def contract_history(table_name: str, limit: int):
    """Show contract version history."""
    from .contracts import get_contract_history

    history = get_contract_history(table_name, limit=limit)
    if not history:
        console.print("[yellow]No version history found.[/yellow]")
        return

    table = Table(title=f"Contract History ({len(history)} versions)")
    table.add_column("Version")
    table.add_column("Owner")
    table.add_column("Description")
    table.add_column("Snapshot At")
    for h in history:
        table.add_row(
            str(h.get("version", "")),
            h.get("owner", ""),
            h.get("description", "")[:40],
            h.get("snapshot_at", "")[:19],
        )
    console.print(table)


@contract.command("diff")
@click.argument("table_name")
@click.option("--v1", required=True, type=int, help="First version")
@click.option("--v2", required=True, type=int, help="Second version")
def contract_diff(table_name: str, v1: int, v2: int):
    """Diff two contract versions."""
    from .contracts import diff_contract_versions

    diff = diff_contract_versions(table_name, v1, v2)
    if "error" in diff:
        console.print(f"[red]{diff['error']}[/red]")
        return

    console.print(f"[bold]{diff['message']}[/bold]")
    for c in diff["changes"]:
        console.print(f"  [yellow]{c['field']}[/yellow]: {c['from']} → {c['to']}")
    if diff["schema_added"]:
        console.print(f"  [green]Columns added:[/green] {', '.join(diff['schema_added'])}")
    if diff["schema_removed"]:
        console.print(f"  [red]Columns removed:[/red] {', '.join(diff['schema_removed'])}")
    if diff["schema_changed"]:
        for sc in diff["schema_changed"]:
            console.print(f"  [yellow]Column '{sc['column']}':[/yellow] {sc['from']} → {sc['to']}")


@contract.command("deprecate")
@click.argument("table_name")
@click.option("--reason", required=True, help="Deprecation reason")
@click.option("--sunset", default=None, help="Sunset date (YYYY-MM-DD)")
def contract_deprecate(table_name: str, reason: str, sunset: str):
    """Deprecate a contract."""
    from .contracts import deprecate_contract

    result = deprecate_contract(table_name, reason, sunset_date=sunset)
    console.print(f"[yellow]{result['message']}[/yellow]")


@contract.command("status")
@click.argument("table_name")
def contract_status(table_name: str):
    """Show lifecycle status of a contract."""
    from .contracts import get_contract_status

    result = get_contract_status(table_name)
    status_color = {"active": "green", "deprecated": "yellow", "not_found": "red"}.get(result["status"], "white")
    console.print(f"  Table: {result['table']}")
    console.print(f"  Status: [{status_color}]{result['status']}[/{status_color}]")
    if result.get("version"):
        console.print(f"  Version: {result['version']}")
    if result.get("deprecation_reason"):
        console.print(f"  Reason: {result['deprecation_reason']}")
    if result.get("sunset_date"):
        console.print(f"  Sunset: {result['sunset_date']}")


@contract.command("monitor")
@click.argument("table_name")
def contract_monitor(table_name: str):
    """Run compliance check and show results."""
    from .catalog import get_catalog
    from .contracts import monitor_contract

    catalog = get_catalog()
    result = monitor_contract(catalog, table_name)

    if not result["checked"]:
        console.print(f"[yellow]{result['message']}[/yellow]")
        return

    if result["passed"]:
        console.print(f"[green]{result['message']}[/green]")
    else:
        console.print(f"[red]{result['message']}[/red]")
        for v in result["violations"]:
            console.print(f"  - {v['message']}")


@contract.command("compliance")
@click.argument("table_name")
def contract_compliance(table_name: str):
    """Show compliance score and history."""
    from .catalog import get_catalog
    from .contracts import get_compliance_score, get_compliance_history

    catalog = get_catalog()
    score = get_compliance_score(catalog, table_name)

    if score["score"] is None:
        console.print(f"[yellow]{score['message']}[/yellow]")
        return

    color = "green" if score["score"] >= 80 else "yellow" if score["score"] >= 50 else "red"
    console.print(f"[bold]Compliance Score: [{color}]{score['score']}/100[/{color}][/bold]")
    console.print(f"  Schema: {score['schema_ratio']:.0%}  |  Constraints: {score['constraint_ratio']:.0%}")
    console.print(f"  Quality: {score['quality_ratio']:.0%}  |  Freshness: {score['freshness_ratio']:.0%}")

    history = get_compliance_history(table_name, limit=5)
    if history:
        console.print("\n[bold]Recent Checks:[/bold]")
        for h in history:
            status = "[green]PASS[/green]" if h["passed"] else f"[red]FAIL ({h['violation_count']})[/red]"
            console.print(f"  {h['checked_at'][:19]}  {status}")


@contract.command("add-consumer")
@click.argument("table_name")
@click.argument("consumer")
@click.option("--contact", default=None, help="Consumer contact email")
@click.option("--usage", default=None, help="How the consumer uses the data")
def contract_add_consumer(table_name: str, consumer: str, contact: str, usage: str):
    """Register a consumer of a table's contract."""
    from .contracts import add_consumer

    result = add_consumer(table_name, consumer, contact=contact, usage=usage)
    console.print(result["message"])


@contract.command("add-producer")
@click.argument("table_name")
@click.argument("producer")
@click.option("--contact", default=None, help="Producer contact email")
def contract_add_producer(table_name: str, producer: str, contact: str):
    """Register the producer for a table."""
    from .contracts import add_producer

    result = add_producer(table_name, producer, contact=contact)
    console.print(result["message"])


@contract.command("consumers")
@click.argument("table_name")
def contract_consumers(table_name: str):
    """List consumers of a table."""
    from .contracts import list_consumers

    consumers = list_consumers(table_name)
    if not consumers:
        console.print("[yellow]No consumers registered.[/yellow]")
        return

    table = Table(title="Consumers")
    table.add_column("Name")
    table.add_column("Contact")
    table.add_column("Usage")
    table.add_column("Added")
    for c in consumers:
        table.add_row(c["name"], c.get("contact", ""), c.get("usage", ""), c.get("added_at", "")[:19])
    console.print(table)


@contract.command("coverage")
def contract_coverage():
    """Show contract coverage across all tables."""
    from .catalog import get_catalog
    from .contracts import get_contract_coverage

    catalog = get_catalog()
    result = get_contract_coverage(catalog)

    color = "green" if result["coverage_pct"] >= 80 else "yellow" if result["coverage_pct"] >= 50 else "red"
    console.print(f"[bold]{result['message']}[/bold]")
    console.print(f"  [{color}]{result['coverage_pct']}%[/{color}] coverage ({result['contracted']}/{result['total_tables']} tables)")
    if result["uncovered_tables"]:
        console.print("\n[yellow]Uncovered tables:[/yellow]")
        for t in result["uncovered_tables"]:
            console.print(f"  - {t}")


@contract.command("generate")
@click.argument("table_name")
@click.option("--strict", is_flag=True, help="Use tighter thresholds")
@click.option("--preview", "preview_only", is_flag=True, help="Preview without saving")
def contract_generate(table_name: str, strict: bool, preview_only: bool):
    """Generate a contract from live data."""
    from .catalog import get_catalog
    from .contracts import generate_contract, preview_contract

    catalog = get_catalog()
    if preview_only:
        contract = preview_contract(catalog, table_name, strict=strict)
        console.print(json.dumps(contract, indent=2, default=str))
    else:
        result = generate_contract(catalog, table_name, strict=strict)
        console.print(f"[green]{result['message']}[/green]")
        console.print(json.dumps(result["contract"], indent=2, default=str))


@contract.command("apply")
@click.argument("table_name")
@click.option("--file", "file_path", required=True, help="Path to contract JSON file")
def contract_apply(table_name: str, file_path: str):
    """Apply a contract from a JSON file."""
    from .catalog import get_catalog
    from .contracts import apply_generated_contract

    catalog = get_catalog()
    contract = json.loads(Path(file_path).read_text())
    result = apply_generated_contract(catalog, table_name, contract)
    console.print(f"[green]{result['message']}[/green]")


@contract.command("dry-run")
@click.argument("table_name")
@click.option("--file", "file_path", required=True, help="Path to contract JSON file")
def contract_dry_run(table_name: str, file_path: str):
    """Test a contract against live data without saving."""
    from .catalog import get_catalog
    from .contracts import dry_run_contract

    catalog = get_catalog()
    contract = json.loads(Path(file_path).read_text())
    result = dry_run_contract(catalog, table_name, contract)

    if result["valid"]:
        console.print(f"[green]{result['message']}[/green]")
    else:
        console.print(f"[red]{result['message']}[/red]")
        for v in result["violations"]:
            console.print(f"  - {v['message']}")


@contract.command("dry-run-migration")
@click.argument("table_name")
@click.option("--file", "file_path", required=True, help="Path to new contract JSON file")
def contract_dry_run_migration(table_name: str, file_path: str):
    """Simulate migration to a new contract."""
    from .catalog import get_catalog
    from .contracts import dry_run_migration

    catalog = get_catalog()
    new_contract = json.loads(Path(file_path).read_text())
    result = dry_run_migration(catalog, table_name, new_contract)

    console.print(f"[bold]{result['message']}[/bold]")
    if result["safe_to_migrate"]:
        console.print("[green]Safe to migrate[/green]")
    else:
        console.print(f"[red]{result['introduced_count']} new violations introduced[/red]")
        for v in result["introduced"]:
            console.print(f"  - {v['message']}")
    if result["resolved"]:
        console.print(f"[green]{result['resolved_count']} violations resolved[/green]")


@contract.command("dashboard")
def contract_dashboard_cmd():
    """Show contract compliance dashboard."""
    from .catalog import get_catalog
    from .contracts import get_contract_dashboard

    catalog = get_catalog()
    d = get_contract_dashboard(catalog)

    console.print(f"\n[bold]Contract Dashboard[/bold]")
    console.print(f"  Contracts: {d['total_contracts']} ({d['active']} active, {d['deprecated']} deprecated)")
    console.print(f"  Coverage: {d['coverage_pct']}% ({len(d['uncovered_tables'])} uncovered)")

    compliance_color = "green" if d["compliance_rate"] >= 80 else "yellow" if d["compliance_rate"] >= 50 else "red"
    console.print(f"  Compliance: [{compliance_color}]{d['compliance_rate']}%[/{compliance_color}]")

    if d["worst_tables"]:
        console.print("\n[bold red]Worst Tables:[/bold red]")
        for t in d["worst_tables"]:
            console.print(f"  - {t['table']}: {t['last_violation_count']} violations")

    if d["recent_violations"]:
        console.print("\n[bold]Recent Violations:[/bold]")
        for v in d["recent_violations"][:5]:
            console.print(f"  {v['checked_at'][:19]}  {v['table']}  ({v['violation_count']} violations)")


@contract.command("health")
@click.argument("table_name")
def contract_health_cmd(table_name: str):
    """Show health card for a single table."""
    from .catalog import get_catalog
    from .contracts import get_contract_health

    catalog = get_catalog()
    h = get_contract_health(catalog, table_name)

    if not h["has_contract"]:
        console.print(f"[yellow]{h['message']}[/yellow]")
        return

    console.print(f"\n[bold]Health: {h['table']}[/bold]")
    console.print(f"  Status: {h['status']}  |  Version: {h['version']}")
    if h["compliance_score"] is not None:
        color = "green" if h["compliance_score"] >= 80 else "yellow" if h["compliance_score"] >= 50 else "red"
        console.print(f"  Score: [{color}]{h['compliance_score']}/100[/{color}]")
    if h["last_check_at"]:
        status = "[green]PASS[/green]" if h["last_check_passed"] else f"[red]FAIL ({h['last_violation_count']})[/red]"
        console.print(f"  Last check: {h['last_check_at'][:19]}  {status}")
    if h["producer"]:
        console.print(f"  Producer: {h['producer']}")
    if h["consumers"]:
        console.print(f"  Consumers: {', '.join(h['consumers'])}")


@contract.command("trends")
@click.argument("table_name", required=False)
@click.option("--days", default=30, help="Number of days to look back")
def contract_trends_cmd(table_name: str, days: int):
    """Show violation trends."""
    from .contracts import get_violation_trends

    trends = get_violation_trends(table_name=table_name, days=days)
    if not trends:
        console.print("[yellow]No compliance data found.[/yellow]")
        return

    table = Table(title=f"Violation Trends (last {days} days)")
    table.add_column("Date")
    table.add_column("Checks")
    table.add_column("Passed")
    table.add_column("Failed")
    table.add_column("Violations")
    for t in trends:
        table.add_row(t["date"], str(t["checks"]), str(t["passed"]), str(t["failed"]), str(t["violations"]))
    console.print(table)


@main.command()
@click.option("--rows", default="100,1000,10000", help="Comma-separated row counts to benchmark")
@click.option("--output", "-o", default=None, help="Output markdown file (default: print to stdout)")
def benchmark(rows: str, output: str):
    """Run Parquet vs Vortex performance benchmarks.

    Examples:
        lakehouse benchmark
        lakehouse benchmark --rows 1000,10000,100000
        lakehouse benchmark -o docs/benchmarks.md
    """
    from benchmarks.format_comparison import run_benchmarks, generate_report

    row_counts = [int(x.strip()) for x in rows.split(",")]

    console.print(f"[bold blue]Running benchmarks...[/bold blue]")
    console.print(f"  Row counts: {', '.join(f'{n:,}' for n in row_counts)}")
    console.print(f"  Data types: numeric, string, mixed\n")

    try:
        results = run_benchmarks(row_counts=row_counts)
        report = generate_report(results, row_counts)

        if output:
            from pathlib import Path
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report)
            console.print(f"[bold green]✓ Report written to {output}[/bold green]")
        else:
            console.print(report)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


if __name__ == "__main__":
    main()
