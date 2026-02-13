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
