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

    catalog = _get_catalog()
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

    catalog = _get_catalog()
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

    catalog = _get_catalog()
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

    catalog = _get_catalog()
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

    catalog = _get_catalog()
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

    catalog = _get_catalog()
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

    catalog = _get_catalog()
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
