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
def query(sql: str, max_rows: int, output_format: str):
    """Execute a SQL query against the lakehouse."""
    from .query import execute_query

    try:
        result = execute_query(sql, max_rows=max_rows)

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


if __name__ == "__main__":
    main()
